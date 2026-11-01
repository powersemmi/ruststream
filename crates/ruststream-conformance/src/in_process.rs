//! The in-process transport held to what the test harness relies on, and to the real server.
//!
//! [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) drives a service through a broker's in-process transport.
//! It counts the deliveries in flight through the [`Coordinator`] the transport reports to, reads
//! the transport's publish log back for its assertions, and, live, waits on the subscriptions
//! [`TestableBroker::routes`] names. A transport that miscounts ends a settle before the handler
//! ran, and one whose routing answer differs from its deliveries waits forever or not at all, so
//! each of those is checked here.
//!
//! [`run_suite`](super::harness::run_suite) runs the checks that need nothing from the broker:
//!
//! * the publish log records what the broker's own publisher sent, not only what a test injected
//!   (for a broker registered with [`register_testable_broker!`](ruststream_runtime::register_testable_broker),
//!   whose default publisher the harness pairs);
//! * two subscriptions of one name, and one of another, receive what
//!   [`routes`](TestableBroker::routes) says they receive: each message reaches exactly the
//!   subscriptions the answer names, every one of them where the broker fans out, the one it picks
//!   where they compete;
//! * the [`Coordinator`] counts balance on a paused clock, the way `TestApp` drives it: a message
//!   is counted in flight before the publish returns, released once when it is settled, counted
//!   again by a requeue, left out when no subscription receives it, and a delayed redelivery is
//!   counted by the time its timer falls due.
//!
//! Two opt-in suites hold what the in-process transport declares to what the server does. They
//! connect the broker twice per probe, with [`Broker::connect`](ruststream_core::Broker::connect) and with
//! [`InProcess::connect_in_process`], so run them where the broker's live suite runs:
//!
//! * [`backlog_matches_server`]: a message published before a subscription opens reaches it on
//!   both transports exactly as [`TestableBroker::backlog`] declares;
//! * [`refuses_like_the_server`]: each [`Refusal`] the broker supplies (a payload over the limit, a
//!   refused destination, a refused subscription) is refused by both transports, and both admit
//!   the same of two subscriptions to one name and one to another.
//!
//! # Examples
//!
//! ```no_run
//! # async fn run() {
//! use ruststream::conformance::in_process::{self, Refusal};
//! use ruststream::memory::{MemoryBroker, MemorySource};
//!
//! in_process::backlog_matches_server(MemoryBroker::new, |connected| connected.publisher()).await;
//! in_process::refuses_like_the_server(
//!     MemoryBroker::new,
//!     |connected| connected.publisher(),
//!     [Refusal::Subscription {
//!         source: MemorySource::new("orders.*"),
//!     }],
//! )
//! .await;
//! # }
//! ```

use std::{any::Any, fmt, panic::resume_unwind, pin::pin, thread, time::Duration};

use futures::{FutureExt, Stream, StreamExt, stream};
use tokio::{runtime, time, time::timeout};

use super::harness::{expect_next, expect_no_more};
use super::helpers::unique_subject;
use ruststream_core::{
    AckError, ConnectedBroker, HeaderMap, OutgoingMessage, RawMessage, SendBroker, SendSubscribe,
    Settle, Subscribe, Subscriber,
};
use ruststream_runtime::testing::{Coordinator, InProcess, TestableBroker, TestableRegistration};

mod server;

pub use server::{Refusal, backlog_matches_server, refuses_like_the_server};

/// How long a scenario waits for a delivery the in-process transport owes.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a scenario waits for a delivery nothing owes before it concludes none is coming.
const QUIET: Duration = Duration::from_millis(100);
/// The delay the counting scenario redelivers with. Whole seconds, so a transport whose delays
/// have second granularity schedules it unrounded; the clock is paused, so it costs nothing.
const REDELIVERY_DELAY: Duration = Duration::from_secs(2);
/// How many messages the two subscriptions of one name share.
const SHARED: u32 = 10;

/// Runs every in-process scenario against a fresh broker from `factory`.
pub(crate) async fn run<B, F>(factory: F)
where
    B: InProcess + SendBroker,
    B::Connected: SendSubscribe + TestableBroker,
    F: Fn() -> B,
{
    own_publishes_are_logged(connect_in_process(factory()).await).await;
    one_name_two_subscriptions(connect_in_process(factory()).await).await;
    counts_balance(factory());
}

async fn connect_in_process<B: InProcess + SendBroker>(broker: B) -> B::Connected {
    broker
        .connect_in_process()
        .await
        .expect("broker must connect in process before a suite scenario")
}

/// The registration the harness finds `connected`'s broker by, when the broker has one.
fn registration_of<C: TestableBroker + 'static>(
    connected: &C,
) -> Option<&'static TestableRegistration> {
    let connected: &dyn Any = connected;
    inventory::iter::<TestableRegistration>
        .into_iter()
        .find(|registration| registration.resolve(connected).is_some())
}

/// Publishes `msg` through the broker's own default publisher, the one the harness pairs for a
/// handler's reply.
async fn publish_own<C>(
    registration: &TestableRegistration,
    connected: &C,
    msg: OutgoingMessage<'_>,
) where
    C: TestableBroker + 'static,
{
    let what = format!("{:?}", msg.name());
    (registration.live())(connected, msg)
        .await
        .unwrap_or_else(|err| {
            panic!("a publish to {what} through the broker's own default publisher failed: {err}")
        });
}

/// Publishes `msg` the way a service does, through the broker's own default publisher, where the
/// broker is registered; by injection, as an external producer, where it is not. Answers whether
/// the broker took the message: a refused publish reaches no subscription.
async fn publish_as_service<C>(
    connected: &C,
    registration: Option<&TestableRegistration>,
    msg: OutgoingMessage<'_>,
) -> bool
where
    C: TestableBroker + 'static,
{
    if let Some(registration) = registration {
        (registration.live())(connected, msg).await.is_ok()
    } else {
        connected.inject(msg);
        true
    }
}

/// Acknowledges `msg`, accepting a transport that cannot acknowledge.
async fn settle_ack<M: Settle>(msg: M) {
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
}

/// Whether the coordinator counts nothing in flight: what `TestApp::settle` waits for.
fn quiescent(coordinator: &Coordinator) -> bool {
    coordinator.drive().now_or_never().is_some()
}

/// What the broker's own publisher sends is in the publish log next to what a test injected, in
/// publish order: a handler's reply is published that way, and `TestApp` reads it from the log.
async fn own_publishes_are_logged<C: TestableBroker + Subscribe>(connected: C) {
    let name = unique_subject("conformance.ownpublish");
    // Opened first, so a queue a subscription declares exists when the publish arrives.
    let subscriber = Subscribe::subscribe(&connected, &name)
        .await
        .expect("subscribe failed");
    let registration = registration_of(&connected);
    let mut headers = HeaderMap::new();
    headers.insert("x-conformance", "own");
    let first = OutgoingMessage::new(name.as_str(), b"first".as_slice()).with_headers(headers);
    // A publisher whose transport sends nowhere from here (a subscription that dials a queue
    // socket, a request-reply socket) refuses the publish, and a refused publish is not logged.
    let Some(registration) = registration else {
        drop(subscriber);
        connected.shutdown().await.expect("shutdown failed");
        return;
    };
    if publish_as_service(&connected, Some(registration), first).await {
        connected.inject(OutgoingMessage::new(name.as_str(), b"injected".as_slice()));
        publish_own(
            registration,
            &connected,
            OutgoingMessage::new(name.as_str(), b"second".as_slice()),
        )
        .await;

        let log = connected.published(&name);
        let payloads: Vec<&[u8]> = log.iter().map(RawMessage::payload).collect();
        assert_eq!(
            payloads,
            [b"first".as_slice(), b"injected", b"second"],
            "the publish log must record what the broker's own publisher sent as well as what was \
             injected, in publish order: a handler's reply is published that way and TestApp's \
             `published` assertions read the log",
        );
        assert_eq!(
            log[0].headers().get("x-conformance"),
            Some(b"own".as_slice()),
            "the publish log must keep the headers the broker's own publisher sent",
        );
    } else {
        assert_eq!(
            connected.published(&name),
            Vec::new(),
            "the publish log recorded a publish the broker's own publisher refused",
        );
    }
    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

/// One delivery the two-subscription scenario read: the subscription it came through and which
/// of the published messages it is.
struct Received {
    position: usize,
    message: u32,
}

/// Two subscriptions of one name, and one of another, receive exactly what the broker's routing
/// answer says: `routes` is what a live `TestApp` waits on, so a transport that delivers
/// otherwise makes it wait for a delivery that never comes, or stop before one that does.
async fn one_name_two_subscriptions<C: TestableBroker + SendSubscribe>(connected: C) {
    let coordinator = Coordinator::new(usize::MAX);
    connected.install_coordinator(coordinator.clone());
    let name = unique_subject("conformance.shared");
    let other = unique_subject("conformance.elsewhere");

    let mut subscribers = vec![(
        name.as_str(),
        Subscribe::subscribe(&connected, &name)
            .await
            .expect("subscribe failed"),
    )];
    // A transport whose model admits one consumer per name, or one subscription per connection,
    // refuses the next one, as its server does; the routing is then checked over the ones it
    // opened. That it admits what the server admits needs the server, so `refuses_like_the_server`
    // holds it.
    let second = Subscribe::subscribe(&connected, &name).await.ok();
    subscribers.extend(second.map(|second| (name.as_str(), second)));
    let third = Subscribe::subscribe(&connected, &other).await.ok();
    subscribers.extend(third.map(|third| (other.as_str(), third)));
    let names: Vec<&str> = subscribers.iter().map(|(name, _)| *name).collect();

    let routed = publish_shared(&connected, &name, &names).await;
    let owed: usize = routed.iter().map(Vec::len).sum();
    let received = receive_shared(&mut subscribers, owed).await;
    assert_received_as_routed(&routed, &received, &name, &names);

    assert!(
        quiescent(&coordinator),
        "one_name_two_subscriptions: every delivery was settled, yet the coordinator still counts \
         one in flight; each delivery is counted once by `enqueued` and released once by \
         `consumed`",
    );
    drop(subscribers);
    connected.shutdown().await.expect("shutdown failed");
}

/// Publishes the two-subscription scenario's messages to `name` the way a service publishes,
/// which is what `routes` answers for, and returns the subscriptions each one is owed to.
async fn publish_shared<C>(connected: &C, name: &str, names: &[&str]) -> Vec<Vec<usize>>
where
    C: TestableBroker + 'static,
{
    let registration = registration_of(connected);
    let mut routed = Vec::new();
    for message in 0..SHARED {
        let answer = connected.routes(name, names);
        let taken = publish_as_service(
            connected,
            registration,
            OutgoingMessage::new(name, message.to_be_bytes().as_slice()),
        )
        .await;
        // A refused publish reaches nobody, whatever `routes` would answer for one the broker
        // took; the harness asks `routes` only about publishes that were taken.
        routed.push(if taken { answer } else { Vec::new() });
    }
    routed
}

/// Reads and settles what the subscriptions receive: the `owed` deliveries, then whatever else
/// arrives before the subscriptions fall quiet.
async fn receive_shared<Sub: Subscriber>(
    subscribers: &mut [(&str, Sub)],
    owed: usize,
) -> Vec<Received> {
    let mut merged = stream::select_all(subscribers.iter_mut().enumerate().map(
        |(position, (_, subscriber))| {
            Box::pin(subscriber.stream().map(move |item| (position, item)))
        },
    ));
    let mut received = Vec::new();
    while received.len() < owed {
        match timeout(DELIVERY_TIMEOUT, merged.next()).await {
            Ok(Some(delivery)) => received.push(read_shared(delivery).await),
            // Every stream ended, or nothing more arrived: what arrived is compared with what
            // was owed, which names the difference.
            Ok(None) | Err(_) => break,
        }
    }
    while let Ok(Some(delivery)) = timeout(QUIET, merged.next()).await {
        received.push(read_shared(delivery).await);
    }
    received
}

/// Each message reached exactly the subscriptions `routes` answered for it, once each.
///
/// Compared per subscription, not per name: a live `TestApp` waits on the very subscriptions the
/// answer names, so a delivery to the other one of the same name leaves it waiting.
fn assert_received_as_routed(
    routed: &[Vec<usize>],
    received: &[Received],
    name: &str,
    names: &[&str],
) {
    for (message, answer) in (0..SHARED).zip(routed) {
        let mut expected = answer.clone();
        expected.sort_unstable();
        let mut delivered: Vec<usize> = received
            .iter()
            .filter(|delivery| delivery.message == message)
            .map(|delivery| delivery.position)
            .collect();
        delivered.sort_unstable();
        assert_eq!(
            delivered, expected,
            "message {message} published to {name:?} with subscriptions {names:?}: `routes` \
             answers {answer:?}, so exactly those subscriptions must receive it, once each; a live \
             TestApp waits on that answer",
        );
    }
}

/// Reads one delivery of the two-subscription scenario and settles it.
async fn read_shared<M, E>((position, item): (usize, Result<M, E>)) -> Received
where
    M: Settle,
    E: fmt::Debug,
{
    let msg = item
        .unwrap_or_else(|err| panic!("one_name_two_subscriptions: stream yielded error: {err:?}"));
    let message = msg.payload().try_into().map(u32::from_be_bytes).expect(
        "one_name_two_subscriptions: a delivery carries a payload it was not published with",
    );
    settle_ack(msg).await;
    Received { position, message }
}

/// The coordinator counts, on a paused clock of their own, the way `TestApp` drives them.
///
/// Why a thread and a runtime of its own: `TestApp` runs an in-process test on a paused clock and
/// fires delayed redeliveries by moving it, which only a current-thread runtime allows, while the
/// suite may run on any runtime. The broker is connected there too, as the harness connects it on
/// the test's runtime. The caller's thread waits for it; nothing of the scenario runs on the
/// caller's runtime.
fn counts_balance<B>(broker: B)
where
    B: InProcess + SendBroker,
    B::Connected: SendSubscribe + TestableBroker,
{
    thread::scope(|scope| {
        let joined = thread::Builder::new()
            .name("conformance-paused-clock".to_owned())
            .spawn_scoped(scope, move || {
                runtime::Builder::new_current_thread()
                    .enable_all()
                    .start_paused(true)
                    .build()
                    .expect("a paused current-thread runtime must build")
                    .block_on(counts(broker));
            })
            .expect("the paused-clock thread must start")
            .join();
        if let Err(panic) = joined {
            resume_unwind(panic);
        }
    });
}

async fn counts<B>(broker: B)
where
    B: InProcess + SendBroker,
    B::Connected: SendSubscribe + TestableBroker,
{
    let connected = connect_in_process(broker).await;
    let coordinator = Coordinator::new(usize::MAX);
    connected.install_coordinator(coordinator.clone());
    let name = unique_subject("conformance.counted");
    let mut subscriber = Subscribe::subscribe(&connected, &name)
        .await
        .expect("subscribe failed");
    let registration = registration_of(&connected);
    {
        let mut stream = pin!(subscriber.stream());
        let inject = |payload: &'static [u8]| {
            connected.inject(OutgoingMessage::new(name.as_str(), payload));
        };
        // Polled once first, as the dispatch loop polls it: a subscription may count work of its
        // own (a pending seek) until its stream picks that up.
        assert!(
            stream.next().now_or_never().is_none(),
            "counts: a subscription to a fresh name delivered before anything was published",
        );
        assert!(
            quiescent(&coordinator),
            "counts: nothing is published yet, and the coordinator already counts a delivery in \
             flight",
        );

        inject(b"injected");
        injected_counts(&coordinator, &mut stream).await;
        if let Some(registration) = registration {
            own_publish_counts(&connected, registration, &coordinator, &mut stream, &name).await;
        }
        unread_publish_counts(&connected, &coordinator, &mut stream, &name).await;

        settlement_counts(&coordinator, &mut stream, inject).await;
    }
    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

/// An injected message is counted in flight before `inject` returns, stays counted while the
/// handler holds it, and is released once by its acknowledgement.
async fn injected_counts<M, E, S>(coordinator: &Coordinator, stream: &mut S)
where
    M: Settle,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    assert!(
        !quiescent(coordinator),
        "counts: a message injected into a subscription must be counted in flight \
         (`Coordinator::enqueued`) before `inject` returns; TestApp would stop waiting before \
         the handler ran",
    );
    let msg = expect_next(stream, "counts injected").await;
    assert!(
        !quiescent(coordinator),
        "counts: a delivery in the handler's hands must stay counted until it is settled",
    );
    settle_ack(msg).await;
    assert!(
        quiescent(coordinator),
        "counts: an acked delivery must be released exactly once (`Coordinator::consumed`, \
         from its `Drop`)",
    );
}

/// A message the broker's own publisher sent to `name` is counted in flight by the time the
/// publish returns, and released once.
async fn own_publish_counts<C, M, E, S>(
    connected: &C,
    registration: &TestableRegistration,
    coordinator: &Coordinator,
    stream: &mut S,
    name: &str,
) where
    C: TestableBroker + 'static,
    M: Settle,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let reaches = !connected.routes(name, &[name]).is_empty();
    let taken = publish_as_service(
        connected,
        Some(registration),
        OutgoingMessage::new(name, b"own".as_slice()),
    )
    .await;
    if taken && reaches {
        assert!(
            !quiescent(coordinator),
            "counts: a message the broker's own publisher sent must be counted in flight \
             by the time the publish returns; a handler's publish is followed by its \
             settlement, and TestApp would stop waiting in between",
        );
        settle_ack(expect_next(stream, "counts own publish").await).await;
    }
    assert!(
        quiescent(coordinator),
        "counts: a delivery of the broker's own publish must be released exactly once, \
         and a publish that reaches no subscription must not be counted",
    );
}

/// A publish to another name than the subscription's `name` is counted only where `routes` says
/// the subscription receives it.
///
/// Where the broker routes by name, a publish to another one reaches no subscription; a
/// transport that routes by connection (a bound queue socket) delivers it all the same.
async fn unread_publish_counts<C, M, E, S>(
    connected: &C,
    coordinator: &Coordinator,
    stream: &mut S,
    name: &str,
) where
    C: TestableBroker + 'static,
    M: Settle,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let unread = unique_subject("conformance.unread");
    let reaches = !connected.routes(&unread, &[name]).is_empty();
    let taken = publish_as_service(
        connected,
        registration_of(connected),
        OutgoingMessage::new(unread.as_str(), b"unread".as_slice()),
    )
    .await;
    if taken && reaches {
        assert!(
            !quiescent(coordinator),
            "counts: `routes` says the subscription receives a publish to {unread:?}, so it \
             must be counted in flight",
        );
        settle_ack(expect_next(stream, "counts routed elsewhere").await).await;
    }
    assert!(
        quiescent(coordinator),
        "counts: a publish no subscription receives must not be counted in flight; TestApp \
         would wait for a handler that never runs",
    );
}

/// Each settlement releases the delivery once, and each redelivery is counted again.
async fn settlement_counts<M, E, S>(
    coordinator: &Coordinator,
    stream: &mut S,
    inject: impl Fn(&'static [u8]),
) where
    M: Settle,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let coordinator = coordinator.clone();
    {
        inject(b"dropped");
        dropped_counts(&coordinator, stream).await;

        inject(b"requeued");
        requeued_counts(&coordinator, stream).await;

        inject(b"delayed");
        let msg = expect_next(stream, "counts nack_after").await;
        if msg.supports_nack_after() {
            delayed_redelivery(msg, &coordinator, stream).await;
        } else {
            settle_ack(msg).await;
        }

        // Last, because a transport may take an unsettled delivery back on its own schedule.
        inject(b"unsettled");
        drop(expect_next(stream, "counts unsettled drop").await);
        if !quiescent(&coordinator) {
            // Counted again: the transport took the delivery back, so it must come back.
            settle_ack(expect_next(stream, "counts unsettled redelivery").await).await;
            assert!(
                quiescent(&coordinator),
                "counts: an unsettled delivery the transport took back is counted once more than \
                 it redelivers",
            );
        }
    }
}

/// A delivery nacked without requeue is released once and does not come back.
async fn dropped_counts<M, E, S>(coordinator: &Coordinator, stream: &mut S)
where
    M: Settle,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let msg = expect_next(stream, "counts nack(false)").await;
    match msg.nack(false).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
    }
    assert!(
        quiescent(coordinator),
        "counts: a delivery nacked without requeue must be released exactly once",
    );
    expect_no_more(stream, "counts nack(false)").await;
}

/// A requeued delivery is counted again before `nack` returns, and its redelivery is released
/// once; a transport with no requeue still releases the delivery.
async fn requeued_counts<M, E, S>(coordinator: &Coordinator, stream: &mut S)
where
    M: Settle,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let msg = expect_next(stream, "counts nack(true)").await;
    match msg.nack(true).await {
        Ok(()) => {
            assert!(
                !quiescent(coordinator),
                "counts: a requeued message must be counted in flight again before `nack` \
                 returns; TestApp would stop waiting before the redelivery was handled",
            );
            settle_ack(expect_next(stream, "counts requeued").await).await;
            assert!(
                quiescent(coordinator),
                "counts: an acked redelivery must be released exactly once",
            );
        }
        Err(AckError::Unsupported) => assert!(
            quiescent(coordinator),
            "counts: a delivery whose requeue is unsupported must still be released",
        ),
        Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
    }
}

/// A delayed nack releases the delivery at once and counts the redelivery when its timer falls
/// due, the way `TestApp::advance` moves the clock and then fires the due timers.
async fn delayed_redelivery<M, E, S>(msg: M, coordinator: &Coordinator, stream: &mut S)
where
    M: Settle,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    match msg.nack_after(REDELIVERY_DELAY).await {
        Ok(()) => {}
        Err(AckError::Unsupported) => {
            assert!(
                quiescent(coordinator),
                "counts: a delivery whose delayed nack is unsupported must still be released",
            );
            return;
        }
        Err(other) => panic!("nack_after must succeed or be unsupported, got: {other:?}"),
    }
    assert!(
        quiescent(coordinator),
        "counts: after `nack_after` the delivery must be released and the redelivery must wait \
         for its delay; TestApp returns from the publish there and delivers it on `advance`",
    );
    let half = REDELIVERY_DELAY / 2;
    time::advance(half).await;
    coordinator.fire_due_timers().await;
    assert!(
        quiescent(coordinator) && stream.next().now_or_never().is_none(),
        "counts: a delayed nack of {REDELIVERY_DELAY:?} came back after {half:?}",
    );
    time::advance(REDELIVERY_DELAY.saturating_sub(half)).await;
    coordinator.fire_due_timers().await;
    assert!(
        !quiescent(coordinator),
        "counts: a delayed redelivery must be counted in flight once its delay ran out and \
         TestApp::advance fired the due timers; schedule it with \
         `Coordinator::schedule_redelivery`",
    );
    settle_ack(expect_next(stream, "counts delayed redelivery").await).await;
    assert!(
        quiescent(coordinator),
        "counts: an acked delayed redelivery must be released exactly once",
    );
}

#[cfg(test)]
mod tests;
