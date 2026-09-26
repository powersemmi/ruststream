//! The in-process transport held to what the test harness relies on, and to the real server.
//!
//! [`TestApp`](crate::testing::TestApp) drives a service through a broker's in-process transport.
//! It counts the deliveries in flight through the [`Coordinator`] the transport reports to, reads
//! the transport's publish log back for its assertions, and, live, waits on the subscriptions
//! [`TestableBroker::routes`] names. A transport that miscounts ends a settle before the handler
//! ran, and one whose routing answer differs from its deliveries waits forever or not at all, so
//! each of those is checked here.
//!
//! [`run_suite`](super::harness::run_suite) runs the checks that need nothing from the broker:
//!
//! * the publish log records what the broker's own publisher sent, not only what a test injected
//!   (for a broker registered with [`register_testable_broker!`](crate::register_testable_broker),
//!   whose default publisher the harness pairs);
//! * two subscriptions of one name receive what [`routes`](TestableBroker::routes) says they
//!   receive: every message each where the broker fans out, and each message once between them
//!   where they compete, shared between them unless the routing answer names the one it goes to;
//! * the [`Coordinator`] counts balance on a paused clock, the way `TestApp` drives it: a message
//!   is counted in flight before the publish returns, released once when it is settled, counted
//!   again by a requeue, left out when no subscription receives it, and a delayed redelivery is
//!   counted by the time its timer falls due.
//!
//! Two opt-in suites hold what the in-process transport declares to what the server does. They
//! connect the broker twice per probe, with [`Broker::connect`](crate::Broker::connect) and with
//! [`InProcess::connect_in_process`], so run them where the broker's live suite runs:
//!
//! * [`backlog_matches_server`]: a message published before a subscription opens reaches it on
//!   both transports exactly as [`TestableBroker::backlog`] declares;
//! * [`refuses_like_the_server`]: each [`Refusal`] the broker supplies (a payload over the limit, a
//!   refused destination, a refused subscription) is refused by both transports.
//!
//! # Examples
//!
//! ```no_run
//! # #[cfg(feature = "memory")]
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

use std::{
    any::Any, collections::BTreeMap, fmt, panic::resume_unwind, pin::pin, thread, time::Duration,
};

use futures::{FutureExt, Stream, StreamExt, stream};
use tokio::{runtime, time::timeout};

use super::harness::{expect_next, expect_no_more};
use super::helpers::unique_subject;
use crate::{
    AckError, Connected, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    RawMessage, Subscribe, Subscriber, SubscriptionSource,
    testing::{Backlog, Coordinator, InProcess, TestableBroker, TestableRegistration},
};

/// How long a scenario waits for a delivery the in-process transport owes.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a scenario waits for a delivery nothing owes before it concludes none is coming.
const QUIET: Duration = Duration::from_millis(100);
/// How long a probe against the server waits for a delivery: a live subscription can take seconds
/// to be assigned its partitions or its queue.
const SERVER_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a probe against the server waits before it concludes a refused publish was not
/// delivered after all.
const SERVER_QUIET: Duration = Duration::from_millis(500);
/// The delay the counting scenario redelivers with. Whole seconds, so a transport whose delays
/// have second granularity schedules it unrounded; the clock is paused, so it costs nothing.
const REDELIVERY_DELAY: Duration = Duration::from_secs(2);
/// How many messages the two subscriptions of one name share.
const SHARED: u32 = 10;

/// Runs every in-process scenario against a fresh broker from `factory`.
pub(crate) async fn run<B, F>(factory: F)
where
    B: InProcess,
    B::Connected: Subscribe,
    F: Fn() -> B,
{
    own_publishes_are_logged(connect_in_process(factory()).await).await;
    one_name_two_subscriptions(connect_in_process(factory()).await).await;
    counts_balance(factory());
}

async fn connect_in_process<B: InProcess>(broker: B) -> B::Connected {
    broker
        .connect_in_process()
        .await
        .expect("broker must connect in process before a suite scenario")
}

/// The registration the harness finds `connected`'s broker by, when the broker has one.
fn registration_of<C: TestableBroker + 'static>(
    connected: &C,
) -> Option<&'static TestableRegistration> {
    inventory::iter::<TestableRegistration>
        .into_iter()
        .find(|registration| registration.resolve(connected as &dyn Any).is_some())
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
async fn settle_ack<M: IncomingMessage>(msg: M) {
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
        assert!(
            connected.published(&name).is_empty(),
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
async fn one_name_two_subscriptions<C: TestableBroker + Subscribe>(connected: C) {
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
    // opened.
    let second = Subscribe::subscribe(&connected, &name).await.ok();
    let paired = second.is_some();
    subscribers.extend(second.map(|second| (name.as_str(), second)));
    let third = Subscribe::subscribe(&connected, &other).await.ok();
    subscribers.extend(third.map(|third| (other.as_str(), third)));
    let names: Vec<&str> = subscribers.iter().map(|(name, _)| *name).collect();

    // Published the way a service publishes, which is what `routes` answers for.
    let registration = registration_of(&connected);
    let mut routed = Vec::new();
    for message in 0..SHARED {
        let answer = connected.routes(&name, &names);
        let taken = publish_as_service(
            &connected,
            registration,
            OutgoingMessage::new(name.as_str(), message.to_be_bytes().as_slice()),
        )
        .await;
        // A refused publish reaches nobody, whatever `routes` would answer for one the broker
        // took; the harness asks `routes` only about publishes that were taken.
        routed.push(if taken { answer } else { Vec::new() });
    }

    let owed: usize = routed.iter().map(Vec::len).sum();
    let received = {
        let mut merged = stream::select_all(subscribers.iter_mut().enumerate().map(
            |(position, (_, subscriber))| {
                Box::pin(subscriber.stream().map(move |item| (position, item)))
            },
        ));
        let mut received = Vec::new();
        while received.len() < owed {
            match timeout(DELIVERY_TIMEOUT, merged.next()).await {
                Ok(Some(delivery)) => received.push(read_shared(delivery).await),
                Ok(None) => panic!("one_name_two_subscriptions: a subscription stream ended"),
                // What arrived is compared with what was owed below, which names the difference.
                Err(_) => break,
            }
        }
        while let Ok(Some(delivery)) = timeout(QUIET, merged.next()).await {
            received.push(read_shared(delivery).await);
        }
        received
    };

    for (message, answer) in (0..SHARED).zip(&routed) {
        let expected = per_name(&names, answer.iter().copied());
        let delivered = per_name(
            &names,
            received
                .iter()
                .filter(|delivery| delivery.message == message)
                .map(|delivery| delivery.position),
        );
        assert_eq!(
            delivered, expected,
            "message {message} published to {name:?} with subscriptions {names:?}: `routes` \
             answers {answer:?}, so each name must receive it that many times; a live TestApp \
             waits on that answer",
        );
    }

    if paired {
        shared_unless_routed_to_one(&routed, &received);
    }

    assert!(
        quiescent(&coordinator),
        "one_name_two_subscriptions: every delivery was settled, yet the coordinator still counts \
         one in flight; each delivery is counted once by `enqueued` and released once by \
         `consumed`",
    );
    drop(subscribers);
    connected.shutdown().await.expect("shutdown failed");
}

/// Reads one delivery of the two-subscription scenario and settles it.
async fn read_shared<M, E>((position, item): (usize, Result<M, E>)) -> Received
where
    M: IncomingMessage,
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

/// How many of `positions` fall on each subscription name.
fn per_name<'a>(
    names: &[&'a str],
    positions: impl Iterator<Item = usize>,
) -> BTreeMap<&'a str, usize> {
    let mut counts = BTreeMap::new();
    for position in positions {
        *counts.entry(names[position]).or_insert(0) += 1;
    }
    counts
}

/// Where the two subscriptions of one name compete (the routing answer names one of them per
/// message), the messages are shared between them, unless the answer names the one every message
/// goes to (a log whose one partition is assigned to one member).
fn shared_unless_routed_to_one(routed: &[Vec<usize>], received: &[Received]) {
    let pair = |position: &usize| *position < 2;
    let competing = routed
        .iter()
        .all(|answer| answer.iter().filter(|position| pair(position)).count() == 1);
    if !competing {
        return;
    }
    for member in 0..2 {
        let took_all = received
            .iter()
            .filter(|delivery| pair(&delivery.position))
            .all(|delivery| delivery.position == member);
        let named_every_time = routed.iter().all(|answer| answer.contains(&member));
        assert!(
            !took_all || named_every_time,
            "two subscriptions of one name compete for its messages, yet subscription {member} \
             received all {SHARED} while `routes` named the other one for some: a competing \
             transport shares the messages between its consumers, or its routing answer names the \
             one each message goes to",
        );
    }
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
    B: InProcess,
    B::Connected: Subscribe,
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
    B: InProcess,
    B::Connected: Subscribe,
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
        assert!(
            !quiescent(&coordinator),
            "counts: a message injected into a subscription must be counted in flight \
             (`Coordinator::enqueued`) before `inject` returns; TestApp would stop waiting before \
             the handler ran",
        );
        let msg = expect_next(&mut stream, "counts injected").await;
        assert!(
            !quiescent(&coordinator),
            "counts: a delivery in the handler's hands must stay counted until it is settled",
        );
        settle_ack(msg).await;
        assert!(
            quiescent(&coordinator),
            "counts: an acked delivery must be released exactly once (`Coordinator::consumed`, \
             from its `Drop`)",
        );

        if let Some(registration) = registration {
            let reaches = !connected.routes(&name, &[name.as_str()]).is_empty();
            let taken = publish_as_service(
                &connected,
                Some(registration),
                OutgoingMessage::new(name.as_str(), b"own".as_slice()),
            )
            .await;
            if taken && reaches {
                assert!(
                    !quiescent(&coordinator),
                    "counts: a message the broker's own publisher sent must be counted in flight \
                     by the time the publish returns; a handler's publish is followed by its \
                     settlement, and TestApp would stop waiting in between",
                );
                settle_ack(expect_next(&mut stream, "counts own publish").await).await;
            }
            assert!(
                quiescent(&coordinator),
                "counts: a delivery of the broker's own publish must be released exactly once, \
                 and a publish that reaches no subscription must not be counted",
            );
        }

        // Where the broker routes by name, a publish to another one reaches no subscription; a
        // transport that routes by connection (a bound queue socket) delivers it all the same.
        let unread = unique_subject("conformance.unread");
        let reaches = !connected.routes(&unread, &[name.as_str()]).is_empty();
        let taken = publish_as_service(
            &connected,
            registration_of(&connected),
            OutgoingMessage::new(unread.as_str(), b"unread".as_slice()),
        )
        .await;
        if taken && reaches {
            assert!(
                !quiescent(&coordinator),
                "counts: `routes` says the subscription receives a publish to {unread:?}, so it \
                 must be counted in flight",
            );
            settle_ack(expect_next(&mut stream, "counts routed elsewhere").await).await;
        }
        assert!(
            quiescent(&coordinator),
            "counts: a publish no subscription receives must not be counted in flight; TestApp \
             would wait for a handler that never runs",
        );

        settlement_counts(&coordinator, &mut stream, inject).await;
    }
    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

/// Each settlement releases the delivery once, and each redelivery is counted again.
async fn settlement_counts<M, E, S>(
    coordinator: &Coordinator,
    stream: &mut S,
    inject: impl Fn(&'static [u8]),
) where
    M: IncomingMessage,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let coordinator = coordinator.clone();
    let mut stream = stream;
    {
        inject(b"dropped");
        let msg = expect_next(&mut stream, "counts nack(false)").await;
        match msg.nack(false).await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
        }
        assert!(
            quiescent(&coordinator),
            "counts: a delivery nacked without requeue must be released exactly once",
        );
        expect_no_more(&mut stream, "counts nack(false)").await;

        inject(b"requeued");
        let msg = expect_next(&mut stream, "counts nack(true)").await;
        match msg.nack(true).await {
            Ok(()) => {
                assert!(
                    !quiescent(&coordinator),
                    "counts: a requeued message must be counted in flight again before `nack` \
                     returns; TestApp would stop waiting before the redelivery was handled",
                );
                settle_ack(expect_next(&mut stream, "counts requeued").await).await;
                assert!(
                    quiescent(&coordinator),
                    "counts: an acked redelivery must be released exactly once",
                );
            }
            Err(AckError::Unsupported) => assert!(
                quiescent(&coordinator),
                "counts: a delivery whose requeue is unsupported must still be released",
            ),
            Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
        }

        inject(b"delayed");
        let msg = expect_next(&mut stream, "counts nack_after").await;
        if msg.supports_nack_after() {
            delayed_redelivery(msg, &coordinator, &mut stream).await;
        } else {
            settle_ack(msg).await;
        }

        // Last, because a transport may take an unsettled delivery back on its own schedule.
        inject(b"unsettled");
        drop(expect_next(&mut stream, "counts unsettled drop").await);
        if !quiescent(&coordinator) {
            // Counted again: the transport took the delivery back, so it must come back.
            settle_ack(expect_next(&mut stream, "counts unsettled redelivery").await).await;
            assert!(
                quiescent(&coordinator),
                "counts: an unsettled delivery the transport took back is counted once more than \
                 it redelivers",
            );
        }
    }
}

/// A delayed nack releases the delivery at once and counts the redelivery when its timer falls
/// due, the way `TestApp::advance` moves the clock and then fires the due timers.
async fn delayed_redelivery<M, E, S>(msg: M, coordinator: &Coordinator, stream: &mut S)
where
    M: IncomingMessage,
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
    tokio::time::advance(half).await;
    coordinator.fire_due_timers().await;
    assert!(
        quiescent(coordinator) && stream.next().now_or_never().is_none(),
        "counts: a delayed nack of {REDELIVERY_DELAY:?} came back after {half:?}",
    );
    tokio::time::advance(REDELIVERY_DELAY.saturating_sub(half)).await;
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

/// Which transport a probe runs against.
#[derive(Debug, Clone, Copy)]
enum Transport {
    /// [`Broker::connect`](crate::Broker::connect): the real server.
    Server,
    /// [`InProcess::connect_in_process`].
    InProcess,
}

impl Transport {
    async fn connect<B: InProcess>(self, broker: B) -> B::Connected {
        match self {
            Self::Server => broker
                .connect()
                .await
                .unwrap_or_else(|err| panic!("the broker must connect to its server: {err}")),
            Self::InProcess => connect_in_process(broker).await,
        }
    }

    const fn delivery_timeout(self) -> Duration {
        match self {
            Self::Server => SERVER_TIMEOUT,
            Self::InProcess => DELIVERY_TIMEOUT,
        }
    }

    const fn quiet(self) -> Duration {
        match self {
            Self::Server => SERVER_QUIET,
            Self::InProcess => QUIET,
        }
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Server => "against the server",
            Self::InProcess => "in process",
        })
    }
}

/// The next delivery on `stream`, within what `transport` allows.
async fn next_on<S, M, E>(stream: &mut S, transport: Transport, label: &str) -> M
where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    timeout(transport.delivery_timeout(), stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label} {transport}: no delivery arrived"))
        .unwrap_or_else(|| panic!("{label} {transport}: the stream ended"))
        .unwrap_or_else(|err| panic!("{label} {transport}: the stream yielded an error: {err:?}"))
}

/// Checks that the in-process transport's [`TestableBroker::backlog`] declaration is what the real
/// server does with a message published before a subscription opens.
///
/// [`run_suite`](super::harness::run_suite) holds the in-process transport to its declaration; this
/// holds the declaration to the server. On each transport, a fresh subject gets one message through
/// the publisher `make_publisher` builds, then a subscription opened by name, then a second
/// message. Under [`Backlog::Delivered`] the subscription receives both, the earlier one first;
/// under [`Backlog::Missed`] it receives the later one only. The declaration is read off the
/// in-process connected form, configured by `make_broker` as the server's is.
///
/// It connects with [`Broker::connect`](crate::Broker::connect), so run it where the broker's live suite runs.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::in_process;
/// use ruststream::memory::MemoryBroker;
///
/// in_process::backlog_matches_server(MemoryBroker::new, |connected| connected.publisher()).await;
/// # }
/// ```
///
/// # Panics
///
/// Panics when either transport delivers the earlier message against the declaration, when a
/// publish or the subscription fails, or when the later message does not arrive.
pub async fn backlog_matches_server<B, MkBroker, Pub, MkPub>(
    make_broker: MkBroker,
    make_publisher: MkPub,
) where
    B: InProcess,
    B::Connected: Subscribe,
    MkBroker: Fn() -> B,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let declared = {
        let connected = connect_in_process(make_broker()).await;
        let declared = connected.backlog();
        connected.shutdown().await.expect("shutdown failed");
        declared
    };

    // Why the first publish may fail: a server that refuses a publish to a name nothing has
    // declared yet keeps no backlog either, and the in-process transport must refuse it too.
    let mut server_refused = None;
    for transport in [Transport::Server, Transport::InProcess] {
        let connected = transport.connect(make_broker()).await;
        let subject = unique_subject("conformance.backlog");
        let publisher = make_publisher(&connected);
        let before = publisher
            .publish(
                OutgoingMessage::new(subject.as_str(), b"before-subscribe".as_slice()),
                None,
            )
            .await
            .map_err(|err| err.to_string());
        match (transport, before, server_refused.as_deref()) {
            (Transport::Server, Err(reason), _) => {
                server_refused = Some(reason);
                connected.shutdown().await.expect("shutdown failed");
                continue;
            }
            (Transport::InProcess, Ok(()), Some(reason)) => panic!(
                "backlog_matches_server: the server refused a publish to a name no subscription \
                 had opened ({reason}), and the in-process transport accepted it; a test passes \
                 where production fails"
            ),
            (Transport::InProcess, Err(_), Some(_)) => {
                connected.shutdown().await.expect("shutdown failed");
                return;
            }
            (Transport::InProcess, Err(reason), None) => panic!(
                "backlog_matches_server in process: a publish to {subject:?} was refused \
                 ({reason}), and the server accepted it; a test fails where production succeeds"
            ),
            (_, Ok(()), _) => {}
        }
        let publish = async |payload: &[u8]| {
            publisher
                .publish(OutgoingMessage::new(subject.as_str(), payload), None)
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "backlog_matches_server {transport}: a publish to {subject:?} failed: {err}"
                    )
                });
        };

        let mut subscriber = Subscribe::subscribe(&connected, &subject)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "backlog_matches_server {transport}: subscribing to {subject:?} failed: {err}"
                )
            });
        publish(b"after-subscribe").await;

        {
            let mut stream = pin!(subscriber.stream());
            let first = next_on(&mut stream, transport, "backlog_matches_server").await;
            let expected: &[u8] = match declared {
                Backlog::Delivered => b"before-subscribe",
                Backlog::Missed => b"after-subscribe",
            };
            assert_eq!(
                first.payload(),
                expected,
                "backlog_matches_server {transport}: the in-process transport declares \
                 `Backlog::{declared:?}`, so a subscription opened after a publish must first \
                 receive {:?}",
                String::from_utf8_lossy(expected),
            );
            settle_ack(first).await;
            if declared == Backlog::Delivered {
                let second = next_on(&mut stream, transport, "backlog_matches_server").await;
                assert_eq!(
                    second.payload(),
                    b"after-subscribe",
                    "backlog_matches_server {transport}: the message published after the \
                     subscription opened comes next",
                );
                settle_ack(second).await;
            }
        }
        drop(subscriber);
        connected.shutdown().await.expect("shutdown failed");
    }
}

/// One thing the real server refuses, which [`refuses_like_the_server`] checks the in-process
/// transport refuses too.
///
/// `Src` is the broker's subscription descriptor; a list with no subscription probe names it with
/// a turbofish (`Refusal::<MySource>::Publish { .. }`).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # {
/// use ruststream::conformance::in_process::Refusal;
/// use ruststream::memory::MemorySource;
///
/// let refusals = [
///     Refusal::Subscription {
///         source: MemorySource::new("orders.*"),
///     },
///     Refusal::PayloadOver {
///         name: "orders".to_owned(),
///         limit: 1024 * 1024,
///     },
/// ];
/// # let _ = refusals;
/// # }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Refusal<Src> {
    /// A publish to `name` of exactly `limit` payload bytes is accepted and delivered, one byte
    /// more is refused. The check opens a subscription to `name` by name before it publishes.
    PayloadOver {
        /// A destination both transports accept publishes to.
        name: String,
        /// The largest payload the server accepts there, in bytes.
        limit: usize,
    },
    /// A publish to `name` is refused: a name the server's grammar rejects, or a destination it
    /// does not have.
    Publish {
        /// The refused destination.
        name: String,
    },
    /// Opening `source` is refused: an invalid name, a declaration the server rejects.
    Subscription {
        /// The refused subscription.
        source: Src,
    },
    /// With `open` subscribed, opening `refused` is refused: a redeclaration that conflicts with
    /// the first one, a second consumer where the server admits one.
    Conflicting {
        /// The subscription opened first, which the server accepts.
        open: Src,
        /// The subscription the server refuses beside it.
        refused: Src,
    },
}

/// Checks that the in-process transport refuses what the real server refuses.
///
/// Each [`Refusal`] runs on a fresh connection to the server first, then on a fresh in-process
/// connection, and both must refuse it. A probe the server accepts fails as such, before the
/// in-process transport is blamed: it proves nothing about it. [`Refusal::PayloadOver`] also
/// requires a payload of exactly the limit to be accepted and delivered on both, so a transport
/// that refuses everything, or refuses one byte early, fails too.
///
/// `make_broker` builds the broker configured as a deployment configures it, and `make_publisher`
/// the publisher the probes publish through. It connects with [`Broker::connect`](crate::Broker::connect), so run it where
/// the broker's live suite runs.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::in_process::{self, Refusal};
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// in_process::refuses_like_the_server(
///     MemoryBroker::new,
///     |connected| connected.publisher(),
///     [Refusal::Subscription {
///         source: MemorySource::new("orders.*"),
///     }],
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics when `refusals` is empty, when the server accepts a probe, and when the in-process
/// transport accepts one the server refused.
pub async fn refuses_like_the_server<B, MkBroker, Src, Pub, MkPub>(
    make_broker: MkBroker,
    make_publisher: MkPub,
    refusals: impl IntoIterator<Item = Refusal<Src>> + Send,
) where
    B: InProcess,
    B::Connected: Subscribe,
    MkBroker: Fn() -> B + Send,
    Src: SubscriptionSource<Connected<B>> + Clone + Send + Sync,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub + Send + Sync,
{
    let refusals: Vec<Refusal<Src>> = refusals.into_iter().collect();
    assert!(
        !refusals.is_empty(),
        "pass at least one probe the server refuses; an empty list passes whatever the in-process \
         transport accepts",
    );
    for refusal in &refusals {
        let mut refused = [false; 2];
        for (leg, transport) in [Transport::Server, Transport::InProcess]
            .into_iter()
            .enumerate()
        {
            let connected = transport.connect(make_broker()).await;
            refused[leg] = probe(&connected, refusal, transport, &make_publisher).await;
            connected.shutdown().await.expect("shutdown failed");
        }
        let what = describe::<Connected<B>, Src>(refusal);
        match refused {
            [true, true] => {}
            [true, false] => panic!(
                "refuses_like_the_server: {what}: the server refused it and the in-process \
                 transport accepted it; a test passes where production fails"
            ),
            [false, true] => panic!(
                "refuses_like_the_server: {what}: the in-process transport refused it and the \
                 server accepted it; a test fails where production succeeds"
            ),
            [false, false] => panic!(
                "refuses_like_the_server: {what}: the server accepted it, so it proves nothing; \
                 pass a probe the server refuses"
            ),
        }
    }
}

/// The probe as a failure names it.
fn describe<C, Src>(refusal: &Refusal<Src>) -> String
where
    C: ConnectedBroker,
    Src: SubscriptionSource<C>,
{
    match refusal {
        Refusal::PayloadOver { name, limit } => format!(
            "a payload of {} bytes to {name:?}, one over the limit",
            limit + 1
        ),
        Refusal::Publish { name } => format!("a publish to {name:?}"),
        Refusal::Subscription { source } => format!("the subscription {:?}", source.name()),
        Refusal::Conflicting { open, refused } => format!(
            "the subscription {:?} beside {:?}",
            refused.name(),
            open.name()
        ),
    }
}

/// Runs one refusal probe on `connected`, answering whether it was refused.
async fn probe<C, Src, Pub, MkPub>(
    connected: &C,
    refusal: &Refusal<Src>,
    transport: Transport,
    make_publisher: &MkPub,
) -> bool
where
    C: Subscribe,
    Src: SubscriptionSource<C> + Clone + Sync,
    Pub: Publisher,
    MkPub: Fn(&C) -> Pub + Sync,
{
    match refusal {
        Refusal::PayloadOver { name, limit } => {
            let mut subscriber = Subscribe::subscribe(connected, name).await.unwrap_or_else(
                |err| {
                    panic!(
                        "refuses_like_the_server {transport}: subscribing to {name:?} failed: {err}"
                    )
                },
            );
            let publisher = make_publisher(connected);
            let at_limit = vec![b'x'; *limit];
            if let Err(err) = publisher
                .publish(
                    OutgoingMessage::new(name.as_str(), at_limit.as_slice()),
                    None,
                )
                .await
            {
                panic!(
                    "refuses_like_the_server {transport}: a payload of exactly {limit} bytes to \
                     {name:?} was refused: {err}"
                );
            }
            let over = vec![b'x'; limit + 1];
            let refused = publisher
                .publish(OutgoingMessage::new(name.as_str(), over.as_slice()), None)
                .await
                .is_err();
            let mut stream = pin!(subscriber.stream());
            let delivered = next_on(&mut stream, transport, "refuses_like_the_server").await;
            assert_eq!(
                delivered.payload().len(),
                *limit,
                "refuses_like_the_server {transport}: the payload of exactly the limit must be \
                 delivered first",
            );
            settle_ack(delivered).await;
            if refused {
                assert!(
                    timeout(transport.quiet(), stream.next()).await.is_err(),
                    "refuses_like_the_server {transport}: the payload over the limit was refused, \
                     and delivered anyway",
                );
            }
            refused
        }
        Refusal::Publish { name } => make_publisher(connected)
            .publish(
                OutgoingMessage::new(name.as_str(), b"refused".as_slice()),
                None,
            )
            .await
            .is_err(),
        Refusal::Subscription { source } => source.clone().subscribe(connected).await.is_err(),
        Refusal::Conflicting { open, refused } => {
            let first = open.clone().subscribe(connected).await.unwrap_or_else(|err| {
                panic!(
                    "refuses_like_the_server {transport}: the subscription {:?} opened first was \
                     refused: {err}",
                    open.name(),
                )
            });
            let refused = refused.clone().subscribe(connected).await.is_err();
            drop(first);
            refused
        }
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests;
