//! The suites of the local family: the routing contract and the lifecycle ladder for a broker
//! whose types are not `Send`.

// The suites drive brokers whose types are not `Send` on the caller's task, so their futures are
// not `Send` by design.
#![allow(clippy::future_not_send)]

use std::fmt;
use std::pin::pin;
use std::time::Duration;

use futures::{FutureExt, Stream};
use ruststream_core::{
    AckError, HeaderMap, LocalBroker, LocalConnectedBroker, LocalDefaultPublish,
    LocalIncomingMessage, LocalPublishPolicy, LocalPublisher, LocalSubscribe, LocalSubscriber,
    LocalSubscriptionSource, OutgoingMessage, RawMessage,
};
use ruststream_runtime::testing::{Coordinator, LocalInProcess, LocalTestableBroker};
use tokio::time::timeout;

use super::routing::{
    ack_consumes_delivery, headers_propagate, nack_with_requeue_redelivers,
    nack_without_requeue_drops, ordering, publish_before_subscribe,
    published_log_observes_publishes,
};
use super::{expect_next, expect_no_more};
use crate::helpers::unique_subject;

/// How long one step of the local lifecycle may take.
const STEP_TIMEOUT: Duration = Duration::from_secs(2);

/// The live publisher a broker's default policy pairs into.
type LocalLive<C> = <<C as LocalDefaultPublish>::Policy as LocalPublishPolicy<C>>::Live;

/// Runs the routing contract against a broker of the local family, panicking with a descriptive
/// message on the first failure.
///
/// It is [`run_suite`](super::run_suite) for a broker whose types are not `Send`: the same
/// scenarios, driven on the caller's task, so nothing of the broker crosses a thread. `factory`
/// builds a fresh production broker per scenario, and each scenario connects it through its
/// [`LocalInProcess`] transition.
///
/// # What it checks
///
/// | Scenario | Asserts |
/// |---|---|
/// | ordering, publish before subscribe, ack, nack with and without requeue, headers, publish log | as in [`run_suite`](super::run_suite) |
/// | own publishes | `published(name)` records what the broker's default publisher sent beside what was injected, in order and with headers |
/// | coordinator counts | a delivery is counted in flight before the publish returns and released once when settled; a requeue counts it again; a publish no subscription receives is not counted |
///
/// Run it on a current-thread Tokio runtime: it spawns nothing, so its futures need not be
/// `Send`, and its timeouts read Tokio's clock.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(any())]
/// # mod demo {
/// use ruststream::conformance::harness;
/// use ruststream::memory::local::LocalMemoryBroker;
///
/// #[tokio::test(flavor = "current_thread")]
/// async fn the_local_bus_passes_the_routing_suite() {
///     harness::run_local_suite(LocalMemoryBroker::<16>::new).await;
/// }
/// # }
/// ```
///
/// # Panics
///
/// Panics if any scenario fails an assertion. The panic message identifies the scenario.
pub async fn run_local_suite<B, F>(factory: F)
where
    B: LocalInProcess,
    B::Connected: LocalSubscribe + LocalDefaultPublish,
    LocalLive<B::Connected>: LocalPublisher,
    F: Fn() -> B,
{
    let connect = async |broker: B| {
        broker
            .connect_in_process()
            .await
            .expect("broker must connect in process before a suite scenario")
    };
    ordering(connect(factory()).await).await;
    publish_before_subscribe(connect(factory()).await).await;
    ack_consumes_delivery(connect(factory()).await).await;
    nack_with_requeue_redelivers(connect(factory()).await).await;
    nack_without_requeue_drops(connect(factory()).await).await;
    headers_propagate(connect(factory()).await).await;
    published_log_observes_publishes(connect(factory()).await).await;
    own_publishes_are_logged(connect(factory()).await).await;
    counts(connect(factory()).await).await;
}

/// Publishes through the broker's default policy, the way the runtime publishes a reply.
async fn publish_own<C>(connected: &C, msg: OutgoingMessage<'_>)
where
    C: LocalDefaultPublish,
    LocalLive<C>: LocalPublisher,
{
    let what = format!("{:?}", msg.name());
    let publisher = C::Policy::default()
        .pair(connected)
        .await
        .unwrap_or_else(|err| panic!("the broker's default policy must pair: {err}"));
    let (name, payload, headers) = msg.into_parts();
    publisher
        .publish(
            OutgoingMessage::new(name, payload).with_headers(headers),
            None,
        )
        .await
        .unwrap_or_else(|err| {
            panic!("a publish to {what} through the broker's own default publisher failed: {err}")
        });
}

async fn own_publishes_are_logged<C>(connected: C)
where
    C: LocalTestableBroker + LocalSubscribe + LocalDefaultPublish,
    LocalLive<C>: LocalPublisher,
{
    let name = unique_subject("conformance.ownpublish");
    let subscriber = LocalSubscribe::subscribe(&connected, &name)
        .await
        .expect("subscribe failed");
    let mut headers = HeaderMap::new();
    headers.insert("x-conformance", "own");
    publish_own(
        &connected,
        OutgoingMessage::new(name.as_str(), b"first".as_slice()).with_headers(headers),
    )
    .await;
    connected.inject(OutgoingMessage::new(name.as_str(), b"injected".as_slice()));
    publish_own(
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
    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

fn quiescent(coordinator: &Coordinator) -> bool {
    coordinator.drive().now_or_never().is_some()
}

async fn counts<C>(connected: C)
where
    C: LocalTestableBroker + LocalSubscribe + LocalDefaultPublish,
    LocalLive<C>: LocalPublisher,
{
    let coordinator = Coordinator::new(usize::MAX);
    connected.install_coordinator(coordinator.clone());
    let name = unique_subject("conformance.counted");
    let mut subscriber = LocalSubscribe::subscribe(&connected, &name)
        .await
        .expect("subscribe failed");
    {
        let mut stream = pin!(subscriber.stream());
        assert!(
            quiescent(&coordinator),
            "counts: nothing is published yet, and the coordinator already counts a delivery in \
             flight",
        );

        connected.inject(OutgoingMessage::new(name.as_str(), b"injected".as_slice()));
        assert!(
            !quiescent(&coordinator),
            "counts: a message injected into a subscription must be counted in flight before \
             `inject` returns; TestApp would stop waiting before the handler ran",
        );
        let msg = expect_next(&mut stream, "counts injected").await;
        assert!(
            !quiescent(&coordinator),
            "counts: a delivery in the handler's hands must stay counted until it is settled",
        );
        settle_ack(msg).await;
        assert!(
            quiescent(&coordinator),
            "counts: an acked delivery must be released exactly once",
        );

        requeue_counts(&connected, &coordinator, &mut stream, &name).await;

        publish_own(
            &connected,
            OutgoingMessage::new(
                unique_subject("conformance.unread").as_str(),
                b"unread".as_slice(),
            ),
        )
        .await;
        assert!(
            quiescent(&coordinator),
            "counts: a publish no subscription receives must not be counted in flight; TestApp \
             would wait for a handler that never runs",
        );
    }
    drop(subscriber);
    connected.shutdown().await.expect("shutdown failed");
}

async fn requeue_counts<C, M, E, S>(
    connected: &C,
    coordinator: &Coordinator,
    stream: &mut S,
    name: &str,
) where
    C: LocalTestableBroker,
    M: LocalIncomingMessage,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    connected.inject(OutgoingMessage::new(name, b"requeued".as_slice()));
    let first = expect_next(stream, "counts requeue").await;
    match first.nack(true).await {
        Ok(()) => {
            assert!(
                !quiescent(coordinator),
                "counts: a requeued delivery must be counted in flight again before `nack` \
                 returns",
            );
            settle_ack(expect_next(stream, "counts redelivery").await).await;
        }
        Err(AckError::Unsupported) => {}
        Err(other) => panic!("nack must succeed or be unsupported, got: {other:?}"),
    }
    assert!(
        quiescent(coordinator),
        "counts: a requeued delivery must be released exactly once when settled",
    );
}

/// Acknowledges `msg`, accepting a transport that cannot acknowledge.
async fn settle_ack<M: LocalIncomingMessage>(msg: M) {
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
}

/// Verifies a broker of the local family honours the lifecycle ladder end to end.
///
/// It is [`lifecycle`](super::lifecycle) for a broker whose types are not `Send`: every step
/// runs on the caller's task, so the checks that move a handle to another thread's runtime do not
/// apply. The steps:
///
/// * **Connect** through [`LocalBroker::connect`], then open a subscription through the broker's
///   own [`LocalSubscriptionSource`], under a subject unique to the run.
/// * **Deliver and settle.** A publish arrives and is acknowledged. A second one settled with
///   `nack(requeue = true)` answering `Ok` must come back with the same payload and, where the
///   broker counts deliveries, a higher [`redelivery_count`](LocalIncomingMessage::redelivery_count);
///   `nack(requeue = false)` must not bring it back.
/// * **Shutdown** must return within two seconds.
/// * **Aliased handles.** After the shutdown, a publish must error through a publisher made
///   before it and never used, and through the one used before. A delivery received before the
///   shutdown and settled with `nack(requeue = true)` after it must answer an error, or come back
///   on its own subscription.
///
/// Run it on a current-thread Tokio runtime: it spawns nothing, and its timeouts read Tokio's
/// clock. A broker that offers [`nack_after`](LocalIncomingMessage::nack_after) on its own timer
/// is checked on that timer by its own crate.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(any())]
/// # mod demo {
/// use ruststream::conformance::harness;
/// use ruststream::memory::local::{ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemorySource};
///
/// #[tokio::test(flavor = "current_thread")]
/// async fn the_local_bus_passes_the_lifecycle() {
///     harness::local_lifecycle(
///         LocalMemoryBroker::<16>::new,
///         |name| LocalMemorySource::new(name),
///         |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
///     )
///     .await;
/// }
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message naming the step when connection, subscription, delivery, a
/// settlement, shutdown, or an aliased handle does not follow the contract.
pub async fn local_lifecycle<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: LocalBroker,
    MkBroker: Fn() -> B,
    Src: LocalSubscriptionSource<B::Connected>,
    MkSrc: Fn(&str) -> Src,
    Pub: LocalPublisher,
    MkPub: Fn(&B::Connected) -> Pub,
{
    let subject = unique_subject("conformance.local");
    let connected = within("connect", make_broker().connect())
        .await
        .unwrap_or_else(|err| panic!("local_lifecycle: connect failed: {err}"));
    let mut subscriber = within("subscribe", make_source(&subject).subscribe(&connected))
        .await
        .unwrap_or_else(|err| panic!("local_lifecycle: subscribe failed: {err}"));
    let publisher = make_publisher(&connected);
    let untouched = make_publisher(&connected);
    let mut stream = pin!(subscriber.stream());

    publish(&publisher, &subject, b"first").await;
    let first = expect_next(&mut stream, "local_lifecycle first").await;
    assert_eq!(
        first.payload(),
        b"first",
        "local_lifecycle: the subscription must receive what was published to it",
    );
    settle_ack(first).await;

    publish(&publisher, &subject, b"requeued").await;
    requeue_round_trip(&mut stream).await;

    publish(&publisher, &subject, b"held").await;
    let held = expect_next(&mut stream, "local_lifecycle held").await;
    within("shutdown", connected.shutdown())
        .await
        .unwrap_or_else(|err| panic!("local_lifecycle: shutdown failed: {err}"));
    after_shutdown(&subject, [&publisher, &untouched], held, &mut stream).await;
}

/// Settles one delivery with `nack(requeue = true)`, then its redelivery with
/// `nack(requeue = false)`.
async fn requeue_round_trip<M, E, S>(stream: &mut S)
where
    M: LocalIncomingMessage,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let delivered = expect_next(stream, "local_lifecycle requeue").await;
    let counted = delivered.redelivery_count();
    let back = match within("nack(requeue = true)", delivered.nack(true)).await {
        Ok(()) => Some(expect_next(stream, "local_lifecycle redelivery").await),
        Err(AckError::Unsupported) => None,
        Err(other) => panic!("local_lifecycle: nack must succeed or be unsupported, got {other:?}"),
    };
    if let Some(back) = back {
        assert_eq!(
            back.payload(),
            b"requeued",
            "local_lifecycle: nack(requeue = true) must redeliver the same payload",
        );
        if let (Some(before), Some(after)) = (counted, back.redelivery_count()) {
            assert!(
                after > before,
                "local_lifecycle: a redelivery must count one more delivery than the first \
                 ({before} then {after})",
            );
        }
        match back.nack(false).await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("local_lifecycle: nack(false) must succeed, got {other:?}"),
        }
        expect_no_more(stream, "local_lifecycle nack(requeue = false)").await;
    }
}

/// Holds the handles that outlive the shutdown to the contract: a publish errors, and a requeue
/// errors or brings the delivery back.
async fn after_shutdown<Pub, M, E, S>(subject: &str, publishers: [&Pub; 2], held: M, stream: &mut S)
where
    Pub: LocalPublisher,
    M: LocalIncomingMessage,
    E: fmt::Debug,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    for (which, handle) in [
        "a publisher used before the shutdown",
        "a publisher never used",
    ]
    .into_iter()
    .zip(publishers)
    {
        let late = within(
            "publish after shutdown",
            handle.publish(OutgoingMessage::new(subject, b"late".as_slice()), None),
        )
        .await;
        assert!(
            late.is_err(),
            "local_lifecycle: {which} published successfully after the shutdown; a handle \
             that outlives the connection must error, never succeed silently",
        );
    }
    if within("nack(requeue = true) after shutdown", held.nack(true))
        .await
        .is_ok()
    {
        {
            let back = expect_next(stream, "local_lifecycle requeue after shutdown").await;
            assert_eq!(
                back.payload(),
                b"held",
                "local_lifecycle: nack(requeue = true) after shutdown answered Ok, so the \
                 delivery must come back",
            );
        }
    }
}

/// Publishes `payload` to `subject`, panicking when the publish fails or hangs.
async fn publish<Pub: LocalPublisher>(publisher: &Pub, subject: &str, payload: &[u8]) {
    within(
        "publish",
        publisher.publish(OutgoingMessage::new(subject, payload), None),
    )
    .await
    .unwrap_or_else(|err| panic!("local_lifecycle: a publish to {subject:?} failed: {err}"));
}

/// Awaits `step` for at most [`STEP_TIMEOUT`].
async fn within<Output>(step: &str, work: impl Future<Output = Output>) -> Output {
    timeout(STEP_TIMEOUT, work)
        .await
        .unwrap_or_else(|_| panic!("local_lifecycle: {step} hung for {STEP_TIMEOUT:?}"))
}
