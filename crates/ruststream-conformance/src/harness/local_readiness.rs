//! The readiness probe of a broker of the local family, held to its contract.

#![allow(clippy::future_not_send)]

use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::Stream;
use futures::task::{ArcWake, noop_waker, waker};
use ruststream_core::{
    AckError, Broker, ConnectedBroker, OutgoingMessage, Publisher, ReadyHint, Settle, Subscriber,
    SubscriptionSource,
};
use tokio::time::timeout;

use crate::helpers::unique_subject;

/// How long one step of the scenario may take.
const STEP_TIMEOUT: Duration = Duration::from_secs(2);

/// Checks that the probe a broker's subscriber returns from `ready_hint` keeps the contract of
/// [`ReadyHint`], for a broker of the local family.
///
/// The probe is taken before the stream opens, as the embedded app takes it, and the checks drive
/// the stream by hand with a waker of their own:
///
/// | Step | Asserts |
/// |---|---|
/// | after a publish, before any poll | the probe says a delivery may be ready |
/// | after a poll that returned `Pending` | a publish wakes the waker of that poll, and the probe says a delivery may be ready |
/// | with two deliveries queued, one taken | whenever the probe says nothing may be ready, a poll returns `Pending` |
/// | after the broker shut down | the probe says the stream may be ready, and the next poll yields the end |
///
/// A broker whose probe is the constant `true` passes: the suite checks that a probe never hides a
/// delivery, not that it is exact. A broker whose subscriber returns no probe passes the same way.
///
/// Run it on a current-thread Tokio runtime: it spawns nothing, and its timeouts read Tokio's
/// clock.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(any())]
/// # mod demo {
/// use ruststream::conformance::harness;
/// use ruststream::memory::local::{ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemorySource};
///
/// // The closures stay: their bounds are higher-ranked.
/// #[tokio::test(flavor = "current_thread")]
/// async fn the_local_bus_keeps_the_readiness_contract() {
///     harness::local_readiness(
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
/// Panics with a message naming the step when the probe says nothing may be ready while the
/// stream has a delivery or its end to yield, when a publish does not wake a poll that returned
/// `Pending`, or when a step hangs.
pub async fn local_readiness<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<B::Connected>,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&B::Connected) -> Pub,
{
    let subject = unique_subject("conformance.local.readiness");
    let connected = within("connect", make_broker().connect())
        .await
        .unwrap_or_else(|err| panic!("local_readiness: connect failed: {err}"));
    let mut subscriber = within("subscribe", make_source(&subject).subscribe(&connected))
        .await
        .unwrap_or_else(|err| panic!("local_readiness: subscribe failed: {err}"));
    let publisher = make_publisher(&connected);
    let probe = subscriber.ready_hint();
    let mut stream = pin!(subscriber.stream());
    let wakes = Arc::new(Wakes::default());
    let counting = waker(Arc::clone(&wakes));

    publish(&publisher, &subject, b"first").await;
    assert!(
        probe.may_be_ready(),
        "local_readiness: the probe said nothing may be ready after a publish and before the \
         first poll; a delivery waits",
    );
    settle(taken(poll(stream.as_mut(), &counting), "first")).await;
    assert!(
        poll(stream.as_mut(), &counting).is_pending(),
        "local_readiness: the stream yielded with nothing published",
    );

    let woken = wakes.count();
    publish(&publisher, &subject, b"second").await;
    assert!(
        wakes.count() > woken,
        "local_readiness: a publish did not wake the poll that returned `Pending`; the \
         stream registers the waker whenever it returns `Pending`",
    );
    assert!(
        probe.may_be_ready(),
        "local_readiness: the probe said nothing may be ready after a wake; the delivery that \
         woke the task waits",
    );
    settle(taken(poll(stream.as_mut(), &counting), "second")).await;

    publish(&publisher, &subject, b"third").await;
    publish(&publisher, &subject, b"fourth").await;
    settle(taken(poll(stream.as_mut(), &counting), "third")).await;
    if !probe.may_be_ready() {
        assert!(
            poll(stream.as_mut(), &noop_waker()).is_pending(),
            "local_readiness: the probe said nothing may be ready, yet the stream yielded a \
             delivery that waited in the queue",
        );
    }
    settle(taken(poll(stream.as_mut(), &counting), "fourth")).await;

    within("shutdown", connected.shutdown())
        .await
        .unwrap_or_else(|err| panic!("local_readiness: shutdown failed: {err}"));
    assert!(
        probe.may_be_ready(),
        "local_readiness: the probe said nothing may be ready after the shutdown; the stream \
         has its end to yield",
    );
    assert!(
        matches!(poll(stream.as_mut(), &counting), Poll::Ready(None)),
        "local_readiness: the stream did not end after the shutdown",
    );
}

/// Counts the wakes of the poll that registered it.
#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wakes {
    fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl ArcWake for Wakes {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Polls `stream` once with `waker`.
fn poll<S: Stream>(stream: Pin<&mut S>, waker: &Waker) -> Poll<Option<S::Item>> {
    stream.poll_next(&mut Context::from_waker(waker))
}

/// The delivery a poll yielded, panicking on anything else.
fn taken<M, E: std::fmt::Debug>(polled: Poll<Option<Result<M, E>>>, which: &str) -> M {
    match polled {
        Poll::Ready(Some(Ok(message))) => message,
        Poll::Ready(Some(Err(err))) => {
            panic!("local_readiness: the stream yielded an error at {which}: {err:?}")
        }
        Poll::Ready(None) => panic!("local_readiness: the stream ended at {which}"),
        Poll::Pending => panic!(
            "local_readiness: the stream returned `Pending` at {which}, though a delivery was \
             published and the stream was woken or polled afresh"
        ),
    }
}

/// Acknowledges `message`, accepting a transport that cannot acknowledge.
async fn settle<M: Settle>(message: M) {
    match within("ack", message.ack()).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("local_readiness: ack must succeed or be unsupported, got {other:?}"),
    }
}

/// Publishes `payload` to `subject`, panicking when the publish fails or hangs.
async fn publish<Pub: Publisher>(publisher: &Pub, subject: &str, payload: &[u8]) {
    within(
        "publish",
        publisher.publish(OutgoingMessage::new(subject, payload), None),
    )
    .await
    .unwrap_or_else(|err| panic!("local_readiness: a publish to {subject:?} failed: {err}"));
}

/// Awaits `work` for at most [`STEP_TIMEOUT`].
async fn within<Output>(step: &str, work: impl Future<Output = Output>) -> Output {
    timeout(STEP_TIMEOUT, work)
        .await
        .unwrap_or_else(|_| panic!("local_readiness: {step} hung for {STEP_TIMEOUT:?}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::Stream;
    use ruststream_core::{
        ConnectedBroker, MaybeSend, MaybeSendSync, ReadyHint, Subscriber, SubscriptionSource,
    };
    use ruststream_memory::local::{
        ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemorySource,
    };

    use super::local_readiness;

    /// A source whose probe answers `true` for its first `true_for` questions and `false` after.
    struct Lying<Src> {
        inner: Src,
        true_for: usize,
    }

    struct LyingSubscriber<Sub> {
        inner: Sub,
        asked: Arc<AtomicUsize>,
        true_for: usize,
    }

    struct Counted {
        asked: Arc<AtomicUsize>,
        true_for: usize,
    }

    impl ReadyHint for Counted {
        fn may_be_ready(&self) -> bool {
            self.asked.fetch_add(1, Ordering::Relaxed) < self.true_for
        }
    }

    impl<Sub: Subscriber> Subscriber for LyingSubscriber<Sub> {
        type Message = Sub::Message;
        type Error = Sub::Error;

        fn stream(
            &mut self,
        ) -> impl Stream<Item = Result<Self::Message, Self::Error>> + MaybeSend + '_ {
            self.inner.stream()
        }

        fn ready_hint(&self) -> impl ReadyHint + MaybeSendSync + 'static {
            Counted {
                asked: Arc::clone(&self.asked),
                true_for: self.true_for,
            }
        }
    }

    impl<C: ConnectedBroker, Src: SubscriptionSource<C>> SubscriptionSource<C> for Lying<Src> {
        type Subscriber = LyingSubscriber<Src::Subscriber>;
        type Copies = Src::Copies;

        fn name(&self) -> &str {
            self.inner.name()
        }

        async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
            Ok(LyingSubscriber {
                inner: self.inner.subscribe(connected).await?,
                asked: Arc::default(),
                true_for: self.true_for,
            })
        }
    }

    async fn run(true_for: usize) {
        local_readiness(
            LocalMemoryBroker::<16>::new,
            move |name| Lying {
                inner: LocalMemorySource::new(name),
                true_for,
            },
            |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_honest_probe_passes() {
        local_readiness(
            LocalMemoryBroker::<16>::new,
            |name| LocalMemorySource::new(name),
            |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_probe_that_always_says_ready_passes() {
        run(usize::MAX).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(
        expected = "local_readiness: the probe said nothing may be ready after a publish"
    )]
    async fn a_probe_that_never_says_ready_fails() {
        run(0).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(expected = "local_readiness: the probe said nothing may be ready after a wake")]
    async fn a_probe_that_misses_the_woken_delivery_fails() {
        run(1).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(
        expected = "local_readiness: the probe said nothing may be ready, yet the stream"
    )]
    async fn a_probe_that_hides_a_queued_delivery_fails() {
        run(2).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(
        expected = "local_readiness: the probe said nothing may be ready after the shutdown"
    )]
    async fn a_probe_that_hides_the_end_fails() {
        run(3).await;
    }
}
