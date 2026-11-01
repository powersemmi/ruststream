//! The readiness probe of a broker of the local family, held to its contract.

#![allow(clippy::future_not_send)]

use std::fmt::Debug;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::Stream;
use futures::task::{ArcWake, waker};
use ruststream_core::{
    AckError, Broker, ConnectedBroker, OutgoingMessage, Publisher, ReadyHint, Settle, Subscriber,
    SubscriptionSource,
};
use tokio::sync::Notify;
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
/// | a poll that returned `Pending` | a publish wakes the waker of that poll, within a deadline |
/// | after such a wake | whenever the probe says nothing may be ready, the next poll returns `Pending` rather than the delivery |
/// | with two deliveries queued | each one arrives by the same rule |
/// | after the broker shut down | the shutdown wakes the parked poll, and the stream yields the end |
///
/// A delivery may take a moment to reach the subscriber after its publish returned, as on a
/// socket: the suite waits for it within a deadline, and a probe that says nothing is ready
/// before the delivery has arrived is correct.
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
/// Panics with a message naming the step when the probe says nothing may be ready after a wake
/// while the stream yields the delivery that woke it, when a publish or the shutdown does not wake
/// a poll that returned `Pending`, or when a step hangs.
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

    // A delivery counts as arrived once the stream has yielded it or woken the task for it: a
    // transport may take a moment between the end of a publish and the delivery's arrival.
    publish(&publisher, &subject, b"first").await;
    settle(taken(
        arrive(stream.as_mut(), &probe, &wakes, None).await,
        "first",
    ))
    .await;

    let parked = park(stream.as_mut(), &wakes);
    publish(&publisher, &subject, b"second").await;
    let asked = awake(&wakes, &probe, parked, "a publish").await;
    settle(taken(
        arrive(stream.as_mut(), &probe, &wakes, Some(asked)).await,
        "second",
    ))
    .await;

    publish(&publisher, &subject, b"third").await;
    publish(&publisher, &subject, b"fourth").await;
    settle(taken(
        arrive(stream.as_mut(), &probe, &wakes, None).await,
        "third",
    ))
    .await;
    settle(taken(
        arrive(stream.as_mut(), &probe, &wakes, None).await,
        "fourth",
    ))
    .await;

    let parked = park(stream.as_mut(), &wakes);
    within("shutdown", connected.shutdown())
        .await
        .unwrap_or_else(|err| panic!("local_readiness: shutdown failed: {err}"));
    let asked = awake(&wakes, &probe, parked, "the shutdown").await;
    assert!(
        matches!(
            arrive(stream.as_mut(), &probe, &wakes, Some(asked)).await,
            Poll::Ready(None)
        ),
        "local_readiness: the stream did not end after the shutdown",
    );
}

/// Counts the wakes of the poll that registered it, and lets a step wait for the next one.
#[derive(Default)]
struct Wakes {
    count: AtomicUsize,
    woken: Notify,
}

impl Wakes {
    fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

impl ArcWake for Wakes {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.count.fetch_add(1, Ordering::Relaxed);
        arc_self.woken.notify_one();
    }
}

/// Polls the stream, which has nothing to yield, and returns the wake count it registered at.
fn park<S: Stream>(stream: Pin<&mut S>, wakes: &Arc<Wakes>) -> usize {
    let at = wakes.count();
    let counting = waker(Arc::clone(wakes));
    assert!(
        poll(stream, &counting).is_pending(),
        "local_readiness: the stream yielded with nothing published",
    );
    at
}

/// Waits for a wake after the count `parked` and answers what the probe says then: `cause` must
/// wake the poll that returned `Pending`, because the stream registers the waker whenever it
/// returns `Pending`.
async fn awake<P: ReadyHint>(wakes: &Wakes, probe: &P, parked: usize, cause: &str) -> bool {
    timeout(STEP_TIMEOUT, async {
        while wakes.count() <= parked {
            wakes.woken.notified().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "local_readiness: {cause} did not wake the poll that returned `Pending` within \
             {STEP_TIMEOUT:?}; the stream registers the waker whenever it returns `Pending`"
        )
    });
    probe.may_be_ready()
}

/// Polls until the stream yields something other than `Pending`.
///
/// `asked` is the answer of the probe to a wake the caller already waited for. Each `Pending` is followed by a wait for the next wake, and after the wake the probe is asked
/// before the poll: a probe that says nothing may be ready while the poll that follows yields a
/// delivery hid it from the app, which would have skipped that poll. A wake that finds nothing
/// yet, as a transport that is still receiving may cause, goes back to waiting.
async fn arrive<S, P>(
    mut stream: Pin<&mut S>,
    probe: &P,
    wakes: &Arc<Wakes>,
    mut asked: Option<bool>,
) -> Poll<Option<S::Item>>
where
    S: Stream,
    P: ReadyHint,
{
    let counting = waker(Arc::clone(wakes));
    loop {
        let at = wakes.count();
        match poll(stream.as_mut(), &counting) {
            Poll::Pending => {
                asked = Some(awake(wakes, probe, at, "the delivery").await);
            }
            ready @ Poll::Ready(_) => {
                assert!(
                    asked != Some(false),
                    "local_readiness: the probe said nothing may be ready after a wake, yet the \
                     stream yielded the delivery that woke the task; the app would have skipped \
                     the poll",
                );
                return ready;
            }
        }
    }
}

/// Polls `stream` once with `waker`.
fn poll<S: Stream>(stream: Pin<&mut S>, waker: &Waker) -> Poll<Option<S::Item>> {
    stream.poll_next(&mut Context::from_waker(waker))
}

/// The delivery a poll yielded, panicking on anything else.
fn taken<M, E: Debug>(polled: Poll<Option<Result<M, E>>>, which: &str) -> M {
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
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

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

        fn ready_hint(&self) -> impl ReadyHint + MaybeSendSync + 'static + use<Sub> {
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

    /// A source whose deliveries take `held` polls to arrive, as on a socket: each of those polls
    /// wakes the task and answers `Pending`, and the probe says nothing is ready until the
    /// delivery has arrived.
    struct InTransit<Src> {
        inner: Src,
        held: usize,
    }

    struct InTransitSubscriber<Sub> {
        inner: Sub,
        left: Arc<AtomicUsize>,
        held: usize,
    }

    struct Arrived<Probe> {
        inner: Probe,
        left: Arc<AtomicUsize>,
    }

    impl<Probe: ReadyHint> ReadyHint for Arrived<Probe> {
        fn may_be_ready(&self) -> bool {
            self.left.load(Ordering::Relaxed) == 0 && self.inner.may_be_ready()
        }
    }

    struct Hold<Inner> {
        inner: Pin<Box<Inner>>,
        left: Arc<AtomicUsize>,
        held: usize,
    }

    impl<Inner: Stream> Stream for Hold<Inner> {
        type Item = Inner::Item;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let left = self.left.load(Ordering::Relaxed);
            if left > 0 {
                self.left.store(left - 1, Ordering::Relaxed);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let polled = self.inner.as_mut().poll_next(cx);
            if polled.is_ready() {
                self.left.store(self.held, Ordering::Relaxed);
            }
            polled
        }
    }

    impl<Sub: Subscriber> Subscriber for InTransitSubscriber<Sub> {
        type Message = Sub::Message;
        type Error = Sub::Error;

        fn stream(
            &mut self,
        ) -> impl Stream<Item = Result<Self::Message, Self::Error>> + MaybeSend + '_ {
            Hold {
                inner: Box::pin(self.inner.stream()),
                left: Arc::clone(&self.left),
                held: self.held,
            }
        }

        fn ready_hint(&self) -> impl ReadyHint + MaybeSendSync + 'static + use<Sub> {
            Arrived {
                inner: self.inner.ready_hint(),
                left: Arc::clone(&self.left),
            }
        }
    }

    impl<C: ConnectedBroker, Src: SubscriptionSource<C>> SubscriptionSource<C> for InTransit<Src> {
        type Subscriber = InTransitSubscriber<Src::Subscriber>;
        type Copies = Src::Copies;

        fn name(&self) -> &str {
            self.inner.name()
        }

        async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
            Ok(InTransitSubscriber {
                inner: self.inner.subscribe(connected).await?,
                left: Arc::new(AtomicUsize::new(self.held)),
                held: self.held,
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_delivery_still_in_transit_after_the_publish_returns_passes() {
        local_readiness(
            LocalMemoryBroker::<16>::new,
            |name| InTransit {
                inner: LocalMemorySource::new(name),
                held: 2,
            },
            |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
        )
        .await;
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
    #[should_panic(expected = "local_readiness: the probe said nothing may be ready after a wake")]
    async fn a_probe_that_never_says_ready_fails() {
        run(0).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(expected = "local_readiness: the probe said nothing may be ready after a wake")]
    async fn a_probe_that_hides_the_end_fails() {
        run(1).await;
    }

    /// A source whose stream polls the bus with a waker of its own, so a publish never wakes the
    /// task that polled it.
    struct Deaf<Src>(Src);

    struct DeafSubscriber<Sub>(Sub);

    struct Muted<Inner>(Pin<Box<Inner>>);

    impl<Inner: Stream> Stream for Muted<Inner> {
        type Item = Inner::Item;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0
                .as_mut()
                .poll_next(&mut Context::from_waker(Waker::noop()))
        }
    }

    impl<Sub: Subscriber> Subscriber for DeafSubscriber<Sub> {
        type Message = Sub::Message;
        type Error = Sub::Error;

        fn stream(
            &mut self,
        ) -> impl Stream<Item = Result<Self::Message, Self::Error>> + MaybeSend + '_ {
            Muted(Box::pin(self.0.stream()))
        }

        fn ready_hint(&self) -> impl ReadyHint + MaybeSendSync + 'static + use<Sub> {
            self.0.ready_hint()
        }
    }

    impl<C: ConnectedBroker, Src: SubscriptionSource<C>> SubscriptionSource<C> for Deaf<Src> {
        type Subscriber = DeafSubscriber<Src::Subscriber>;
        type Copies = Src::Copies;

        fn name(&self) -> &str {
            self.0.name()
        }

        async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
            Ok(DeafSubscriber(self.0.subscribe(connected).await?))
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[should_panic(expected = "local_readiness: a publish did not wake the poll")]
    async fn a_stream_that_does_not_wake_on_a_publish_fails() {
        local_readiness(
            LocalMemoryBroker::<16>::new,
            |name| Deaf(LocalMemorySource::new(name)),
            |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
        )
        .await;
    }
}
