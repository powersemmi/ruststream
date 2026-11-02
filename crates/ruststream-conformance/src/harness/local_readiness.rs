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
    AckError, Broker, ConnectedBroker, Publisher, ReadyHint, Settle, Subscriber, SubscriptionSource,
};
use tokio::sync::Notify;
use tokio::time::{Instant, timeout, timeout_at};

use super::outgoing;
use crate::helpers::unique_subject;

/// How long one step of the scenario may take, the whole wait for a delivery included.
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
/// | after such a wake | the delivery arrives within a deadline through the polls the app makes: none while the probe says nothing may be ready |
/// | with two deliveries queued | each one arrives by the same rule |
/// | after the broker shut down | the shutdown wakes the parked poll, and the end arrives by the same rule |
///
/// The suite polls the stream the way the embedded app does: after a poll that returned `Pending`,
/// it polls again only after a wake, and only when the probe then says something may be ready. A
/// delivery may take a moment to reach the subscriber after its publish returned, as on a socket:
/// the suite waits for it within a deadline, and a probe that says nothing is ready before the
/// delivery has arrived is correct, as long as the arrival wakes the task and turns the probe.
///
/// A broker whose probe is the constant `true` passes: the suite checks that a probe never hides a
/// delivery, not that it is exact. A stream that needs a poll to move a delivery along while its
/// probe says nothing is ready fails: the app would never make that poll. A broker whose subscriber returns no probe passes the same way.
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
/// Panics with a message naming the step when a delivery or the end does not arrive within the
/// deadline through the polls the app makes (a probe that says nothing may be ready while one
/// waits, or a stream that keeps answering `Pending`), when a publish or the shutdown does not
/// wake a poll that returned `Pending`, or when a step hangs.
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
        arrive(stream.as_mut(), &probe, &wakes, None, "first").await,
        "first",
    ))
    .await;

    let parked = park(stream.as_mut(), &wakes);
    publish(&publisher, &subject, b"second").await;
    let asked = awake(&wakes, &probe, parked, "a publish").await;
    settle(taken(
        arrive(stream.as_mut(), &probe, &wakes, Some(asked), "second").await,
        "second",
    ))
    .await;

    publish(&publisher, &subject, b"third").await;
    publish(&publisher, &subject, b"fourth").await;
    settle(taken(
        arrive(stream.as_mut(), &probe, &wakes, None, "third").await,
        "third",
    ))
    .await;
    settle(taken(
        arrive(stream.as_mut(), &probe, &wakes, None, "fourth").await,
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
            arrive(stream.as_mut(), &probe, &wakes, Some(asked), "the end").await,
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

/// The probe's answer to a wake, with the wake count read before the question: a wake that
/// lands while the probe answers is one the app still sees.
#[derive(Clone, Copy)]
struct Asked {
    ready: bool,
    at: usize,
}

/// Reads the wake count, then asks the probe.
fn ask<P: ReadyHint>(wakes: &Wakes, probe: &P) -> Asked {
    let at = wakes.count();
    Asked {
        ready: probe.may_be_ready(),
        at,
    }
}

/// Waits for a wake after the count `parked` and answers what the probe says then: `cause` must
/// wake the poll that returned `Pending`, because the stream registers the waker whenever it
/// returns `Pending`.
async fn awake<P: ReadyHint>(wakes: &Wakes, probe: &P, parked: usize, cause: &str) -> Asked {
    assert!(
        woken(wakes, parked, Instant::now() + STEP_TIMEOUT).await,
        "local_readiness: {cause} did not wake the poll that returned `Pending` within \
         {STEP_TIMEOUT:?}; the stream registers the waker whenever it returns `Pending`"
    );
    ask(wakes, probe)
}

/// Waits until the wake count passes `at`, and answers whether it did before `deadline`.
async fn woken(wakes: &Wakes, at: usize, deadline: Instant) -> bool {
    timeout_at(deadline, async {
        while wakes.count() <= at {
            wakes.woken.notified().await;
        }
    })
    .await
    .is_ok()
}

/// Drives the stream the way the embedded app does until it yields something other than
/// `Pending`, within one deadline for the whole wait.
///
/// `asked` is the probe's answer to a wake the caller already waited for, with the count read
/// before it; `None` means the last poll was not `Pending`, so the app polls at once. After a `Pending` the app waits for a
/// wake and polls only when the probe then says something may be ready; while it says nothing is,
/// the app goes back to waiting without a poll. A delivery that arrives only through the polls the
/// app skips, or a stream that keeps answering `Pending`, runs out the deadline.
async fn arrive<S, P>(
    mut stream: Pin<&mut S>,
    probe: &P,
    wakes: &Arc<Wakes>,
    asked: Option<Asked>,
    which: &str,
) -> Poll<Option<S::Item>>
where
    S: Stream,
    P: ReadyHint,
{
    let deadline = Instant::now() + STEP_TIMEOUT;
    let counting = waker(Arc::clone(wakes));
    let mut asked = asked.unwrap_or_else(|| Asked {
        ready: true,
        at: wakes.count(),
    });
    loop {
        // A stream that wakes its task inside every poll never lets the wait below suspend, so
        // the deadline is read here as well.
        if Instant::now() < deadline && asked.ready {
            asked.at = wakes.count();
            if let ready @ Poll::Ready(_) = poll(stream.as_mut(), &counting) {
                return ready;
            }
        }
        if Instant::now() >= deadline || !woken(wakes, asked.at, deadline).await {
            let why = if asked.ready {
                "the stream kept answering `Pending`"
            } else {
                "after a wake the probe said nothing may be ready, so the app would not poll the \
                 stream and the subscription stalls"
            };
            panic!("local_readiness: {which} did not arrive within {STEP_TIMEOUT:?}; {why}");
        }
        asked = ask(wakes, probe);
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
        publisher.publish(outgoing::<Pub::Payload>(subject, payload), None),
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
    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker, ready};

    use futures::Stream;
    use ruststream_core::{
        ConnectedBroker, MaybeSend, MaybeSendSync, ReadyHint, Subscriber, SubscriptionSource,
    };
    use ruststream_memory::local::{
        ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemorySource,
    };

    use tokio::spawn;
    use tokio::task::yield_now;

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

    /// A source whose stream needs `held` polls before it takes a delivery off the bus, while its
    /// probe says nothing is ready: the app skips exactly those polls, so the subscription stalls.
    struct PollDriven<Src> {
        inner: Src,
        held: usize,
    }

    struct PollDrivenSubscriber<Sub> {
        inner: Sub,
        left: Arc<AtomicUsize>,
        held: usize,
    }

    struct Withheld<Probe> {
        inner: Probe,
        left: Arc<AtomicUsize>,
    }

    impl<Probe: ReadyHint> ReadyHint for Withheld<Probe> {
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

    impl<Sub: Subscriber> Subscriber for PollDrivenSubscriber<Sub> {
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
            Withheld {
                inner: self.inner.ready_hint(),
                left: Arc::clone(&self.left),
            }
        }
    }

    impl<C: ConnectedBroker, Src: SubscriptionSource<C>> SubscriptionSource<C> for PollDriven<Src> {
        type Subscriber = PollDrivenSubscriber<Src::Subscriber>;
        type Copies = Src::Copies;

        fn name(&self) -> &str {
            self.inner.name()
        }

        async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
            Ok(PollDrivenSubscriber {
                inner: self.inner.subscribe(connected).await?,
                left: Arc::new(AtomicUsize::new(self.held)),
                held: self.held,
            })
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[should_panic(
        expected = "local_readiness: first did not arrive within 2s; after a wake the \
                               probe said nothing may be ready"
    )]
    async fn a_stream_that_needs_the_polls_its_probe_withholds_fails() {
        local_readiness(
            LocalMemoryBroker::<16>::new,
            |name| PollDriven {
                inner: LocalMemorySource::new(name),
                held: 2,
            },
            |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
        )
        .await;
    }

    /// A source whose deliveries arrive a few scheduler turns after the stream took them off the
    /// bus, as on a socket: a task of the transport finishes the arrival and wakes the stream, and
    /// the probe says something is ready from then on.
    ///
    /// With `racing` the transport's task first only announces the delivery with a wake, and the
    /// arrival lands right after the probe answered that wake with `false`: a second wake follows
    /// the probe's answer, as when a socket's receiver queues the delivery between the app's
    /// question and its next wait.
    struct InTransit<Src> {
        inner: Src,
        racing: bool,
    }

    struct InTransitSubscriber<Sub> {
        inner: Sub,
        wire: Arc<Wire>,
    }

    /// What the transport's task and the stream share.
    #[derive(Default)]
    struct Wire {
        racing: bool,
        announced: AtomicBool,
        arrived: AtomicBool,
        reader: Mutex<Option<Waker>>,
    }

    impl Wire {
        fn wake_reader(&self) {
            // The waker stays registered until the next poll, as a stream's waker does.
            if let Some(reader) = self.reader.lock().expect("the wire's waker").as_ref() {
                reader.wake_by_ref();
            }
        }
    }

    struct Landed<Probe> {
        inner: Probe,
        wire: Arc<Wire>,
    }

    impl<Probe: ReadyHint> ReadyHint for Landed<Probe> {
        fn may_be_ready(&self) -> bool {
            let answer = self.wire.arrived.load(Ordering::Relaxed) || self.inner.may_be_ready();
            if self.wire.announced.swap(false, Ordering::Relaxed) {
                self.wire.arrived.store(true, Ordering::Relaxed);
                self.wire.wake_reader();
            }
            answer
        }
    }

    struct Carry<Inner: Stream> {
        inner: Pin<Box<Inner>>,
        wire: Arc<Wire>,
        held: Option<Inner::Item>,
    }

    // The held delivery is moved out whole, never pinned in place.
    impl<Inner: Stream> Unpin for Carry<Inner> {}

    impl<Inner: Stream> Stream for Carry<Inner> {
        type Item = Inner::Item;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if this.held.is_some() {
                if this.wire.arrived.swap(false, Ordering::Relaxed) {
                    return Poll::Ready(this.held.take());
                }
                *this.wire.reader.lock().expect("the wire's waker") = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let item = ready!(this.inner.as_mut().poll_next(cx));
            if item.is_none() {
                return Poll::Ready(None);
            }
            this.held = item;
            *this.wire.reader.lock().expect("the wire's waker") = Some(cx.waker().clone());
            let wire = Arc::clone(&this.wire);
            spawn(async move {
                for _ in 0..3 {
                    yield_now().await;
                }
                if wire.racing {
                    wire.announced.store(true, Ordering::Relaxed);
                } else {
                    wire.arrived.store(true, Ordering::Relaxed);
                }
                wire.wake_reader();
            });
            Poll::Pending
        }
    }

    impl<Sub: Subscriber> Subscriber for InTransitSubscriber<Sub> {
        type Message = Sub::Message;
        type Error = Sub::Error;

        fn stream(
            &mut self,
        ) -> impl Stream<Item = Result<Self::Message, Self::Error>> + MaybeSend + '_ {
            Carry {
                inner: Box::pin(self.inner.stream()),
                wire: Arc::clone(&self.wire),
                held: None,
            }
        }

        fn ready_hint(&self) -> impl ReadyHint + MaybeSendSync + 'static + use<Sub> {
            Landed {
                inner: self.inner.ready_hint(),
                wire: Arc::clone(&self.wire),
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
                wire: Arc::new(Wire {
                    racing: self.racing,
                    ..Wire::default()
                }),
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_delivery_still_in_transit_after_the_publish_returns_passes() {
        local_readiness(
            LocalMemoryBroker::<16>::new,
            |name| InTransit {
                inner: LocalMemorySource::new(name),
                racing: false,
            },
            |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_delivery_that_lands_right_after_the_probe_answered_passes() {
        local_readiness(
            LocalMemoryBroker::<16>::new,
            |name| InTransit {
                inner: LocalMemorySource::new(name),
                racing: true,
            },
            |connected: &ConnectedLocalMemoryBroker<16>| connected.publisher(),
        )
        .await;
    }

    /// A source whose stream wakes its task on every poll and never yields: the app polls it
    /// without end, and the delivery never arrives.
    struct Spinning<Src>(Src);

    struct SpinningSubscriber<Sub>(Sub);

    struct Spin<Item>(PhantomData<Item>);

    impl<Item> Stream for Spin<Item> {
        type Item = Item;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    impl<Sub: Subscriber> Subscriber for SpinningSubscriber<Sub> {
        type Message = Sub::Message;
        type Error = Sub::Error;

        fn stream(
            &mut self,
        ) -> impl Stream<Item = Result<Self::Message, Self::Error>> + MaybeSend + '_ {
            Spin(PhantomData)
        }

        fn ready_hint(&self) -> impl ReadyHint + MaybeSendSync + 'static + use<Sub> {
            self.0.ready_hint()
        }
    }

    impl<C: ConnectedBroker, Src: SubscriptionSource<C>> SubscriptionSource<C> for Spinning<Src> {
        type Subscriber = SpinningSubscriber<Src::Subscriber>;
        type Copies = Src::Copies;

        fn name(&self) -> &str {
            self.0.name()
        }

        async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
            Ok(SpinningSubscriber(self.0.subscribe(connected).await?))
        }
    }

    // Real time: the stream never lets the runtime go idle, so a paused clock would not move.
    #[tokio::test(flavor = "current_thread")]
    #[should_panic(
        expected = "local_readiness: first did not arrive within 2s; the stream kept \
                               answering `Pending`"
    )]
    async fn a_stream_that_wakes_on_every_poll_and_never_yields_fails() {
        local_readiness(
            LocalMemoryBroker::<16>::new,
            |name| Spinning(LocalMemorySource::new(name)),
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

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[should_panic(
        expected = "local_readiness: second did not arrive within 2s; after a wake the \
                               probe said nothing may be ready"
    )]
    async fn a_probe_that_never_says_ready_fails() {
        run(0).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[should_panic(
        expected = "local_readiness: the end did not arrive within 2s; after a wake \
                               the probe said nothing may be ready"
    )]
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
