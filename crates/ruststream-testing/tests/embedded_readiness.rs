//! An idle subscription of the embedded app whose subscriber hands out a readiness probe is not
//! polled on the wakes that belong to its neighbours.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::cell::Cell;
use std::convert::Infallible;
use std::error::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

use embassy_futures::block_on;
use futures_core::Stream;
use ruststream::memory::local::prelude::{
    ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError, LocalMemoryMessage,
    LocalMemorySource, LocalMemorySubscriber, Subscribe,
};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, IntoSource, MountSteps, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{
    BrokerMoves, Deserialized, Outgoing, ReadyHint, Serialized, Subscriber, SubscriptionSource,
    subscriber,
};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

thread_local! {
    /// How many times the probing subscriptions' streams were polled on this thread, which is
    /// the one thread a test's app runs on.
    static POLLS: Cell<u32> = const { Cell::new(0) };
}

fn polls() -> u32 {
    POLLS.with(Cell::get)
}

/// A frame, as its bytes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A frame the test publishes, to the destination it names.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// The stream of a probing subscription: the bus's own, with every poll counted.
struct Counted<Inner>(Pin<Box<Inner>>);

impl<Inner: Stream> Stream for Counted<Inner> {
    type Item = Inner::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        POLLS.with(|polls| polls.set(polls.get() + 1));
        self.0.as_mut().poll_next(cx)
    }
}

/// A subscription that hands the app the bus's readiness probe.
struct HintedSubscriber(LocalMemorySubscriber<4>);

impl Subscriber for HintedSubscriber {
    type Message = LocalMemoryMessage<4>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Infallible>> + '_ {
        Counted(Box::pin(self.0.stream()))
    }

    fn ready_hint(&self) -> impl ReadyHint + 'static {
        self.0.ready_hint()
    }
}

/// The same subscription with no probe: the app polls it on every wake.
struct PolledSubscriber(LocalMemorySubscriber<4>);

impl Subscriber for PolledSubscriber {
    type Message = LocalMemoryMessage<4>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Infallible>> + '_ {
        Counted(Box::pin(self.0.stream()))
    }
}

/// The descriptor of a hinted subscription.
#[derive(Clone)]
struct HintedSource(&'static str);

impl HintedSource {
    const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

impl SubscriptionSource<ConnectedLocalMemoryBroker<4>> for HintedSource {
    type Subscriber = HintedSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.0
    }

    async fn subscribe(
        self,
        connected: &ConnectedLocalMemoryBroker<4>,
    ) -> Result<HintedSubscriber, LocalMemoryError> {
        Ok(HintedSubscriber(connected.subscribe(self.0).await?))
    }
}

impl IntoSource for HintedSource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// The descriptor of a subscription with no probe.
#[derive(Clone)]
struct PolledSource(&'static str);

impl PolledSource {
    const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

impl SubscriptionSource<ConnectedLocalMemoryBroker<4>> for PolledSource {
    type Subscriber = PolledSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.0
    }

    async fn subscribe(
        self,
        connected: &ConnectedLocalMemoryBroker<4>,
    ) -> Result<PolledSubscriber, LocalMemoryError> {
        Ok(PolledSubscriber(connected.subscribe(self.0).await?))
    }
}

impl IntoSource for PolledSource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

#[subscriber(LocalMemorySource::new("busy"))]
async fn on_busy(busy: &Frame<'_>) -> HandlerOutcome {
    let _ = busy.0;
    HandlerOutcome::ack()
}

#[subscriber(HintedSource::new("probe"))]
async fn on_hinted(probe: &Frame<'_>) -> HandlerOutcome {
    let _ = probe.0;
    HandlerOutcome::ack()
}

#[subscriber(PolledSource::new("probe"))]
async fn on_polled(probe: &Frame<'_>) -> HandlerOutcome {
    let _ = probe.0;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn hinted_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_busy).include(on_hinted))
}

#[ruststream::app(runtime = embassy)]
fn polled_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_busy).include(on_polled))
}

#[ruststream::app(runtime = embassy)]
fn pooled_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_busy).include(on_hinted).pool::<2>())
}

#[ruststream::app(runtime = embassy)]
fn keyed_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_busy).include(on_hinted).keyed::<2>())
}

/// Publishes `count` frames to the busy subscription.
async fn busy_work(tb: &TestApp, count: usize) -> Result<(), Box<dyn Error>> {
    for _ in 0..count {
        tb.broker::<Bus>()
            .message(&Sent(b"21"))
            .to("busy")
            .publish()
            .await?;
    }
    Ok(())
}

#[test]
fn an_idle_hinted_subscription_is_not_polled_while_its_neighbour_works()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        POLLS.with(|polls| polls.set(0));
        let tb = TestApp::start(hinted_app(Bus::new())).await?;
        busy_work(&tb, 5).await?;

        tb.broker::<Bus>().subscriber("busy").assert_called(5);
        assert_eq!(
            polls(),
            1,
            "the idle subscription was polled on its neighbour's wakes"
        );
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_idle_subscription_without_a_probe_is_polled_on_every_wake() -> Result<(), Box<dyn Error>> {
    block_on(async {
        POLLS.with(|polls| polls.set(0));
        let tb = TestApp::start(polled_app(Bus::new())).await?;
        busy_work(&tb, 5).await?;

        tb.broker::<Bus>().subscriber("busy").assert_called(5);
        assert_eq!(
            polls(),
            6,
            "the first poll, then one on each of the busy subscription's wakes"
        );
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_frame_for_a_hinted_subscription_is_delivered_after_the_idle_stretch()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(hinted_app(Bus::new())).await?;
        busy_work(&tb, 3).await?;
        tb.broker::<Bus>()
            .message(&Sent(b"high"))
            .to("probe")
            .publish()
            .await?;

        tb.broker::<Bus>()
            .subscriber("probe")
            .assert_called_once()
            .with_raw(b"high");
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn a_shutdown_ends_a_parked_hinted_subscription() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(hinted_app(Bus::new())).await?;
        busy_work(&tb, 2).await?;

        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_idle_pooled_hinted_subscription_is_not_polled_while_its_neighbour_works()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        POLLS.with(|polls| polls.set(0));
        let tb = TestApp::start(pooled_app(Bus::new())).await?;
        busy_work(&tb, 5).await?;

        tb.broker::<Bus>().subscriber("busy").assert_called(5);
        assert_eq!(
            polls(),
            1,
            "the idle subscription was polled on its neighbour's wakes"
        );
        tb.broker::<Bus>()
            .message(&Sent(b"high"))
            .to("probe")
            .publish()
            .await?;
        tb.broker::<Bus>().subscriber("probe").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}

#[test]
fn an_idle_keyed_hinted_subscription_is_not_polled_while_its_neighbour_works()
-> Result<(), Box<dyn Error>> {
    block_on(async {
        POLLS.with(|polls| polls.set(0));
        let tb = TestApp::start(keyed_app(Bus::new())).await?;
        busy_work(&tb, 5).await?;

        tb.broker::<Bus>().subscriber("busy").assert_called(5);
        assert_eq!(
            polls(),
            1,
            "the idle subscription was polled on its neighbour's wakes"
        );
        tb.broker::<Bus>()
            .message(&Sent(b"high"))
            .to("probe")
            .publish()
            .await?;
        tb.broker::<Bus>().subscriber("probe").assert_called_once();
        tb.shutdown().await?;
        Ok(())
    })
}
