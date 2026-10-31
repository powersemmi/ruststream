//! The subscription side: the name descriptor, the subscriber it opens and the delivery it
//! yields.

use std::{
    borrow::Cow,
    convert::Infallible,
    fmt,
    future::ready,
    marker::PhantomData,
    sync::{Arc, OnceLock},
    task::{Context, Poll, ready},
    time::Duration,
};

use futures::{Stream, stream};
use ruststream_core::{
    AckError, AddressedCopies, FromName, HeaderMap, IncomingMessage, RawMessage, RedeliveryAddress,
    RedeliveryAddressed, Settle, Subscribe, Subscriber, SubscriptionSource,
};
use ruststream_runtime::IntoSource;
#[cfg(feature = "testing")]
use ruststream_runtime::testing::Coordinator;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::capability::SeekControl;
use crate::log;
use crate::{
    ConnectedMemoryBroker, Discarding, LogMode, MemoryDelivery, MemoryError, MemoryPattern,
    MemoryState, Sender,
};
#[cfg(doc)]
use crate::{MemoryBroker, MemorySeeker, Retaining};

/// A subscription descriptor for [`MemoryBroker`], naming the subject to receive on.
///
/// The broker-owned counterpart to the generic [`Name`](ruststream_core::Name) source: it carries no extra
/// configuration, the in-memory broker having none.
/// Pass it to the descriptor form of the macro, `#[subscriber(MemorySource::new("orders"))]`, the
/// way a NATS service passes `SubscribeOptions`.
///
/// It reads one name, which is also where a retry copy goes. A name with a wildcard token (`*` or
/// `>`) refuses to open with [`MemoryError::WildcardName`]; [`MemoryPattern`] reads a pattern.
#[derive(Debug, Clone)]
pub struct MemorySource {
    name: String,
}

impl MemorySource {
    /// Creates a source bound to `name`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

// The in-memory subscription needs nothing beyond a name, so it offers the name-only
// constructor the `#[subscriber(MemorySource)]` form builds through.
impl FromName for MemorySource {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

impl<Log: LogMode> SubscriptionSource<ConnectedMemoryBroker<Log>> for MemorySource {
    type Subscriber = MemorySubscriber<Log>;
    // The bus moves nothing on its own, and one subject is both ends of it, so a copy goes back
    // to the subject the subscription reads.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> Result<Self::Subscriber, MemoryError> {
        Subscribe::subscribe(connected, &self.name).await
    }
}

impl<Log: LogMode> RedeliveryAddressed<ConnectedMemoryBroker<Log>> for MemorySource {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> {
        // One subject is both ends of the bus, and no lookup is needed to say so.
        ready(Ok(RedeliveryAddress::new(self.name.clone())))
    }
}

impl IntoSource for MemorySource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl IntoSource for MemoryPattern {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// Subscriber returned by [`MemoryBroker::subscribe`]. Yields one [`MemoryMessage`] per
/// delivery; consumers must call `ack` or `nack` on each.
///
/// Also consumable in batches through the [`BatchSubscriber`](ruststream_core::BatchSubscriber) capability,
/// which caps each batch at the size it is asked for and allocates it at that size up front. A
/// subscription of a [`Retaining`] broker is
/// repositionable over its publish log through the [`Seekable`](ruststream_core::Seekable) capability: mint
/// a [`MemorySeeker`] with [`seeker`](ruststream_core::Seekable::seeker) before opening the stream.
pub struct MemorySubscriber<Log = Discarding> {
    pub(super) rx: mpsc::UnboundedReceiver<MemoryDelivery>,
    /// What this subscription shares with every delivery it yields and every seeker minted off
    /// it.
    pub(super) subscription: Arc<SubscriptionShared>,
    mode: PhantomData<Log>,
}

/// One subscription's state that outlives a poll: its name, the bus, the channel a requeue goes
/// back through, and the seek handoff.
///
/// A subscription allocates it once, and every delivery and every [`MemorySeeker`] holds it
/// behind one reference count: a delivery takes one increment where a requeue sender and a
/// seeker would take one each, and a seeker minted off a delivery is that same increment.
pub(super) struct SubscriptionShared {
    /// The name the subscription reads, which a seek resolves against the publish log.
    pub(super) name: String,
    /// Bus state, kept so a seek can read the publish log and check liveness.
    pub(super) state: Arc<MemoryState>,
    /// The subscription's own channel: a requeue and a replay go back through it.
    pub(super) requeue: Sender,
    /// The runtime a delayed redelivery's timer runs on: the one the broker connected on.
    ///
    /// Optional because [`MemoryBroker::subscribe`] opens a subscription without connecting, and
    /// may run outside any runtime; such a subscription's timer runs on the settling caller's.
    runtime: Option<Handle>,
    /// The pending reposition, the stale-delivery watermark, and the waker that rouses a parked
    /// stream.
    pub(super) seek: SeekControl,
}

impl<Log> MemorySubscriber<Log> {
    /// The subscriber reading `rx`, whose registration on the bus is `requeue`'s twin.
    pub(super) fn new(
        name: String,
        rx: mpsc::UnboundedReceiver<MemoryDelivery>,
        requeue: Sender,
        state: &Arc<MemoryState>,
        runtime: Option<Handle>,
    ) -> Self {
        Self {
            rx,
            subscription: Arc::new(SubscriptionShared {
                name,
                state: Arc::clone(state),
                requeue,
                runtime,
                seek: SeekControl::default(),
            }),
            mode: PhantomData,
        }
    }

    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    ///
    /// Read off the bus at stream time rather than captured at subscribe time: a subscriber built
    /// before the app was handed to the harness would otherwise carry `None` forever and never
    /// decrement what the bus counted in, hanging the quiescence wait.
    #[cfg(feature = "testing")]
    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.subscription.state.coordinator()
    }
}

#[cfg(feature = "testing")]
impl<Log> Drop for MemorySubscriber<Log> {
    /// What is still queued leaves with the subscription, so the harness stops counting it in
    /// flight. A delivery handed back unsettled while the service tears down (a fail-fast panic)
    /// lands here, and the reaction the test waits on is over once the subscription is gone.
    fn drop(&mut self) {
        let Some(coordinator) = self.coordinator() else {
            return;
        };
        // Closed first, so a requeue racing the drop either lands before the drain or fails.
        self.rx.close();
        while self.rx.try_recv().is_ok() {
            coordinator.consumed();
        }
    }
}

impl<Log> fmt::Debug for MemorySubscriber<Log> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySubscriber")
            .field("name", &self.subscription.name)
            .finish_non_exhaustive()
    }
}

impl<Log: LogMode> Subscriber for MemorySubscriber<Log> {
    type Message = MemoryMessage<Log>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        #[cfg(feature = "testing")]
        let coordinator = self.coordinator();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream` can
        // be called again after the returned stream is dropped (helpers re-enter it per call).
        stream::poll_fn(move |cx| {
            self.begin_poll(cx);
            let delivery = ready!(self.poll_fresh(
                cx,
                #[cfg(feature = "testing")]
                coordinator.as_ref(),
            ));
            Poll::Ready(delivery.map(|delivery| {
                Ok(self.message(
                    delivery,
                    #[cfg(feature = "testing")]
                    coordinator.as_ref(),
                ))
            }))
        })
    }
}

impl<Log: LogMode> MemorySubscriber<Log> {
    /// The start of every poll of the stream and the batch paths: register for a seeker's wake-up,
    /// then apply a pending seek.
    #[inline]
    pub(crate) fn begin_poll(&mut self, cx: &Context<'_>) {
        // Register before reading the pending seek: a seek landing between the read and the
        // park then still finds a waker to rouse.
        // Only a seeker wakes this waker, and a discarding subscription mints none.
        if log::retains::<Log>() {
            self.subscription.seek.waker.register(cx.waker());
        }
        self.apply_pending_seek();
    }

    /// The next queued delivery that is not a stale pre-seek copy, or the end of the queue.
    #[inline]
    pub(crate) fn poll_fresh(
        &mut self,
        cx: &mut Context<'_>,
        #[cfg(feature = "testing")] coordinator: Option<&Coordinator>,
    ) -> Poll<Option<MemoryDelivery>> {
        loop {
            match ready!(self.rx.poll_recv(cx)) {
                // A stale pre-seek copy (a requeue that raced the seek): drop it, the replay
                // already covers everything from the watermark on.
                Some(delivery)
                    if log::retains::<Log>()
                        && delivery.seq < self.subscription.seek.watermark() =>
                {
                    #[cfg(feature = "testing")]
                    if let Some(coordinator) = coordinator {
                        coordinator.consumed();
                    }
                }
                fresh => return Poll::Ready(fresh),
            }
        }
    }

    /// The message a delivery off this subscription is handed out as.
    #[inline]
    pub(crate) fn message(
        &self,
        delivery: MemoryDelivery,
        #[cfg(feature = "testing")] coordinator: Option<&Coordinator>,
    ) -> MemoryMessage<Log> {
        MemoryMessage {
            delivery: Some(delivery),
            subscription: Some(Arc::clone(&self.subscription)),
            #[cfg(feature = "testing")]
            coordinator: coordinator.cloned(),
            mode: PhantomData,
        }
    }
}

/// A delivery yielded by [`MemorySubscriber::stream`].
///
/// Consumers call [`Settle::ack`] to confirm processing or
/// [`Settle::nack`] to negatively acknowledge. `nack` with `requeue = true` pushes the
/// delivery back to the same subscriber's queue; with `requeue = false` it is dropped.
/// [`nack_after`](Settle::nack_after) pushes it back once the delay has elapsed, and at
/// once for a zero delay. A delivery dropped without a settlement is pushed back too: only a
/// settlement consumes it.
pub struct MemoryMessage<Log = Discarding> {
    pub(super) delivery: Option<MemoryDelivery>,
    /// The subscription this delivery came from, which a requeue goes back to and the seek
    /// context builds off. `None` for a request-reply inbox message, which has no subscription
    /// behind it: nothing reads the inbox once the request returns, so a requeue drops it.
    pub(super) subscription: Option<Arc<SubscriptionShared>>,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in flight and
    /// is decremented once when the message is consumed or dropped (see the `Drop` impl). `None`
    /// outside a harness run and for request-reply inbox messages (which are not dispatch-driven).
    #[cfg(feature = "testing")]
    pub(super) coordinator: Option<Coordinator>,
    pub(super) mode: PhantomData<Log>,
}

impl<Log> Drop for MemoryMessage<Log> {
    /// Hands an unsettled delivery back to its subscription, the way a broker with
    /// acknowledgement takes back what its consumer let go of: only a settlement consumes a
    /// delivery. The redelivery counts as one more delivery, and it is a send on the
    /// subscription's queue, so it runs on no runtime at all.
    ///
    /// Under the harness it also counts this delivery consumed exactly once: on ack, nack,
    /// `into_raw`, or an unsettled drop. A requeue (`nack(true)`, `nack_after`, this drop)
    /// re-enqueues a fresh delivery first, so the in-flight count stays balanced across
    /// redelivery.
    fn drop(&mut self) {
        if let Some(delivery) = self.delivery.take() {
            self.requeue_now(delivery.redelivered());
        }
        #[cfg(feature = "testing")]
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl<Log> fmt::Debug for MemoryMessage<Log> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryMessage")
            .field("name", &self.delivery.as_ref().map(|d| &*d.shared.name))
            .finish_non_exhaustive()
    }
}

impl<Log> MemoryMessage<Log> {
    /// Returns the name the message was published to.
    #[must_use]
    pub fn name(&self) -> &str {
        self.delivery
            .as_ref()
            .map(|d| &*d.shared.name)
            .unwrap_or_default()
    }

    /// Converts the delivery into a broker-agnostic [`RawMessage`]. Consumes the handle without
    /// acknowledging; useful only for assertions that do not care about ack state.
    ///
    /// # Panics
    ///
    /// Panics if the delivery has already been moved out (only possible if internal invariants
    /// were violated; not reachable through the public API).
    #[must_use]
    pub fn into_raw(mut self) -> RawMessage {
        let delivery = self.delivery.take().expect("delivery already consumed");
        // Cold path (test assertions): the name, the payload and the headers are copied out of
        // the shared block here, not on the fanout.
        RawMessage::new(&*delivery.shared.name, delivery.shared.payload.clone())
            .with_headers(delivery.shared.headers.clone())
    }
}

impl<Log> MemoryMessage<Log> {
    /// Puts `delivery` back on its subscription's queue now.
    fn requeue_now(&self, delivery: MemoryDelivery) {
        // An inbox reply has no subscription to come back to; see `subscription`.
        let Some(subscription) = &self.subscription else {
            return;
        };
        let sent = subscription.requeue.send(delivery);
        // The requeue bypasses `fanout`, so count the re-enqueue here to balance this message's
        // `Drop` decrement. The redelivered copy is consumed (and decremented) in turn.
        #[cfg(feature = "testing")]
        if sent.is_ok()
            && let Some(coordinator) = &self.coordinator
        {
            coordinator.enqueued();
        }
        // The subscriber may be gone by now; a dropped receiver is not an error.
        #[cfg(not(feature = "testing"))]
        let _ = sent;
    }
}

impl<Log: LogMode> IncomingMessage for MemoryMessage<Log> {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.shared.payload.as_ref())
            .unwrap_or_default()
    }

    fn partition_key(&self) -> Option<&[u8]> {
        ruststream_core::Partitioned::partition_key(self)
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.shared.headers)
    }

    /// This broker counts what it delivers, so a registration's `max_attempts(..)` is spent on
    /// its own requeues and delayed redeliveries rather than only on the copies the runtime
    /// publishes. The first delivery of a message answers one.
    fn redelivery_count(&self) -> Option<u64> {
        self.delivery.as_ref().map(|d| d.deliveries.get())
    }
}

impl<Log: LogMode> Settle for MemoryMessage<Log> {
    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self.delivery.take().expect("delivery already consumed");
        if requeue {
            self.requeue_now(delivery.redelivered());
        }
        ready(Ok(()))
    }

    fn supports_nack_after(&self) -> bool {
        true
    }

    /// Native delayed redelivery: the message returns to the same subscriber's queue once
    /// `delay` has elapsed and counts as one more delivery. A zero delay returns it at once, as
    /// `nack(true)` does, so under `TestApp` it arrives without an `advance`.
    fn nack_after(mut self, delay: Duration) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("delivery already consumed")
            .redelivered();
        // A zero delay is a requeue that counts as a redelivery: a timer task for it would cost a
        // task allocation and two scheduler turns to reach the same place in the queue.
        if delay.is_zero() {
            self.requeue_now(delivery);
            return ready(Ok(()));
        }
        // Taken rather than cloned: the message is consumed here, so its count moves to the
        // redelivery. An inbox reply has no subscription to come back to; see `subscription`.
        let Some(subscription) = self.subscription.take() else {
            return ready(Ok(()));
        };
        // The timer holds the requeue sender, not the subscription: the subscription holds the
        // bus and its publish log, which a redelivery an hour away must not keep alive once the
        // broker and the subscriber are gone.
        let back = subscription.requeue.clone();
        // Under the harness, register the redelivery with the coordinator so the in-flight count is
        // re-balanced when it fires and a test can drive it with `TestApp::advance`. The immediate
        // settlement (`NackAfter`) was already recorded; the redelivery is off the synchronous
        // reaction `drive` waits on.
        #[cfg(feature = "testing")]
        if let Some(coordinator) = self.coordinator.clone() {
            drop(subscription);
            let counter = coordinator.clone();
            coordinator.schedule_redelivery(delay, move || {
                if back.send(delivery).is_ok() {
                    counter.enqueued();
                }
            });
            return ready(Ok(()));
        }
        let redeliver = async move {
            sleep(delay).await;
            // The subscriber may be gone by then; a dropped receiver is not an error.
            let _ = back.send(delivery);
        };
        // On the runtime the broker connected on, not the settling caller's: a handler on a
        // dedicated thread settles from a runtime that may stop before the delay runs out.
        match &subscription.runtime {
            Some(runtime) => drop(runtime.spawn(redeliver)),
            None => drop(tokio::spawn(redeliver)),
        }
        ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use futures::{FutureExt, StreamExt};
    use ruststream_core::{OutgoingMessage, Publisher};
    use tokio::runtime::Builder;
    use tokio::{task, time};

    use super::*;
    use crate::{MemoryBroker, Retention};

    /// A delivery reports the name it was published to, and `into_raw` consumes it without settling
    /// into a broker-agnostic message that keeps the name and the payload.
    #[tokio::test]
    async fn into_raw_keeps_the_name_and_the_payload() {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();
        let mut sub = broker.subscribe("dbg");
        publisher
            .publish(OutgoingMessage::new("dbg", b"payload"), None)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.name(), "dbg");

        let raw = msg.into_raw();
        assert_eq!(raw.name(), "dbg");
        assert_eq!(raw.payload(), b"payload");
    }

    #[tokio::test(start_paused = true)]
    async fn nack_after_redelivers_after_the_delay() {
        const DELAY: Duration = Duration::from_secs(5);
        let broker = MemoryBroker::new();
        let mut sub = MemoryBroker::subscribe(&broker, "delayed");
        let publisher = broker.publisher();

        publisher
            .publish(OutgoingMessage::new("delayed", b"later"), None)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        msg.nack_after(DELAY).await.unwrap();

        // The timer task needs a turn to arm its timer, and one after each advance to act on it. One
        // tick short of the delay nothing is redelivered; the tick that reaches it redelivers.
        task::yield_now().await;
        time::advance(DELAY.saturating_sub(Duration::from_millis(1))).await;
        task::yield_now().await;
        assert!(futures::poll!(stream.next()).is_pending());
        time::advance(Duration::from_millis(1)).await;
        task::yield_now().await;

        let Some(Some(Ok(redelivered))) = stream.next().now_or_never() else {
            panic!("the redelivery must land on the tick that reaches the delay");
        };
        assert_eq!(redelivered.payload(), b"later");
        redelivered.ack().await.unwrap();
    }

    /// A subscription opened without connecting keeps no runtime: one opened on a runtime that has
    /// since stopped is redelivered on the runtime that settles it.
    #[test]
    fn an_unconnected_subscription_redelivers_on_the_settling_runtime() {
        let broker = MemoryBroker::new();
        let opening = Builder::new_current_thread().build().unwrap();
        let mut sub = opening.block_on(async { broker.subscribe("delayed") });
        drop(opening);

        let settling = Builder::new_current_thread()
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
            let redelivered = time::timeout(Duration::from_secs(60), stream.next()).await;
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
        let broker = MemoryBroker::retaining(Retention::Messages(ruststream_core::nonzero!(8)));
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
        time::advance(Duration::from_secs(3600)).await;
        task::yield_now().await;
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

    /// A `MemorySource` names the subscription of a manual definition, as a subject string does.
    #[test]
    fn a_memory_source_names_a_manual_subscription() {
        use std::future::ready;

        use schemars::JsonSchema;
        use serde::Deserialize;

        use ruststream_runtime::{Context, Handle, Router, Verdict, subscriber};

        #[derive(Deserialize, JsonSchema)]
        struct Order {
            id: u64,
        }

        struct Audit;

        impl Handle<Order> for Audit {
            fn handle(
                &self,
                order: &Order,
                _outs: &(),
                _ctx: &mut Context<'_>,
            ) -> impl Future<Output = Verdict<Order, ()>> {
                let _ = order.id;
                ready(Ok(()))
            }
        }

        let _ = Router::<MemoryBroker>::new()
            .include(subscriber(MemorySource::new("orders"), Audit).build());
    }
}
