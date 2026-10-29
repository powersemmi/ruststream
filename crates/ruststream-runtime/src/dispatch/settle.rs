//! Settling a delivery by its handler's outcome: the ack, the nack, and the copies the runtime
//! publishes for a retry or a dead letter.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time;
use tracing::warn;

#[cfg(feature = "testing")]
use super::harness_scope;
use super::{Delivery, RETRY_COUNT_HEADER, Slot};
use crate::handler::HandlerResult;
use crate::publish::PublishContext;
use crate::redelivery::DeferredRetry;
#[cfg(feature = "testing")]
use crate::testing::coordinator::{Coordinator, in_harness_scope};
use crate::{AckError, HeaderMap, IncomingMessage, OutgoingMessage, RetryDeclaration, Str};

/// Parses the current [`RETRY_COUNT_HEADER`] value, defaulting to zero when absent or malformed.
fn current_retry_count(headers: &HeaderMap) -> u64 {
    headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Settles one delivery by `outcome`, logging an ack / nack failure without propagating it.
///
/// The single place a settlement reaches the broker, single-message and batch paths alike: a
/// second one would be free to answer a [`NackAfter`](HandlerResult::NackAfter) differently, and
/// the retry path below is exactly the part that is easy to leave out.
pub(crate) async fn settle_outcome<M, C>(
    slot: &mut Slot<M>,
    outcome: HandlerResult,
    name: &str,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    let ack_result = match outcome {
        HandlerResult::Ack => slot.take().ack().await,
        HandlerResult::Nack { requeue: false } => slot.take().nack(false).await,
        HandlerResult::Nack { requeue: true } => settle_retry(slot, name, delivery, build_cx).await,
        HandlerResult::NackAfter { delay } => {
            settle_nack_after(slot, name, delay, delivery, build_cx).await
        }
    };
    if let Err(err) = ack_result {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            error = %err,
            "ack / nack failed",
        );
    }
}

/// Where the next copy of one delivery goes, read off the registration's declaration and the
/// count the delivery carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Redelivery<'a> {
    /// Back to the subscription itself: the attempts are not spent and no destination has taken
    /// the copies over.
    Subscription,
    /// To the declared destination.
    DeadLetter {
        /// Where the copy is published.
        destination: &'a str,
        /// Whether it goes there because the attempts are spent, rather than because the
        /// registration named a destination and no cap.
        at_cap: bool,
    },
    /// Rejected without a copy: the attempts are spent and the registration named nowhere to send
    /// it. A rejection keeps the broker's own dead-letter policy in play where one is configured.
    Reject,
}

/// How many times this message has been delivered, counting this delivery.
///
/// The broker's own count where the transport keeps one, the framework's header otherwise, never
/// both. A transport that counts its deliveries answers for all of them, the ones a delay or a
/// requeue brought round included. The header starts absent and is incremented on every copy
/// published for the delivery, so the first delivery counts as one either way.
fn attempt_of<M: IncomingMessage>(msg: &M) -> u64 {
    msg.redelivery_count()
        .unwrap_or_else(|| current_retry_count(msg.headers()) + 1)
}

/// Reads the registration's declaration against this delivery.
fn redelivery_of<'d, M: IncomingMessage>(
    msg: &M,
    declaration: &'d RetryDeclaration,
) -> Redelivery<'d> {
    match (declaration.max_attempts(), declaration.dead_letter()) {
        (Some(cap), destination) if attempt_of(msg) >= u64::from(cap.get()) => {
            destination.map_or(Redelivery::Reject, |destination| Redelivery::DeadLetter {
                destination,
                at_cap: true,
            })
        }
        (None, Some(destination)) => Redelivery::DeadLetter {
            destination,
            at_cap: false,
        },
        _ => Redelivery::Subscription,
    }
}

/// Settles an immediate retry ([`Nack { requeue: true }`](HandlerResult::Nack)).
///
/// A registration that declared neither a cap nor a destination gets the broker's own requeue,
/// which is what an immediate retry has always been. Under a declaration the outcome obeys it:
/// where the transport counts its own redeliveries the requeue stays the broker's and the count
/// is read from the delivery, and where it counts none the runtime publishes the copy itself so
/// that the framework's header carries the count forward and the cap applies to an immediate
/// retry as it does to a delayed one.
///
/// A registration whose copies the broker moves itself is never capped here, exactly as on the
/// native delayed path: the broker's requeue is the immediate retry, and the declaration reached
/// the subscription descriptor at startup for the broker to apply. Reading the cap here would
/// settle a spent delivery with a plain rejection, and on a queue that deletes a rejected
/// delivery (SQS) that loses the message instead of leaving it to the redrive policy.
///
/// # Errors
///
/// Returns the [`AckError`] from settling the original delivery.
async fn settle_retry<M, C>(
    slot: &mut Slot<M>,
    name: &str,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    if delivery.declaration.declares_nothing() {
        return slot.take().nack(true).await;
    }
    // The broker moves this subscription's deliveries itself: its requeue is what an immediate
    // retry is, and the declaration is the broker's to apply, so no cap is read here, as on the
    // native delayed path.
    let Some(retry) = delivery.retry.as_ref() else {
        return slot.take().nack(true).await;
    };
    match redelivery_of(slot.get(), &delivery.declaration) {
        // The broker counts its own redeliveries, so its requeue carries the count forward and
        // there is nothing for a copy to add.
        Redelivery::Subscription if slot.get().redelivery_count().is_some() => {
            slot.take().nack(true).await
        }
        Redelivery::Subscription => {
            let destination = retry.destination.clone();
            publish_copy(slot, name, destination, None, retry, delivery, build_cx).await
        }
        Redelivery::DeadLetter {
            destination,
            at_cap,
        } => dead_letter(slot, name, destination, at_cap, retry, delivery, build_cx).await,
        Redelivery::Reject => reject_spent(slot, name).await,
    }
}

/// Settles a [`NackAfter`](HandlerResult::NackAfter) outcome, choosing native delayed redelivery
/// or the copy the runtime publishes.
///
/// When the broker reports native support (`supports_nack_after`), this defers to
/// [`IncomingMessage::nack_after`] and nothing is published - unless the delivery is already at
/// the registration's cap. The cap is read first, from the one count the transport has (see
/// [`attempt_of`]): the broker's own where it keeps one, the framework's header otherwise, never
/// both. A delivery at the cap goes to the declared dead-letter destination or is rejected,
/// exactly as on the copy path.
/// Where the broker moves a spent delivery itself the cap is not read on this path: the
/// declaration is the subscription descriptor's to map onto the broker's own mechanism.
///
/// Without native support this captures the message, drops the original, and schedules a copy of it
/// after the delay, with the [`RETRY_COUNT_HEADER`] incremented. A zero delay publishes the copy at
/// once, on the dispatch path, as an immediate retry does. The copy goes to the address the
/// subscription's descriptor reported at startup, not to the subscription's name: the two differ
/// wherever a subscription is a resource of its own. It leaves through the registration's retry
/// slot, so the mount site's transforms and the publisher's own headers reach it as they reach any
/// slot publish; it carries bytes already, so no codec encodes it and the call adjusts no
/// per-message settings.
///
/// Once the registration's attempts are spent the copy goes to the declared dead-letter
/// destination instead, immediately, and with no destination declared the delivery is rejected.
///
/// A transport with no settlement at all ([`AckError::Unsupported`] from `nack`, as on MQTT at
/// `QoS` 0, `ZeroMQ`, or Redis pub/sub) still gets the copy: there is no original to drop and no
/// redelivery of the broker's own to fall back on, so the copy is the only form the retry can
/// take.
///
/// Where the broker moves the subscription's deliveries itself and cannot hold one back, the
/// delivery is requeued at once, and a warning says the delay is dropped. A delivery the
/// declaration sends elsewhere (its attempts are spent, or a destination takes every copy) gets
/// the same requeue without the warning: the broker moves it on that requeue, so no delay was due.
///
/// # Errors
///
/// Returns the [`AckError`] from settling the original when the transport does settle but this
/// settle failed. The delivery then stays with the broker, which redelivers it on its own timers,
/// and a copy on top of that would duplicate the message; the caller logs the error.
///
/// # Cancel safety
///
/// A deferred copy runs on a task that sleeps for `delay`, spawned where the delivery runs and
/// tracked, so a graceful shutdown waits for the timer and the copy leaves before the broker
/// closes. It is at-most-once across a crash: if the process dies (or the runtime is dropped
/// without a graceful shutdown) before the timer fires, the copy is lost, since the original has
/// already been dropped. Brokers that need at-least-once delayed redelivery across a crash must
/// provide native support.
async fn settle_nack_after<M, C>(
    slot: &mut Slot<M>,
    name: &str,
    delay: Duration,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    if slot.get().supports_nack_after() {
        // The cap is read before the delay reaches the broker: a native redelivery would otherwise
        // circle past a cap nothing in this process ever gets to apply. It is read wherever this
        // process publishes the subscription's copies, because a crate that honours the delay by
        // republishing the delivery increments the header the cap counts with where the transport
        // counts nothing; a broker that moves a spent delivery itself applies the declaration
        // itself.
        if let Some(retry) = delivery.retry.as_ref()
            && delivery.declaration.max_attempts().is_some()
        {
            match redelivery_of(slot.get(), &delivery.declaration) {
                // Under a cap a destination takes only the spent deliveries.
                Redelivery::DeadLetter { destination, .. } => {
                    return dead_letter(slot, name, destination, true, retry, delivery, build_cx)
                        .await;
                }
                Redelivery::Reject => return reject_spent(slot, name).await,
                Redelivery::Subscription => {}
            }
        }
        let accepted = slot.take().nack_after(delay).await;
        // The broker holds this redelivery on its own timer, out of the harness's sight, so the
        // harness is told when it falls due.
        #[cfg(feature = "testing")]
        if accepted.is_ok()
            && let Some(coordinator) = delivery.hooks.coordinator()
        {
            coordinator.expect_redelivery(delivery.subscription, delay);
        }
        return accepted;
    }

    let Some(retry) = delivery.retry.as_ref() else {
        return requeue_now(slot, name, &delivery.declaration).await;
    };

    match redelivery_of(slot.get(), &delivery.declaration) {
        Redelivery::Subscription => {
            let destination = retry.destination.clone();
            publish_copy(
                slot,
                name,
                destination,
                Some(delay),
                retry,
                delivery,
                build_cx,
            )
            .await
        }
        // A delivery that has run out of attempts is not retried, so the delay it asked for does
        // not apply to the copy that carries it away.
        Redelivery::DeadLetter {
            destination,
            at_cap,
        } => dead_letter(slot, name, destination, at_cap, retry, delivery, build_cx).await,
        Redelivery::Reject => reject_spent(slot, name).await,
    }
}

/// Requeues a delayed retry at once, on a broker that moves the subscription's deliveries itself
/// and cannot hold one back.
///
/// # Errors
///
/// Returns the [`AckError`] from the requeue.
async fn requeue_now<M: IncomingMessage>(
    slot: &mut Slot<M>,
    name: &str,
    declaration: &RetryDeclaration,
) -> Result<(), AckError> {
    // A delivery the declaration sends elsewhere is the broker's to move, and this requeue is
    // what moves it, so it was never due a delay. Only one that comes back loses its delay.
    if matches!(
        redelivery_of(slot.get(), declaration),
        Redelivery::Subscription
    ) {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            "retry_after on a broker with neither native delayed redelivery nor a copy path \
             of its own; requeuing immediately (the delay is dropped)",
        );
    }
    slot.take().nack(true).await
}

/// Sends the delivery to the declared dead-letter `destination` at once, saying so where it goes
/// there because its attempts are spent (`at_cap`).
///
/// # Errors
///
/// Returns the [`AckError`] from dropping the original, as [`publish_copy`] does.
async fn dead_letter<M, C>(
    slot: &mut Slot<M>,
    name: &str,
    destination: &str,
    at_cap: bool,
    retry: &DeferredRetry<C>,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    if at_cap {
        warn_at_cap(name, attempt_of(slot.get()), Some(destination));
    }
    let destination = Arc::from(destination);
    publish_copy(
        slot,
        name,
        Some(destination),
        None,
        retry,
        delivery,
        build_cx,
    )
    .await
}

/// Rejects a delivery whose attempts are spent and which has nowhere declared to go.
///
/// # Errors
///
/// Returns the [`AckError`] from the rejection.
async fn reject_spent<M: IncomingMessage>(slot: &mut Slot<M>, name: &str) -> Result<(), AckError> {
    warn_at_cap(name, attempt_of(slot.get()), None);
    slot.take().nack(false).await
}

/// Says once per exhausted delivery where it went, because that is the point at which a message
/// stops coming back and an operator has to know.
fn warn_at_cap(name: &str, attempt: u64, destination: Option<&str>) {
    if let Some(destination) = destination {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            attempt,
            dead_letter = %destination,
            "retry attempts exhausted; publishing the delivery to the dead-letter destination",
        );
    } else {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            attempt,
            "retry attempts exhausted and no dead-letter destination is declared; rejecting the \
             delivery",
        );
    }
}

/// Drops the original delivery and sends a copy of it to `destination`, now or after `delay`; a
/// zero delay is now.
///
/// `destination` is `None` only where the registration's transforms name one per delivery; the
/// copy then starts at the subscription's own name, which is what the transforms read as the
/// delivery's channel, and one of them renames it.
///
/// # Errors
///
/// Returns the [`AckError`] from dropping the original where that failed and the transport does
/// settle: the delivery then stays with the broker, and a copy on top of it would duplicate the
/// message.
async fn publish_copy<M, C>(
    slot: &mut Slot<M>,
    name: &str,
    destination: Option<Arc<str>>,
    delay: Option<Duration>,
    retry: &DeferredRetry<C>,
    delivery: &Delivery<C>,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: IncomingMessage,
    C: Send + Sync + 'static,
{
    let publisher = Arc::clone(&retry.publisher);
    let subscription: Arc<str> = Arc::from(name);
    let target = destination.unwrap_or_else(|| Arc::clone(&subscription));

    // Settling consumes the message, so capture everything the copy needs first: its bytes, the
    // headers the handler saw, and the broker's own per-delivery context, which is what a
    // transform on this position reads. The bytes are lent on from here rather than handed over:
    // this path runs only after a delivery has failed, so a broker that keeps owned bytes copies
    // once there, where handing them over would cost the runtime a conversion on every retry.
    let msg = slot.get();
    let payload = Bytes::copy_from_slice(msg.payload());
    let delivered = msg.headers().clone();
    let mut headers = delivered.clone();
    let next_count = current_retry_count(&headers) + 1;
    headers.insert(Str::from_static(RETRY_COUNT_HEADER), next_count.to_string());
    let context = build_cx(msg);

    // Drop the original so the broker does not also redeliver it; the copy carries the retry
    // forward. A transport with no settlement at all has nothing to drop and no redelivery of its
    // own, so there the copy is the only way the message survives: aborting on that error would
    // lose it. Any other settle failure leaves the delivery with the broker, which will redeliver
    // it on its own timers, so a copy on top would duplicate it.
    //
    // Under the harness the original's settlement closes its count before the copy's publish
    // opens the copy's; the hold keeps the reaction open across that gap until this function
    // returns, so `drive` cannot settle between the two.
    #[cfg(feature = "testing")]
    let _hold = delivery.hooks.coordinator().map(Coordinator::hold);
    match slot.take().nack(false).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(err) => return Err(err),
    }

    let republish = async move {
        let cx = PublishContext::new(&subscription, &delivered, &context);
        let copy = OutgoingMessage::new(target.as_ref(), payload.as_ref()).with_headers(headers);
        if let Err(err) = publisher.publish_copy(copy, &cx).await {
            warn!(
                target: "ruststream::dispatch",
                subscription = %subscription,
                destination = %target,
                error = %err,
                "publishing the retry copy failed; message lost",
            );
        }
    };

    // The copy leaves after the handler returned, so the harness watching the delivery is
    // handed on to it: the copy is one of the delivery's publishes.
    #[cfg(feature = "testing")]
    let republish = in_harness_scope(harness_scope(delivery), republish);

    // A zero delay is an immediate copy: a timer task for it would cost a task allocation and
    // two scheduler turns to publish the same copy a little later.
    let Some(delay) = delay.filter(|delay| !delay.is_zero()) else {
        // An immediate copy is awaited on the dispatch path: there is no timer to wait for, and a
        // detached task would let the loop pull the next delivery before this one is back.
        republish.await;
        return Ok(());
    };

    // Under the harness the copy is scheduled through the coordinator, the way a broker schedules
    // its native delayed redelivery: `TestApp::advance` then fires it and counts it before it
    // drives the reaction, so a test sees the copy rather than a race.
    #[cfg(feature = "testing")]
    if let Some(coordinator) = delivery.hooks.coordinator() {
        coordinator.schedule_redelivery_future(delay, republish);
        return Ok(());
    }
    // The timer is the delivery's own work, so it runs where the delivery ran; being tracked, a
    // graceful shutdown waits for it and the copy leaves before the broker closes.
    delivery.spawn_after(async move {
        time::sleep(delay).await;
        republish.await;
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::ready;
    use std::io;
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicU8, Ordering};

    use futures::{StreamExt, poll};
    use tokio::sync::mpsc;
    use tokio::task;
    use tokio_util::task::TaskTracker;

    use super::super::fixtures::{PlainMessage, RejectingPublisher, Settlement};
    use super::*;
    use crate::redelivery::bare_retry_publisher;
    use crate::{Lend, Publisher, Subscriber};
    use ruststream::memory::MemoryBroker;

    /// The per-delivery context of a registration that reads none: what a test delivery's transforms
    /// would see.
    fn unit_cx<M>(_msg: &M) {}

    /// A delivery whose settlement always fails, so the dispatcher's ack-failure path runs.
    struct UnsettleableMessage;

    impl IncomingMessage for UnsettleableMessage {
        fn payload(&self) -> &[u8] {
            b"body"
        }

        fn headers(&self) -> &HeaderMap {
            static EMPTY: LazyLock<HeaderMap> = LazyLock::new(HeaderMap::new);
            &EMPTY
        }

        fn ack(self) -> impl Future<Output = Result<(), AckError>> {
            ready(Err(AckError::Timeout))
        }

        fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
            ready(Err(AckError::Unsupported))
        }
    }

    /// A reading publisher that reports the address the payload reached it at, for the assertion the
    /// benchmarks cannot make: the in-memory broker redelivers natively, so no measured scenario
    /// reaches the deferred copy at all.
    struct FormReportingPublisher(mpsc::UnboundedSender<usize>);

    impl Publisher for FormReportingPublisher {
        type Payload = Lend;
        type Error = io::Error;
        type Options = ();

        fn publish(
            &self,
            msg: OutgoingMessage<'_, &[u8]>,
            _options: Option<&Self::Options>,
        ) -> impl Future<Output = Result<(), Self::Error>> {
            let _ = self.0.send(msg.into_payload().as_ptr() as usize);
            ready(Ok(()))
        }
    }

    fn plain(name_headers: &[(&str, &str)], settled: &Arc<AtomicU8>) -> PlainMessage {
        plain_on(name_headers, settled, Settlement::Accepted)
    }

    fn plain_on(
        name_headers: &[(&str, &str)],
        settled: &Arc<AtomicU8>,
        settlement: Settlement,
    ) -> PlainMessage {
        let mut headers = HeaderMap::new();
        for (k, v) in name_headers {
            headers.insert((*k).to_owned(), Bytes::copy_from_slice(v.as_bytes()));
        }
        PlainMessage {
            payload: Bytes::from_static(b"body"),
            headers,
            settled: Arc::clone(settled),
            settlement,
        }
    }

    #[tokio::test]
    async fn a_failed_acknowledgement_is_logged_rather_than_propagated() {
        // Settlement is best-effort: a broker that rejects the ack must not take the loop down.
        settle_outcome(
            &mut Slot::new(UnsettleableMessage),
            HandlerResult::Ack,
            "orders",
            &Delivery::empty(),
            unit_cx,
        )
        .await;
        settle_outcome(
            &mut Slot::new(UnsettleableMessage),
            HandlerResult::drop(),
            "orders",
            &Delivery::empty(),
            unit_cx,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_deferred_republish_is_logged_rather_than_propagated() {
        let delivery = Delivery::deferring_to(
            bare_retry_publisher(RejectingPublisher),
            "orders",
            TaskTracker::new(),
        );
        let settled = Arc::new(AtomicU8::new(0));
        settle_nack_after(
            &mut Slot::new(plain(&[], &settled)),
            "orders",
            Duration::from_secs(1),
            &delivery,
            unit_cx,
        )
        .await
        .unwrap();

        // The original is already dropped, so the failed republish loses the message; the point
        // is that the deferred task reports it instead of panicking the runtime.
        assert_eq!(settled.load(Ordering::SeqCst), 1);
        time::advance(Duration::from_secs(1)).await;
        task::yield_now().await;
    }

    /// The bytes of a deferred copy are the dispatch's own - it read them off the delivery before
    /// settling it - and a publisher that reads the payload is lent them: a broker that keeps owned
    /// bytes copies once here, on a path that runs only after a delivery has failed, instead of the
    /// runtime converting a buffer on every retry.
    #[tokio::test(start_paused = true)]
    async fn the_deferred_copy_lends_the_buffer_the_dispatch_holds() {
        let (reported, mut arrived) = mpsc::unbounded_channel();
        let delivery = Delivery::deferring_to(
            bare_retry_publisher(FormReportingPublisher(reported)),
            "orders",
            TaskTracker::new(),
        );

        let settled = Arc::new(AtomicU8::new(0));
        let delivered = plain(&[], &settled);
        let delivered_at = delivered.payload().as_ptr() as usize;
        settle_nack_after(
            &mut Slot::new(delivered),
            "orders",
            Duration::from_secs(30),
            &delivery,
            unit_cx,
        )
        .await
        .unwrap();

        time::advance(Duration::from_secs(30)).await;

        let at = arrived.recv().await.expect("the copy is published");
        assert_ne!(
            at, delivered_at,
            "and what it lends is the dispatch's own buffer, read off the delivery before it settled",
        );
    }

    /// The deferred copy goes to the address the subscription's source reported, which is not the
    /// subscription's own name wherever the two are separate resources (a Pub/Sub subscription and
    /// its topic). Publishing under the subscription name would reach nothing there.
    #[tokio::test(start_paused = true)]
    async fn fallback_defers_republish_to_the_reported_address_with_incremented_retry_count() {
        let broker = MemoryBroker::new();
        // Subscribe before publishing: the in-memory broker does not buffer earlier messages.
        let mut sub = broker.subscribe("orders");
        let delivery = Delivery::deferring_to(
            bare_retry_publisher(broker.publisher()),
            "orders",
            TaskTracker::new(),
        );

        let settled = Arc::new(AtomicU8::new(0));
        let msg = plain(&[], &settled);
        settle_nack_after(
            &mut Slot::new(msg),
            "orders-workers",
            Duration::from_secs(30),
            &delivery,
            unit_cx,
        )
        .await
        .unwrap();

        // The original is dropped (nack(false)), not requeued, so the broker will not redeliver it.
        assert_eq!(settled.load(Ordering::SeqCst), 1);

        // Nothing is republished before the delay elapses.
        let mut stream = std::pin::pin!(sub.stream());
        assert!(poll!(stream.next()).is_pending());

        time::advance(Duration::from_secs(30)).await;
        task::yield_now().await;

        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.payload(), b"body");
        assert_eq!(
            redelivered.headers().get_str(RETRY_COUNT_HEADER),
            Some("1"),
            "the first deferred republish must carry retry-count 1",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_defers_republish_when_the_transport_cannot_settle() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("orders");
        let delivery = Delivery::deferring_to(
            bare_retry_publisher(broker.publisher()),
            "orders",
            TaskTracker::new(),
        );

        let settled = Arc::new(AtomicU8::new(0));
        let msg = plain_on(&[], &settled, Settlement::Unsupported);
        settle_nack_after(
            &mut Slot::new(msg),
            "orders",
            Duration::from_secs(30),
            &delivery,
            unit_cx,
        )
        .await
        .expect("an unsettleable transport is not a settle failure");
        // The drop is still attempted; the transport just has nothing to drop it with.
        assert_eq!(settled.load(Ordering::SeqCst), 1);

        time::advance(Duration::from_secs(30)).await;
        task::yield_now().await;

        let mut stream = std::pin::pin!(sub.stream());
        let redelivered = stream
            .next()
            .await
            .expect("the deferred copy is the only way the message survives here")
            .unwrap();
        assert_eq!(redelivered.payload(), b"body");
        assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), Some("1"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejected_settle_aborts_the_fallback() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("orders");
        let delivery = Delivery::deferring_to(
            bare_retry_publisher(broker.publisher()),
            "orders",
            TaskTracker::new(),
        );

        let settled = Arc::new(AtomicU8::new(0));
        let msg = plain_on(&[], &settled, Settlement::Rejected);
        let failed = settle_nack_after(
            &mut Slot::new(msg),
            "orders",
            Duration::from_secs(30),
            &delivery,
            unit_cx,
        )
        .await
        .expect_err("a broker that rejected the settle must be reported");
        assert!(matches!(failed, AckError::Timeout));
        // The abort happens at the settle, so the drop was attempted before it was reported.
        assert_eq!(settled.load(Ordering::SeqCst), 1);

        time::advance(Duration::from_secs(30)).await;
        task::yield_now().await;

        // The original is still the broker's to redeliver, so a deferred copy would duplicate it.
        let mut stream = std::pin::pin!(sub.stream());
        assert!(poll!(stream.next()).is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_increments_an_existing_retry_count() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("orders");
        let delivery = Delivery::deferring_to(
            bare_retry_publisher(broker.publisher()),
            "orders",
            TaskTracker::new(),
        );

        let settled = Arc::new(AtomicU8::new(0));
        let msg = plain(&[(RETRY_COUNT_HEADER, "4")], &settled);
        settle_nack_after(
            &mut Slot::new(msg),
            "orders",
            Duration::from_secs(1),
            &delivery,
            unit_cx,
        )
        .await
        .unwrap();

        time::advance(Duration::from_secs(1)).await;
        task::yield_now().await;

        let mut stream = std::pin::pin!(sub.stream());
        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), Some("5"));
    }

    #[tokio::test]
    async fn without_a_copy_path_a_delay_falls_back_to_a_requeue() {
        let delivery = Delivery::empty();
        let settled = Arc::new(AtomicU8::new(0));
        let msg = plain(&[], &settled);
        settle_nack_after(
            &mut Slot::new(msg),
            "orders",
            Duration::from_secs(30),
            &delivery,
            unit_cx,
        )
        .await
        .unwrap();
        // The broker moves this subscription's deliveries itself and has no delayed redelivery to
        // ride, so the requeue is all that is left - rather than dropping the message silently.
        assert_eq!(settled.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn native_support_defers_to_the_broker_nack_after() {
        // A native delivery: redelivered by its own timer, never through the retry publisher.
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("orders");
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("orders", b"native"), None)
            .await
            .unwrap();

        // A separate broker backs the retry publisher; if the fallback fired, the republish would
        // land here and never on `sub`.
        let other = MemoryBroker::new();
        let delivery = Delivery::deferring_to(
            bare_retry_publisher(other.publisher()),
            "orders",
            TaskTracker::new(),
        );

        let msg = {
            let mut stream = std::pin::pin!(sub.stream());
            stream.next().await.unwrap().unwrap()
        };
        assert!(msg.supports_nack_after());
        settle_nack_after(
            &mut Slot::new(msg),
            "orders",
            Duration::from_secs(5),
            &delivery,
            unit_cx,
        )
        .await
        .unwrap();

        time::advance(Duration::from_secs(5)).await;
        task::yield_now().await;

        let mut stream = std::pin::pin!(sub.stream());
        let redelivered = stream.next().await.unwrap().unwrap();
        // Native redelivery keeps the original payload and adds no retry-count header.
        assert_eq!(redelivered.payload(), b"native");
        assert_eq!(redelivered.headers().get_str(RETRY_COUNT_HEADER), None);
    }

    /// A subscription whose deliveries the broker moves itself carries no retry path, so a
    /// declaration on it is the broker's to apply: an immediate retry stays the broker's requeue.
    #[tokio::test]
    async fn a_broker_moved_subscription_keeps_its_own_requeue_under_a_cap() {
        let delivery = Delivery::empty()
            .declaring(RetryDeclaration::new().with_max_attempts(crate::nonzero!(3u32)));
        let settled = Arc::new(AtomicU8::new(0));
        settle_outcome(
            &mut Slot::new(plain(&[], &settled)),
            HandlerResult::retry(),
            "orders",
            &delivery,
            unit_cx,
        )
        .await;
        assert_eq!(settled.load(Ordering::SeqCst), 2);
    }

    /// The same subscription at the cap: the declaration reached the descriptor at startup, so the
    /// runtime never reads it again here. A rejection would settle the delivery ahead of the move the
    /// broker was about to make, and on a queue that deletes a rejected delivery it would lose it.
    #[tokio::test]
    async fn a_broker_moved_spent_delivery_stays_the_brokers_requeue() {
        let delivery = Delivery::empty().declaring(
            RetryDeclaration::new()
                .with_max_attempts(crate::nonzero!(1u32))
                .with_dead_letter("orders.dead"),
        );
        let settled = Arc::new(AtomicU8::new(0));
        settle_outcome(
            &mut Slot::new(plain(&[], &settled)),
            HandlerResult::retry(),
            "orders",
            &delivery,
            unit_cx,
        )
        .await;
        assert_eq!(settled.load(Ordering::SeqCst), 2);
    }

    /// A registration that declares nothing keeps the immediate retry it always had, whatever the
    /// transport: no copy, no count, the broker's own requeue.
    #[tokio::test]
    async fn an_undeclared_immediate_retry_stays_a_broker_requeue() {
        let broker = MemoryBroker::new();
        let delivery = Delivery::deferring_to(
            bare_retry_publisher(broker.publisher()),
            "orders",
            TaskTracker::new(),
        );
        let settled = Arc::new(AtomicU8::new(0));
        settle_outcome(
            &mut Slot::new(plain(&[], &settled)),
            HandlerResult::retry(),
            "orders",
            &delivery,
            unit_cx,
        )
        .await;
        assert_eq!(settled.load(Ordering::SeqCst), 2);
    }
}
