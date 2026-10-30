//! The dispatch side of the harness: the scope a dispatch task runs under, the entry points
//! the runtime records through, the records themselves, and the hooks slot that carries the
//! coordinator into dispatch.

use std::any::Any;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::HandlerResult;
use crate::pairing::Origin;
use crate::testing::attribution::{RecordedOptions, RuntimePublish, Span};
use crate::testing::hooks::{DueCall, DuePublish};
use crate::{OutgoingMessage, RawMessage};

use super::{Coordinator, LiveSubscription};

tokio::task_local! {
    /// The harness driving the current dispatch task, so a slot publisher and the batch settle
    /// path can reach it without threading a handle through every signature they sit behind.
    /// Installed by `dispatch` and `run_batch` around each handler invocation when a harness is
    /// attached.
    static HARNESS: HarnessScope;

    /// Set around one publish through a publisher the runtime paired: the broker it was paired
    /// against, which is the broker the message goes to.
    static PUBLISHING: Origin;
}

/// Runs `publish` as a publish to the broker `origin` names.
pub(crate) fn publishing_to<F: Future>(
    origin: Origin,
    publish: F,
) -> impl Future<Output = F::Output> {
    PUBLISHING.scope(origin, publish)
}

/// The broker the publish running in this task goes to, where the runtime paired its publisher.
fn publishing() -> Origin {
    PUBLISHING.try_with(|origin| *origin).unwrap_or_default()
}

/// The harness a dispatch task runs under: which coordinator records it, which broker's
/// registration the delivery belongs to, and which subscription of the app it came through.
pub(crate) struct HarnessScope {
    coordinator: Coordinator,
    scope_id: usize,
    subscription: usize,
    /// The calls this dispatch recorded from inside the handler (a batch records its call where
    /// it settles), with the hooks they owe, for the dispatch to run once it is done.
    calls: Mutex<Vec<DueCall>>,
}

impl HarnessScope {
    pub(crate) const fn new(
        coordinator: Coordinator,
        scope_id: usize,
        subscription: usize,
    ) -> Self {
        Self {
            coordinator,
            scope_id,
            subscription,
            calls: Mutex::new(Vec::new()),
        }
    }
}

/// Records a publish made through an `Out` slot, with the broker's per-message options it
/// carried, against the harness driving the current dispatch task, if any. Called by the slot
/// publisher wrapper on every publish; outside a harness-driven handler (production, or a test
/// without a `TestApp`) it is a no-op.
///
/// Returns the hooks registered on the slot's view, for the slot publisher to run once the
/// publish is through.
pub(crate) fn record_slot_publish<Options, Payload>(
    slot: &'static str,
    msg: &OutgoingMessage<'_, Payload>,
    options: Option<&Options>,
) -> DuePublish
where
    Options: Clone + Send + Sync + 'static,
    Payload: AsRef<[u8]>,
{
    HARNESS
        .try_with(|scope| {
            scope
                .coordinator
                .record_slot(slot, msg, RecordedOptions::capture(options))
        })
        .unwrap_or(DuePublish::NONE)
}

/// One message the app is handing a broker from a harness-driven task, with the broker's
/// per-message options it goes with, captured where its publish pipeline hands it over, so the
/// record holds what the broker is handed with every publish layer and transform applied. It
/// enters the harness's record once the broker took it: a publish the broker refused was not
/// published.
///
/// Outside a harness-driven task nothing is captured, and the calls cost a task-local lookup.
/// The record is what the `published` assertions read (the messages live, the options in both
/// modes), and what a live settle waits for the subscriptions to handle.
#[must_use]
pub(crate) struct PipelinePublish(Option<(Coordinator, Origin, RuntimePublish)>);

impl PipelinePublish {
    /// Captures `msg` and the `options` it is published with for the harness driving the current
    /// task, if any.
    pub(crate) fn capture<Payload, Options>(
        msg: &OutgoingMessage<'_, Payload>,
        options: Option<&Options>,
    ) -> Self
    where
        Payload: AsRef<[u8]>,
        Options: Clone + Send + Sync + 'static,
    {
        Self(
            HARNESS
                .try_with(|scope| {
                    let publish = RuntimePublish {
                        message: raw_of(msg),
                        options: RecordedOptions::capture(options),
                        span: Span {
                            started: scope.coordinator.tick(),
                            finished: 0,
                        },
                    };
                    (scope.coordinator.clone(), publishing(), publish)
                })
                .ok(),
        )
    }

    /// Records the captured message, the broker took it, and runs the hooks registered on the
    /// destination it went to.
    pub(crate) async fn sent(self) {
        if let Some((coordinator, origin, mut publish)) = self.0 {
            publish.span.finished = coordinator.tick();
            coordinator.record_sent(origin, publish).run().await;
        }
    }
}

/// The owned copy of `msg` the harness keeps.
fn raw_of<Payload: AsRef<[u8]>>(msg: &OutgoingMessage<'_, Payload>) -> RawMessage {
    RawMessage::new(msg.name().to_owned(), msg.payload().as_ref().to_vec())
        .with_headers(msg.headers().clone())
}

/// Records one batch against the harness driving the current dispatch task, if any: the batch
/// settle path knows the deliveries and their settlements but not which harness (if any) is
/// watching, and it sits behind four call sites whose signatures would otherwise all have to
/// carry one.
pub(crate) fn record_batch(name: &str, deliveries: Vec<Delivered>) {
    let _ = HARNESS.try_with(|scope| {
        let due = scope.coordinator.record(Record {
            scope_id: scope.scope_id,
            subscription: scope.subscription,
            name: name.to_owned(),
            deliveries,
            panicked: false,
            decode_failed: false,
        });
        if let Some(due) = due {
            scope
                .calls
                .lock()
                .expect("harness scope calls mutex poisoned")
                .push(due);
        }
    });
}

/// One batch's record still owed to the harness, held from the moment the settle path captures
/// the batch until the record is pushed.
///
/// The settlements a batch applies are what release the in-flight count, and the record is
/// written after the last of them, so without this a [`drive`](Coordinator::drive) could return
/// in between and a test would assert against a batch that has not been recorded yet.
pub(crate) struct PendingRecord(Option<Coordinator>);

impl PendingRecord {
    /// Takes the count against the harness driving the current dispatch task, if any.
    pub(crate) fn new() -> Self {
        Self(
            HARNESS
                .try_with(|scope| {
                    scope.coordinator.enqueued();
                    scope.coordinator.clone()
                })
                .ok(),
        )
    }
}

impl Drop for PendingRecord {
    fn drop(&mut self) {
        if let Some(coordinator) = &self.0 {
            coordinator.consumed();
        }
    }
}

/// Runs `fut` with the harness (when one is attached) visible to the recorders above.
pub(crate) async fn in_harness_scope<F: Future>(scope: Option<HarnessScope>, fut: F) -> F::Output {
    match scope {
        Some(scope) => HARNESS.scope(scope, fut).await,
        None => fut.await,
    }
}

/// Like [`in_harness_scope`], and hands back the calls `fut` recorded with hooks still to run.
pub(crate) async fn calls_in_harness_scope<F: Future>(
    scope: Option<HarnessScope>,
    fut: F,
) -> (F::Output, Vec<DueCall>) {
    match scope {
        Some(scope) => {
            HARNESS
                .scope(scope, async move {
                    let output = fut.await;
                    let calls = HARNESS.with(|scope| {
                        mem::take(
                            &mut *scope
                                .calls
                                .lock()
                                .expect("harness scope calls mutex poisoned"),
                        )
                    });
                    (output, calls)
                })
                .await
        }
        None => (fut.await, Vec::new()),
    }
}

/// One publish made through an `Out` slot: the slot's name, the outgoing message captured as a
/// [`RawMessage`] (destination, payload, headers), and the per-message options it carried.
#[derive(Clone, Debug)]
pub(crate) struct SlotRecord {
    pub(crate) slot: &'static str,
    pub(crate) message: RawMessage,
    pub(crate) options: RecordedOptions,
}

/// The classified outcome the harness records for one delivery to a handler.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum Outcome {
    /// The handler acknowledged the message.
    Ack,
    /// The handler nacked with requeue (the broker would redeliver).
    Nack,
    /// The handler dropped the message (nack without requeue).
    Drop,
    /// The delivery did not decode into the handler's input type, by the codec's verdict or by
    /// its broker's report.
    DecodeFailed,
    /// The handler panicked (under the panic failure policy in effect).
    Panicked,
}

/// One delivery inside a recorded handler call: the payload the handler saw, the value its
/// delivery lent where the handler took one ([`Carries`](crate::Carries)), and how the
/// dispatcher settled it (`None` when a fail-fast panic left the message unsettled).
#[doc(hidden)]
#[derive(Debug)]
pub struct Delivered {
    pub(crate) raw: Bytes,
    /// A clone of the carried value, taken only on the carried lane: that value never was bytes,
    /// so the payload cannot stand for it.
    pub(crate) value: Option<RecordedValue>,
    pub(crate) settle: Option<HandlerResult>,
}

/// A carried value as the harness keeps it: cloned once, read back by its own type.
pub(crate) type RecordedValue = Arc<dyn Any + Send + Sync>;

/// One recorded call into a handler: a single delivery, or a whole batch.
pub(crate) struct Record {
    /// The broker's registration index in the app, used to scope assertions per broker.
    pub(crate) scope_id: usize,
    /// The subscription of the app the call came through, as [`TestHooks::subscribed`] numbered
    /// it: what tells apart two subscriptions that report one name.
    pub(crate) subscription: usize,
    /// The subscription (channel) name the message arrived on.
    pub(crate) name: String,
    /// What this call carried: exactly one delivery for a single-message handler, one per
    /// element of the batch for a batch handler.
    pub(crate) deliveries: Vec<Delivered>,
    /// Whether the handler panicked.
    pub(crate) panicked: bool,
    /// Whether the delivery failed to decode before the handler ran.
    pub(crate) decode_failed: bool,
}

impl Record {
    /// Classifies this call into a single [`Outcome`]. Panic and decode-failure dominate the
    /// settlement (a fail-fast panic acks nothing; a skip-policy panic still records `Panicked`).
    ///
    /// A batch settling its elements differently has no one outcome; the first element's stands
    /// for the call, which is why the per-element assertions
    /// ([`settled`](super::SubscriberAssertions::settled)) are what a mixed batch is read with.
    pub(crate) fn outcome(&self) -> Outcome {
        if self.panicked {
            Outcome::Panicked
        } else if self.decode_failed {
            Outcome::DecodeFailed
        } else {
            settled_as(self.deliveries.first().and_then(|one| one.settle))
        }
    }
}

/// The outcome one settlement classifies as. An unsettled delivery (a fail-fast panic tore the
/// service down before it settled) reads as dropped: nothing acknowledged it.
fn settled_as(settle: Option<HandlerResult>) -> Outcome {
    match settle {
        Some(HandlerResult::Ack) => Outcome::Ack,
        Some(HandlerResult::Nack { requeue: true } | HandlerResult::NackAfter { .. }) => {
            Outcome::Nack
        }
        Some(HandlerResult::Nack { requeue: false }) | None => Outcome::Drop,
    }
}

/// A shared slot installed once per broker scope into the dispatch `Delivery`.
///
/// It is empty in production (the `testing` feature can be on without a harness running), so the
/// per-delivery read is a single atomic load returning `None`. The harness fills it before any
/// dispatch task starts, so the read path never races the write.
// Machinery the harness crate drives, hidden from the docs; it holds erased closures and
// futures that have no useful `Debug`.
#[allow(missing_debug_implementations)]
pub struct TestHooks {
    coordinator: OnceLock<Coordinator>,
    /// Every subscription the app mounts, as `(broker scope, subscription name)` in mount order,
    /// noted while the app is built; a subscription's position here is its identity. A live settle
    /// waits only for what reaches one of these: a publish to a name nothing in the app consumes
    /// has nothing to be handled by.
    subscriptions: Mutex<Vec<(usize, String)>>,
}

impl TestHooks {
    /// A hooks slot that is never installed: the production / no-harness path.
    pub(crate) fn detached() -> Self {
        Self {
            coordinator: OnceLock::new(),
            subscriptions: Mutex::new(Vec::new()),
        }
    }

    /// Notes that the broker registered at `scope_id` carries a subscription named `name`, and
    /// returns the subscription's identity. Two subscriptions may report one name (two
    /// subscriptions on one Pulsar topic both report the topic), so the name does not identify
    /// one.
    pub(crate) fn subscribed(&self, scope_id: usize, name: &str) -> usize {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .expect("test hooks subscriptions mutex poisoned");
        subscriptions.push((scope_id, name.to_owned()));
        subscriptions.len() - 1
    }

    /// Every subscription the app mounts, in mount order.
    #[doc(hidden)]
    pub fn subscriptions(&self) -> Vec<LiveSubscription> {
        self.subscriptions
            .lock()
            .expect("test hooks subscriptions mutex poisoned")
            .iter()
            .enumerate()
            .map(|(id, (scope_id, name))| LiveSubscription {
                id,
                scope_id: *scope_id,
                name: name.clone(),
            })
            .collect()
    }

    /// Installs the coordinator for a harness run. Idempotent; a second install is ignored.
    #[doc(hidden)]
    pub fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// The installed coordinator, or `None` when no harness is driving this app.
    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }
}
