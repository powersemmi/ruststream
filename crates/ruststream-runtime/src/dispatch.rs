//! The per-subscriber dispatch loop: pulls messages off one subscriber and invokes its handler
//! until shutdown is signalled or the stream ends. The runner spawns it, on the runtime it owns.

#[cfg(feature = "testing")]
use alloc::borrow::ToOwned;
#[cfg(any(feature = "std", feature = "testing"))]
use alloc::format;
#[cfg(feature = "testing")]
use alloc::vec;
#[cfg(any(feature = "std", feature = "testing"))]
use core::any::Any;
use core::cell::Cell;
#[cfg(not(any(feature = "std", feature = "testing")))]
use core::convert::Infallible;
#[cfg(any(feature = "std", feature = "testing"))]
use core::panic::AssertUnwindSafe;
use core::pin::Pin;
use core::task::{Context as TaskContext, Poll};
#[cfg(feature = "testing")]
use ruststream_core::__alloc::bytes::Bytes;
use ruststream_core::{MaybeSendSync, ReadyHint};

#[cfg(any(feature = "std", feature = "testing"))]
use futures::FutureExt;
use futures::Stream;
use pin_project_lite::pin_project;
use ruststream_core::EncodeBuf;
#[cfg(not(ruststream_alloc))]
use ruststream_core::FixedBuf;
#[cfg(ruststream_alloc)]
use ruststream_core::PayloadBuf;
use ruststream_core::diag::error;

#[cfg(not(ruststream_alloc))]
use crate::publish_failure::FailureSlot;
use crate::publish_failure::Reaction;
use crate::redelivery::RetryCopies;
use crate::shared::{StateHandle, SubscriptionName, state_in_place};
use crate::{BuildContext, HeaderSet, Settle, Subscriber};

use super::batch::{BatchHandler, TakenBatch};
use super::context::Context;
use super::failure::DispatchFailure;
#[cfg(any(feature = "std", feature = "testing"))]
use super::failure::{FailurePolicy, panic_reason};
use super::handler::Handler;
use super::handler::HandlerOutcome;
#[cfg(doc)]
use super::handler::HandlerResult;
use super::host::{Halt, HostDetach, PrepareKept, SendHost, ShutdownWait, detach_continuation};
#[cfg(feature = "testing")]
use crate::testing::coordinator::{
    Coordinator, Delivered, HarnessScope, Record, calls_in_harness_scope, in_harness_scope,
};
#[cfg(feature = "testing")]
use crate::testing::hooks::DueCall;
#[cfg(feature = "testing")]
use crate::testing::{Bookkept, harness_section};
pub use delivery::{Delivery, Lent, Slot};
pub use headers::{HeadersAtOnce, HeadersOnDemand, LendHeaders};
pub(crate) use settle::{settle_outcome, settle_reacting};
pub use workers::{Placement, Workers};

/// Header carrying the framework's own retry count.
///
/// The runtime increments it on every copy of a delivery it publishes, and reads it back where the
/// transport counts nothing of its own
/// ([`IncomingMessage::redelivery_count`](crate::IncomingMessage::redelivery_count)): that is what
/// a registration's [`max_attempts`](super::RouterWith::max_attempts) cap counts there, on the
/// runtime's copy path and on a delay a broker crate honours by publishing a copy of its own
/// alike. A handler can read it too, to tell a first delivery from a redelivery.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::runtime::RETRY_COUNT_HEADER;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn reconcile(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
///     // The copies the runtime publishes count themselves; the first delivery carries no header.
///     let attempt: u64 = ctx
///         .headers()
///         .get_str(RETRY_COUNT_HEADER)
///         .and_then(|v| v.parse().ok())
///         .unwrap_or(0);
///     if attempt >= 3 {
///         tracing::warn!(order.id, attempt, "giving up");
///         return HandlerOutcome::drop();
///     }
///     HandlerOutcome::retry()
/// }
/// # }
/// # fn main() {}
/// ```
pub const RETRY_COUNT_HEADER: &str = "x-ruststream-retry-count";

/// The storage a dispatch loop encodes its replies into, owned by the loop for as long as it runs.
///
/// A runner hands the loop the room its registration asked for: a [`PayloadBuf`] by default,
/// or without `alloc` a `FixedBuf` of the capacity `.encode_capacity::<N>()` named. The loop
/// reads it as an [`EncodeBuf`], so no code that writes depends on the capacity.
#[doc(hidden)]
pub trait EncodeRoom {
    /// The buffer replies are encoded into.
    fn buffer(&mut self) -> &mut EncodeBuf;
}

#[cfg(ruststream_alloc)]
impl EncodeRoom for PayloadBuf {
    fn buffer(&mut self) -> &mut EncodeBuf {
        self
    }
}

#[cfg(not(ruststream_alloc))]
impl<const CAPACITY: usize> EncodeRoom for FixedBuf<CAPACITY> {
    fn buffer(&mut self) -> &mut EncodeBuf {
        self
    }
}

/// The handler's future, observed by the poll-time diagnostics where the feature is on and handed
/// through untouched where it is off.
#[cfg(feature = "poll-diagnostics")]
macro_rules! observed {
    ($delivery:expr, $future:expr) => {
        $delivery.poll.observe($future)
    };
}

/// See the feature's counterpart above: without the feature, the expression itself.
#[cfg(not(feature = "poll-diagnostics"))]
macro_rules! observed {
    ($delivery:expr, $future:expr) => {
        $future
    };
}

/// Drives `subscriber` through `handler` until `shutdown` is signalled or the stream ends: the
/// sequential loop a runner spawns for a subscription. Each delivery is given a [`Context`] built
/// from `name`, the message headers, shared `state`, and the publish context `lent` holds;
/// `held` lives as long as the loop (a runner's guard on its shutdown count, say), and `headers`
/// says how each delivery lends its headers to the context (see [`LendHeaders`]).
// The parts are independent and each runner passes its own; bundling them into a struct would
// hide that.
// Written over the contract traits, so the future is `Send` exactly where the types it runs are: a runner that spawns it on a multi-thread runtime proves that for its own.
#[allow(clippy::future_not_send)]
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn run_sequential<S, H, C, St, Signal, Lending, Held, Headers, Park, Room>(
    mut subscriber: S,
    handler: H,
    shutdown: Signal,
    name: SubscriptionName,
    state: StateHandle<'_, St>,
    lent: Lending,
    failure: DispatchFailure<Signal>,
    held: Held,
    headers: Headers,
    parked: Park,
    mut room: Room,
) where
    S: Subscriber,
    H: Handler<S::Message, C, St>,
    Headers: LendHeaders<S::Message>,
    // See `dispatch`.
    C: BuildContext<S::Message> + MaybeSendSync + 'static,
    Signal: ShutdownWait + Halt,
    Lending: Lent<C>,
    Park: Parks,
    Room: EncodeRoom,
{
    // Owned by the loop itself, so a runner spawns this future as it is, with no future of its
    // own around it: the loop is the task.
    let _held = held;
    let delivery = lent.delivery();
    let host = lent.host();
    // Taken before the stream opens: the stream borrows the subscriber for the whole loop.
    let probe = subscriber.ready_hint();
    let mut stream = core::pin::pin!(subscriber.stream());
    let mut cancelled = core::pin::pin!(shutdown.cancelled());
    // One encode buffer for the whole loop: a reply that leaves through a publisher which
    // reads the payload is written into this one every time, so the subscription allocates
    // it once and never again. The runner chose its room; the loop owns it.
    let encode = room.buffer();
    // The loop's slot, one for the whole loop: the stream's poll writes each delivery
    // straight into it, and the settle takes it out again. A delivery returned by value
    // through the turn and bound to a local on its way here would be copied twice more.
    let mut slot = Slot::empty();
    loop {
        match turn_into(
            &shutdown,
            &probe,
            stream.as_mut(),
            cancelled.as_mut(),
            &mut slot,
            parked,
        )
        .await
        {
            Turn::Delivery(Ok(())) => {
                // Pinned rather than awaited in place: a delivery's future carries the
                // context, the handler's own state and the settle path, and awaiting the
                // call expression makes the loop build it on the stack and copy it into
                // its own state on every delivery.
                let handling = core::pin::pin!(dispatch(
                    &handler,
                    &mut slot,
                    &mut *encode,
                    &name,
                    state_in_place(&state),
                    delivery,
                    host,
                    &failure,
                    headers,
                ));
                handling.await;
            }
            Turn::Delivery(Err(err)) => {
                error!(
                    target: "ruststream::dispatch",
                    error = %err,
                    "subscriber stream error",
                );
            }
            Turn::Ended => {
                shutdown.stream_ended(&name);
                break;
            }
            Turn::Shutdown => break,
        }
    }
}

/// Waits for whichever comes first: the next item off `stream`, or `shutdown`. A delivery is
/// written into the loop's slot where it lands rather than returned by value.
///
/// The stream is polled first and `cancelled` only where it answers `Pending`, so a turn that
/// finds a delivery ready never touches the token's waiter list: what shutdown costs a busy
/// subscription is the one relaxed load of the shutdown's flag. `cancelled` is the token's own
/// wait future, built once per subscription by the caller and pinned across the whole loop, so
/// the turn that does park registers with the waiter list once rather than on every iteration,
/// which is what a `select!` over `cancelled()` charges.
///
/// # Cancel safety
///
/// Both halves are cancel-safe: a delivery is written into the slot in the same poll that takes
/// it off the stream, and a registration with the token's waiter list outlives the poll. Once
/// `cancelled` has resolved the flag stays set - a token never un-cancels - so the check at the
/// top answers every later turn and the resolved future is never polled again.
///
/// A subscription whose probe declares readiness is polled again within a turn only once the probe
/// says something may be ready (see [`idle`]); the first poll of every turn is real, so the
/// stream and the token have registered with the task before any poll is skipped.
///
/// The other loops write the same wait out rather than share it: one instantiation shared with
/// them is enough for the compiler to stop inlining it here.
fn turn_into<'s, St, Message, Failure, Wait, Signal, Probe, Park>(
    shutdown: &'s Signal,
    probe: &'s Probe,
    stream: Pin<&'s mut St>,
    cancelled: Pin<&'s mut Wait>,
    slot: &'s mut Slot<Message>,
    flag: Park,
) -> TurnInto<'s, St, Message, Wait, Signal, Probe, Park>
where
    Park: Parks,
    St: Stream<Item = Result<Message, Failure>>,
    Wait: Future<Output = ()>,
    Signal: ShutdownWait,
    Probe: ReadyHint,
{
    TurnInto {
        shutdown: shutdown.is_cancelled(),
        signal: shutdown,
        probe,
        parked: false,
        flag,
        stream,
        cancelled,
        slot,
    }
}

/// Whether an idle subscription may skip the poll of its stream and of its shutdown wait.
///
/// The poll is skipped only after a poll of the same wait that answered `Pending` (`parked`), so
/// the stream and the shutdown token have registered with the task and a later wake still reaches
/// it; only where the probe says nothing is ready; and never once shutdown is signalled, because a
/// stop wakes the task and a skipped wait would not see it. A probe that declares none makes the
/// answer the constant `false`.
#[doc(hidden)]
#[inline]
pub fn idle<Probe, Signal>(parked: bool, probe: &Probe, shutdown: &Signal) -> bool
where
    Probe: ReadyHint,
    Signal: ShutdownWait,
{
    Probe::GATED && parked && !probe.may_be_ready() && !shutdown.is_cancelled()
}

/// Where a subscription's loop records that it is idle, for a join above it to read.
///
/// `()` records nothing, which is what a loop that no join gates passes; a `&Cell<bool>` is the
/// flag a [`Gated`](crate::Gated) reads. The loop sets it when a turn found the stream and the
/// shutdown wait both pending, and clears it when the turn resumes, so a handler that awaits is
/// never taken for an idle subscription.
#[doc(hidden)]
pub trait Parks: Copy + Unpin {
    /// Records whether the subscription is idle.
    fn park(self, parked: bool);
}

impl Parks for () {
    #[inline]
    fn park(self, _parked: bool) {}
}

impl Parks for &Cell<bool> {
    #[inline]
    fn park(self, parked: bool) {
        self.set(parked);
    }
}

pin_project! {
    /// A subscription's loop under the join's gate: while the loop recorded that it is idle and
    /// its probe says nothing is ready, the join does not resume the loop's frame.
    ///
    /// The flag lives in the app's future, above the join, and the loop borrows it (see
    /// [`Parks`]); the gate holds the probe and a handle on the shutdown signal. A subscription
    /// whose probe declares none has a gate that is the constant `false`, and the poll is the
    /// loop's own.
    #[doc(hidden)]
    pub struct Gated<'flag, Probe, Signal, Work> {
        parked: &'flag Cell<bool>,
        probe: Probe,
        signal: Signal,
        #[pin]
        work: Work,
    }
}

impl<'flag, Probe, Signal, Work> Gated<'flag, Probe, Signal, Work> {
    /// Gates `work`, a loop that records its idleness in `parked`.
    pub const fn new(parked: &'flag Cell<bool>, probe: Probe, signal: Signal, work: Work) -> Self {
        Self {
            parked,
            probe,
            signal,
            work,
        }
    }
}

impl<Probe, Signal, Work> Future for Gated<'_, Probe, Signal, Work>
where
    Probe: ReadyHint,
    Signal: ShutdownWait,
    Work: Future<Output = ()>,
{
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        let this = self.project();
        if idle(this.parked.get(), this.probe, this.signal) {
            // The loop's last real poll registered the task's waker with the stream and the
            // shutdown wait, so a delivery or a stop wakes the app and the gate asks again.
            return Poll::Pending;
        }
        this.work.poll(cx)
    }
}

/// The future [`turn_into`] returns: written out rather than an `async fn` so its poll carries an
/// inline hint, which a closure inside `poll_fn` cannot.
struct TurnInto<'s, St, Message, Wait, Signal, Probe, Park> {
    shutdown: bool,
    signal: &'s Signal,
    probe: &'s Probe,
    // Set once a poll of this turn found the stream and the shutdown wait both pending.
    parked: bool,
    // Where the join above this loop reads that the subscription is idle.
    flag: Park,
    stream: Pin<&'s mut St>,
    cancelled: Pin<&'s mut Wait>,
    slot: &'s mut Slot<Message>,
}

impl<St, Message, Failure, Wait, Signal, Probe, Park> Future
    for TurnInto<'_, St, Message, Wait, Signal, Probe, Park>
where
    Park: Parks,
    St: Stream<Item = Result<Message, Failure>>,
    Wait: Future<Output = ()>,
    Signal: ShutdownWait,
    Probe: ReadyHint,
{
    type Output = Turn<Result<(), Failure>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // Every field is a reference or a flag, so the future is `Unpin`.
        let this = self.get_mut();
        if this.shutdown {
            return Poll::Ready(Turn::Shutdown);
        }
        if idle(this.parked, this.probe, this.signal) {
            return Poll::Pending;
        }
        // A resumed subscription is not idle until a poll of this turn finds it so again.
        if Probe::GATED {
            this.flag.park(false);
        }
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                this.slot.fill(msg);
                Poll::Ready(Turn::Delivery(Ok(())))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Turn::Delivery(Err(err))),
            Poll::Ready(None) => Poll::Ready(Turn::Ended),
            Poll::Pending => {
                let waiting = this.cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown);
                this.parked = waiting.is_pending();
                if Probe::GATED {
                    this.flag.park(this.parked);
                }
                waiting
            }
        }
    }
}

/// What one turn of a dispatch loop found on its subscriber.
#[doc(hidden)]
#[derive(Debug)]
pub enum Turn<T> {
    /// The subscriber yielded a delivery, or an error reading one.
    Delivery(T),
    /// The subscriber's stream ended; the loop is done.
    Ended,
    /// Shutdown was signalled; the loop stops without touching the stream again.
    Shutdown,
}

/// The policy a subscription's loop runs under: the declared one, except under the harness.
// Without either feature the delivery has nothing to say about the placement.
#[cfg_attr(
    not(any(feature = "poll-diagnostics", feature = "testing")),
    allow(unused_variables)
)]
#[doc(hidden)]
#[must_use]
pub fn placed<C>(workers: Workers, delivery: &Delivery<C>) -> Workers {
    // Before the harness maps the placement away: the declaration is what the warning advises.
    #[cfg(feature = "poll-diagnostics")]
    if matches!(workers.placement, Placement::Threads) {
        delivery.poll.on_dedicated_threads();
    }
    // The harness drives every subscription on the test's own runtime, so a timer the handler
    // arms is one `advance` reaches; it does not reproduce the thread topology.
    #[cfg(feature = "testing")]
    if delivery.hooks.coordinator().is_some() {
        return workers.on_runtime();
    }
    workers
}

/// The failure buffer of a delivery, which outlives its context: the `.error` it holds runs after
/// the settle.
#[cfg(not(ruststream_alloc))]
type Failures = Option<FailureSlot>;
#[cfg(ruststream_alloc)]
struct Failures;

#[cfg(not(ruststream_alloc))]
fn take_failures<C, S>(ctx: &mut Context<'_, C, S>) -> Failures {
    ctx.take_failures()
}

#[cfg(ruststream_alloc)]
const fn take_failures<C, S>(_ctx: &mut Context<'_, C, S>) -> Failures {
    Failures
}

#[cfg(not(ruststream_alloc))]
async fn react_to(failures: Failures) {
    if let Some(mut failures) = failures {
        failures.react().await;
    }
}

#[cfg(ruststream_alloc)]
#[allow(clippy::unused_async)]
async fn react_to(_failures: Failures) {}

/// One delivery, end to end: the handler, the settle and what the delivery leaves behind.
// Written over the contract traits, so the future is `Send` exactly where the types it runs are: a runner that spawns it on a multi-thread runtime proves that for its own.
#[allow(clippy::future_not_send)]
#[doc(hidden)]
// Each part is the delivery's own or the runner's; bundling them would hide which is which.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch<H, M, C, St, Signal, Host, Headers>(
    handler: &H,
    slot: &mut Slot<M>,
    encode: &mut EncodeBuf,
    name: &str,
    state: &St,
    delivery: &Delivery<C>,
    host: &Host,
    failure: &DispatchFailure<Signal>,
    _headers: Headers,
) where
    H: Handler<M, C, St>,
    // A deferred retry copy carries the broker's context into the work the delivery leaves
    // behind, which may run on another thread.
    C: BuildContext<M> + MaybeSendSync + 'static,
    M: Settle,
    Signal: Halt,
    Host: HostDetach<Kept: PrepareKept<C>, Retry: RetryCopies<C>>,
    Headers: LendHeaders<M>,
{
    // Settling the message is what releases the harness's quiescence wait, and the post-settle
    // continuations are spawned after it; one in-flight token spanning the whole dispatch keeps a
    // `drain` from running before they exist.
    #[cfg(feature = "testing")]
    let watcher = delivery.hooks.coordinator().cloned();
    #[cfg(feature = "testing")]
    if let Some(coordinator) = &watcher {
        coordinator.enqueued();
    }
    // Build the broker's typed per-delivery context from the message, then attach the fail-fast
    // handle. The delivery is borrowed out of the slot for the whole handler phase and moves
    // only at the settle below.
    // The context lives in a block of its own and is dropped where it stands when the block ends:
    // a `drop(ctx)` call would move the whole context out of this future's state into the call's
    // argument first, a `memcpy` of the context per delivery.
    // The hooks a test registered on this subscription, owed by this call once it is settled.
    #[cfg(feature = "testing")]
    let mut due_calls = None;
    let (settle, after, continuations, reaction, failures) = {
        let msg = slot.get();
        let cx = C::build(msg);
        // The delivery itself where it can be shared, not its header map: a handler that reads no
        // headers never reaches the broker's accessor, which is a parse and a buffer on most
        // transports.
        let source = Headers::source(msg);
        let mut ctx = Context::new(name, &source, state, cx, delivery)
            .with_failfast(&failure.shutdown)
            .with_encode_buffer(encode)
            .with_decode_policy(failure.policies.decode);
        // Catch a panicking handler so it cannot silently kill the dispatch loop (which would stop
        // the subscriber consuming) or leave the message unsettled. The call runs inside the
        // caught future, so a handler (a middleware) that panics while building its future is
        // caught too. AssertUnwindSafe is required
        // because the future borrows `&mut ctx`; that state is discarded with the failed delivery.
        // Under the harness, the invocation runs in a task-local slot scope so publishes made
        // through injected `Out` publishers are attributed to their slot.
        #[cfg(feature = "testing")]
        let result = in_harness_scope(
            harness_scope(delivery),
            observed!(
                delivery,
                AssertUnwindSafe(async { handler.handle(msg, &mut ctx).await }).catch_unwind()
            ),
        )
        .await;
        #[cfg(all(feature = "std", not(feature = "testing")))]
        let result = observed!(
            delivery,
            AssertUnwindSafe(async { handler.handle(msg, &mut ctx).await }).catch_unwind()
        )
        .await;
        // Without `std` a panic aborts, so the handler's future runs uncaught.
        #[cfg(not(any(feature = "std", feature = "testing")))]
        let result = Ok::<_, Infallible>(observed!(delivery, handler.handle(msg, &mut ctx)).await);
        #[cfg(feature = "testing")]
        let panicked = result.is_err();
        // Resolve into a `HandlerOutcome` regardless of whether the handler panicked.
        let settled = match result {
            Ok(s) => Some(s),
            #[cfg(any(feature = "std", feature = "testing"))]
            Err(payload) => outcome_after_panic(payload.as_ref(), name, failure),
        };
        // Drain the matching post-settle hooks BEFORE settling: `ctx` borrows `msg`'s headers, and
        // settling consumes `msg`. The drained futures own their captures. A fail-fast (no
        // settlement) runs no hooks. Most deliveries register none, and those pay the branch alone:
        // the list, its scan and the drop glue of both belong to the deliveries that did register
        // one.
        // What a publish made for this settlement hands its result to: the `.error` of the outcome
        // (with the `.and_after` beside it, which then follows the publish), else the one the
        // context registered. Most deliveries carry neither, and pay the two checks.
        // The outcome is taken apart here, so none of it is left to drop once it is settled.
        let failures = take_failures(&mut ctx);
        let (settle, after, reaction) = match settled.map(HandlerOutcome::into_parts) {
            Some((status, after, Some(error))) => {
                ctx.demote_errors();
                (Some(status), None, Some(Reaction::new(error, after)))
            }
            Some((status, after, None)) => (Some(status), after, ctx.take_reaction_for(status)),
            None => (None, None, None),
        };
        let continuations = match settle {
            Some(status) if ctx.has_hooks() => Some(ctx.take_hooks_for(status)),
            _ => None,
        };
        // The harness records what the handler saw and how it settled, BEFORE settling the
        // message: the matching decrement runs in the broker message's `Drop` (during
        // `settle_outcome`, or at the end of this function on the fail-fast path), so the record
        // is in place by the time `drive` wakes. Captured here because `settle_outcome` consumes
        // `msg` and dropping `ctx` clears the decode flag.
        #[cfg(feature = "testing")]
        if let Some(coordinator) = delivery.hooks.coordinator() {
            // The record is the harness's own bookkeeping, not the delivery's cost.
            due_calls = harness_section(|| {
                let deliveries = vec![Delivered {
                    raw: Bytes::copy_from_slice(msg.payload()),
                    value: ctx.take_carried(),
                    settle,
                }];
                record_call(
                    coordinator,
                    delivery,
                    name,
                    deliveries,
                    panicked,
                    ctx.took_decode_failed(),
                )
            });
        }
        (settle, after, continuations, reaction, failures)
    };
    if let Some(status) = settle {
        // Named for the same reason as the delivery's own future above: the settle path is
        // built where it is polled instead of being copied into this future's state.
        if reaction.is_some() {
            let settling = settle_reacting(slot, status, reaction, name, delivery, host, C::build);
            settling.await;
        } else {
            let settling = settle_outcome(slot, status, name, delivery, host, C::build);
            settling.await;
        }
        // Spawn the `and_after` continuation (if any) onto the tracked set so a graceful shutdown
        // drains it. At-most-once: the message is already settled, so a lost or panicking
        // continuation never redelivers it.
        if let Some(after) = after {
            detach_continuation!(host, after);
        }
    } else {
        // A fail-fast left the delivery unsettled: it is released here, as it always was at the
        // end of its dispatch, rather than when the loop's slot is next filled.
        drop(slot.take());
    }
    // Context-registered hooks run after the message is settled: at-most-once, off the delivery
    // path. They ride the same app-wide tracker as an `and_after` continuation, so one drain
    // covers both - the harness's `drain` and the shutdown's alike.
    if let Some(continuations) = continuations {
        for fut in continuations {
            detach_continuation!(host, fut);
        }
    }
    react_to(failures).await;
    // The hooks a test registered on this subscription run once the call is settled, inside the
    // dispatch's own in-flight count, so a publish returns only after they are done.
    #[cfg(feature = "testing")]
    if let Some(coordinator) = &watcher {
        coordinator.run_calls(due_calls).await;
        coordinator.consumed();
    }
}

#[cfg(any(feature = "std", feature = "testing"))]
/// What a delivery whose handler panicked settles by: the panic policy's settlement, or `None`
/// where a fail-fast panic tore the service down and left the delivery unsettled (a broker with
/// redelivery hands it back after the restart).
#[inline]
fn outcome_after_panic<Signal: Halt>(
    payload: &(dyn Any + Send),
    name: &str,
    failure: &DispatchFailure<Signal>,
) -> Option<HandlerOutcome> {
    let reason = panic_reason(payload);
    error!(
        target: "ruststream::dispatch",
        subscription = %name,
        panic = %reason,
        "handler panicked",
    );
    match failure.policies.panic {
        FailurePolicy::FailFast => {
            failure
                .shutdown
                .fail(name, &format!("handler panicked: {reason}"));
            None
        }
        other => Some(
            other
                .settlement()
                .map_or_else(HandlerOutcome::drop, Into::into),
        ),
    }
}

/// Records one call of this subscription with the harness, and hands back the hooks a test
/// registered on it, which the call now owes.
#[cfg(feature = "testing")]
fn record_call<C>(
    coordinator: &Coordinator,
    delivery: &Delivery<C>,
    name: &str,
    deliveries: impl IntoIterator<Item = Delivered>,
    panicked: bool,
    decode_failed: bool,
) -> Option<DueCall> {
    coordinator.record(Record {
        scope_id: delivery.scope_id,
        subscription: delivery.subscription,
        name: name.to_owned(),
        deliveries: deliveries.into_iter().collect(),
        panicked,
        decode_failed,
    })
}

/// Runs one batch through its handler under panic protection. The handler owns and settles the
/// batch's deliveries, so a panic there has already consumed them: the panic policy can only tear
/// the service down (`fail_fast`) or be logged and skipped. Per-element settlement is out of scope
/// (see the batch decode path for per-element decode handling). Ungated `after_settle` hooks run
/// once the batch has settled (per-element outcomes make a gated hook ill-defined on a batch).
// The loop hands this everything one batch needs, the buffers it lends included: see
// spawn_dispatch_workers.
// Written over the contract traits, so the future is `Send` exactly where the types it runs are: a runner that spawns it on a multi-thread runtime proves that for its own.
#[allow(clippy::future_not_send)]
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn run_batch<H, Batch, C, St, Signal, Host>(
    handler: &H,
    slot: &mut Batch,
    taken: &mut H::Taken,
    scratch: &mut H::Scratch,
    encode: &mut EncodeBuf,
    name: &str,
    state: &St,
    delivery: &Delivery<C>,
    host: &Host,
    failure: &DispatchFailure<Signal>,
) where
    H: BatchHandler<Batch, C, St>,
    Signal: Halt,
    Host: SendHost<C>,
{
    // The broker filled `slot`; the exchange leaves it empty for the next batch, and the
    // handler's own buffer holds this one.
    H::load(taken, slot);
    let batch = taken;
    // A batch spans many deliveries, so its context carries only subscription-scoped data, built
    // from its first delivery, or from the batch itself where the handler lends its values; the
    // shared app state is threaded the same way as on the single-message path. A batch with
    // nothing in it has nothing to settle and nothing to build a context from.
    let Some(cx) = batch.context() else { return };
    let empty = HeaderSet::new();
    let mut ctx = Context::new(name, &empty, state, cx, delivery)
        .with_failfast(&failure.shutdown)
        .with_encode_buffer(encode)
        .with_decode_policy(failure.policies.decode);
    // See `dispatch`: the harness scope attributes `Out` publishes to their slot, and lets the
    // batch settle path record the batch it applied.
    // A batch settles its own deliveries inside the handler (a panic settles them by dropping
    // them), so the last decrement lands before the batch record and the fail-fast signal below.
    // One extra in-flight token spans the whole dispatch, so a harness driving to quiescence
    // cannot return into that window.
    #[cfg(feature = "testing")]
    let watcher = delivery.hooks.coordinator().cloned();
    // A panicking batch settles nothing, so what it carried is captured here (the handler owns
    // the deliveries and a panic consumes them) to record the call the settle path never reached.
    // Only a harness reads that record, so a dispatch no harness watches captures nothing.
    #[cfg(feature = "testing")]
    let unsettled = watcher
        .as_ref()
        .map(|_| harness_section(|| Bookkept::new(batch.unsettled())));
    #[cfg(feature = "testing")]
    if let Some(coordinator) = &watcher {
        coordinator.enqueued();
    }
    // A batch records its call where it settles, inside the handler, so the scope hands back the
    // hooks that call owes.
    #[cfg(feature = "testing")]
    let (result, mut due_calls) = calls_in_harness_scope(
        harness_scope(delivery),
        observed!(
            delivery,
            AssertUnwindSafe(async {
                handler
                    .handle_batch(&mut *batch, scratch, &mut ctx, host)
                    .await;
            })
            .catch_unwind()
        ),
    )
    .await;
    #[cfg(all(feature = "std", not(feature = "testing")))]
    let result = observed!(
        delivery,
        AssertUnwindSafe(async {
            handler
                .handle_batch(&mut *batch, scratch, &mut ctx, host)
                .await;
        })
        .catch_unwind()
    )
    .await;
    // Without `std` a panic aborts, so the handler's future runs uncaught.
    #[cfg(not(any(feature = "std", feature = "testing")))]
    let result: Result<(), Infallible> = {
        observed!(
            delivery,
            handler.handle_batch(&mut *batch, scratch, &mut ctx, host)
        )
        .await;
        Ok(())
    };
    match result {
        Ok(()) => {
            // As on the single-message path: a batch that registered no hook pays the branch.
            if ctx.has_hooks() {
                for fut in ctx.take_settle_hooks() {
                    detach_continuation!(host, fut);
                }
            }
        }
        #[cfg(any(feature = "std", feature = "testing"))]
        Err(payload) => {
            let reason = panic_reason(payload.as_ref());
            error!(
                target: "ruststream::dispatch",
                subscription = %name,
                panic = %reason,
                "batch handler panicked",
            );
            // What the panic left unsettled is dropped, so the worker's storage serves the next
            // batch.
            batch.discard();
            #[cfg(feature = "testing")]
            if let (Some(coordinator), Some(deliveries)) = (&watcher, unsettled) {
                due_calls.extend(record_call(
                    coordinator,
                    delivery,
                    name,
                    deliveries.into_inner(),
                    true,
                    false,
                ));
            }
            if failure.policies.panic == FailurePolicy::FailFast {
                failure
                    .shutdown
                    .fail(name, &format!("batch handler panicked: {reason}"));
            }
        }
    }
    // The hooks a test registered on this subscription run once the call is settled, inside the
    // dispatch's own in-flight count, so a publish returns only after they are done.
    #[cfg(feature = "testing")]
    if let Some(coordinator) = &watcher {
        coordinator.run_calls(due_calls).await;
        coordinator.consumed();
    }
}

/// The harness scope a delivery runs under, or `None` when no [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html)
/// is driving this app.
#[cfg(feature = "testing")]
fn harness_scope<C>(delivery: &Delivery<C>) -> Option<HarnessScope> {
    delivery
        .hooks
        .coordinator()
        .cloned()
        .map(|coordinator| HarnessScope::new(coordinator, delivery.scope_id, delivery.subscription))
}

mod delivery;
#[cfg(test)]
#[cfg(ruststream_alloc)]
mod fixtures;
#[cfg(all(test, feature = "testing"))]
mod harness_tests;
mod headers;
mod settle;
mod workers;
