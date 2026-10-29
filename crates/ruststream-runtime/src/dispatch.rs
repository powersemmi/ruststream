//! The per-subscriber dispatch loop: pulls messages off one subscriber and invokes its handler
//! until shutdown is signalled or the stream ends. [`RustStream`](super::RustStream) owns the
//! task spawning.

use std::future::poll_fn;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

#[cfg(feature = "testing")]
use bytes::Bytes;
use bytes::BytesMut;
use futures::{FutureExt, Stream};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tracing::error;

use crate::{BatchSubscriber, HeaderMap, IncomingMessage, Subscriber};

use super::batch::{BatchHandler, TakenBatch};
use super::context::{Context, FromDelivery};
use super::failure::{DispatchFailure, FailurePolicy, panic_reason};
use super::handler::Handler;
#[cfg(doc)]
use super::handler::HandlerResult;
use super::shutdown::Shutdown;
#[cfg(feature = "testing")]
use crate::testing::coordinator::{
    Delivered, HarnessScope, Record, calls_in_harness_scope, in_harness_scope,
};
pub(crate) use delivery::{Delivery, Slot};
pub(crate) use settle::settle_outcome;
pub(crate) use threads::StartThreadError;
use threads::Threads;
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

/// Spawns a task that drives `subscriber` through `handler` until `shutdown` is triggered or the
/// stream terminates. Each delivery is given a [`Context`] built from `name`, the message headers,
/// shared `state`, and the `delivery` publish context.
pub(crate) fn spawn_dispatch<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    let consuming = shutdown.consuming();
    tokio::spawn(async move {
        let _consuming = consuming;
        let mut stream = std::pin::pin!(subscriber.stream());
        let mut cancelled = std::pin::pin!(shutdown.cancelled());
        // One encode buffer for the whole loop: a reply that leaves through a publisher which
        // reads the payload is written into this one every time, so the subscription allocates
        // it once and never again.
        let mut encode = BytesMut::new();
        // The loop's slot, one for the whole loop: the stream's poll writes each delivery
        // straight into it, and the settle takes it out again. A delivery returned by value
        // through the turn and bound to a local on its way here would be copied twice more.
        let mut slot = Slot::empty();
        loop {
            match turn_into(&shutdown, stream.as_mut(), cancelled.as_mut(), &mut slot).await {
                Turn::Delivery(Ok(())) => {
                    // Pinned rather than awaited in place: a delivery's future carries the
                    // context, the handler's own state and the settle path, and awaiting the
                    // call expression makes the loop build it on the stack and copy it into
                    // its own state on every delivery.
                    let handling = std::pin::pin!(dispatch(
                        &*handler,
                        &mut slot,
                        &mut encode,
                        &name,
                        &state,
                        &delivery,
                        &failure
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
    })
}

/// Waits for whichever comes first: the next item off `stream`, or `shutdown`. A delivery is
/// written into the loop's slot where it lands rather than returned by value.
///
/// The stream is polled first and `cancelled` only where it answers `Pending`, so a turn that
/// finds a delivery ready never touches the token's waiter list: what shutdown costs a busy
/// subscription is the one relaxed load of [`Shutdown`]'s flag. `cancelled` is the token's own
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
/// The other loops write the same wait out rather than share it: one instantiation shared with
/// them is enough for the compiler to stop inlining it here.
fn turn_into<'s, St, Message, Failure, Wait>(
    shutdown: &Shutdown,
    stream: Pin<&'s mut St>,
    cancelled: Pin<&'s mut Wait>,
    slot: &'s mut Slot<Message>,
) -> TurnInto<'s, St, Message, Wait>
where
    St: Stream<Item = Result<Message, Failure>>,
    Wait: Future<Output = ()>,
{
    TurnInto {
        shutdown: shutdown.is_cancelled(),
        stream,
        cancelled,
        slot,
    }
}

/// The future [`turn_into`] returns: written out rather than an `async fn` so its poll carries an
/// inline hint, which a closure inside `poll_fn` cannot.
struct TurnInto<'s, St, Message, Wait> {
    shutdown: bool,
    stream: Pin<&'s mut St>,
    cancelled: Pin<&'s mut Wait>,
    slot: &'s mut Slot<Message>,
}

impl<St, Message, Failure, Wait> Future for TurnInto<'_, St, Message, Wait>
where
    St: Stream<Item = Result<Message, Failure>>,
    Wait: Future<Output = ()>,
{
    type Output = Turn<Result<(), Failure>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // Every field is a reference or a flag, so the future is `Unpin`.
        let this = self.get_mut();
        if this.shutdown {
            return Poll::Ready(Turn::Shutdown);
        }
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                this.slot.fill(msg);
                Poll::Ready(Turn::Delivery(Ok(())))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Turn::Delivery(Err(err))),
            Poll::Ready(None) => Poll::Ready(Turn::Ended),
            Poll::Pending => this.cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        }
    }
}

/// What one turn of a dispatch loop found on its subscriber.
enum Turn<T> {
    /// The subscriber yielded a delivery, or an error reading one.
    Delivery(T),
    /// The subscriber's stream ended; the loop is done.
    Ended,
    /// Shutdown was signalled; the loop stops without touching the stream again.
    Shutdown,
}

/// Waits for whichever comes first: a finished worker off `tasks`, or `shutdown`.
///
/// [`turn_into`]'s counterpart for a full pool, in the same order and over the same pinned token
/// future: a worker that has already finished is reaped without the token's waiter list being
/// touched. `Ended` answers an empty set, which a full pool cannot be.
///
/// # Cancel safety
///
/// Cancel-safe, like [`turn_into`]: a `JoinSet` keeps its finished workers across polls, and the
/// registration a parked poll made with the waiter list outlives that poll.
async fn reap<Wait, Done>(
    tasks: &mut JoinSet<Done>,
    mut cancelled: Pin<&mut Wait>,
) -> Turn<Result<Done, JoinError>>
where
    Wait: Future<Output = ()>,
    Done: 'static,
{
    poll_fn(|cx| match tasks.poll_join_next(cx) {
        Poll::Ready(Some(joined)) => Poll::Ready(Turn::Delivery(joined)),
        Poll::Ready(None) => Poll::Ready(Turn::Ended),
        Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
    })
    .await
}

/// Spawns a task that drives `subscriber` through `handler` with `workers.count` long-lived
/// workers: a pool that hands each delivery to a free worker, or with `by_key` lanes that hand it
/// to the worker its key hashes to (see [`pool`]).
///
/// Sequential policies delegate to [`spawn_dispatch`]. On shutdown the stream stops being
/// polled and the workers drain what they hold; if the app's `shutdown_timeout` aborts this task,
/// the workers abort with it.
// The parts are independent and each spawn site passes its own; bundling them into a struct
// would hide that.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_dispatch_workers<S, H, C, St>(
    subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
    workers: Workers,
) -> Result<JoinHandle<()>, StartThreadError>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    // Before the harness maps the placement away: the declaration is what the warning advises.
    #[cfg(feature = "poll-diagnostics")]
    if matches!(workers.placement, Placement::Threads) {
        delivery.poll.on_dedicated_threads();
    }
    // The harness drives every subscription on the test's own runtime, so a timer the handler
    // arms is one `advance` reaches; it does not reproduce the thread topology.
    #[cfg(feature = "testing")]
    let workers = if delivery.hooks.coordinator().is_some() {
        workers.on_runtime()
    } else {
        workers
    };
    if workers.is_sequential() {
        return Ok(spawn_dispatch(
            subscriber, handler, shutdown, name, state, delivery, failure,
        ));
    }
    if matches!(workers.placement, Placement::Threads) {
        return spawn_dispatch_threads(
            subscriber, handler, shutdown, name, state, delivery, failure, workers,
        );
    }
    Ok(pool::spawn_dispatch_pool(
        subscriber, handler, shutdown, name, state, delivery, failure, workers,
    ))
}

/// Starts the dedicated threads of a `threads(n)` subscription and spawns its loop on the app's
/// runtime (see [`threads`]).
// See `spawn_dispatch_workers`: each part is the registration's own.
#[allow(clippy::too_many_arguments)]
fn spawn_dispatch_threads<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
    workers: Workers,
) -> Result<JoinHandle<()>, StartThreadError>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    let shared = Arc::new(pool::Shared {
        handler,
        name,
        state,
        delivery,
        failure,
    });
    let threads = Threads::start(&shared.name, workers.count.get(), workers.by_key, |own| {
        let shared = Arc::clone(&shared);
        let delivery = shared.delivery.on_thread(own);
        // One encode buffer per thread, for the reason the sequential loop has one.
        let mut encode = BytesMut::new();
        async move |msg: S::Message| {
            let mut slot = Slot::new(msg);
            dispatch(
                &*shared.handler,
                &mut slot,
                &mut encode,
                &shared.name,
                &shared.state,
                &delivery,
                &shared.failure,
            )
            .await;
        }
    })?;
    let consuming = shutdown.consuming();
    Ok(tokio::spawn(async move {
        let _consuming = consuming;
        threads
            .feed(
                subscriber.stream(),
                &shared.name,
                &shutdown,
                <S::Message as IncomingMessage>::partition_key,
            )
            .await;
    }))
}

fn lane_of(key: &[u8], lanes: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    // The modulo keeps the value below `lanes`, which is a usize.
    #[allow(clippy::cast_possible_truncation)]
    {
        (hasher.finish() % lanes as u64) as usize
    }
}

/// What a finished worker handed back, or nothing and a log line where it failed.
fn log_worker_exit<Done>(joined: Result<Done, JoinError>) -> Option<Done> {
    match joined {
        Ok(done) => Some(done),
        Err(err) => {
            error!(target: "ruststream::dispatch", error = %err, "worker task failed");
            None
        }
    }
}

/// Spawns a task that drives `subscriber` through a batch `handler`, one
/// [`BatchSubscriber::batches`] item per invocation, until `shutdown` is triggered or the stream
/// terminates. The handler owns the batch's deliveries and settles each of them.
///
/// With a non-sequential `workers` policy, up to `workers.count` batches are in flight at once,
/// each in its own task; keyed lanes do not apply at batch granularity (the macro rejects
/// `by_key` on batch forms), so a keyed policy degrades to the plain pool.
#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
pub(crate) fn spawn_batch_dispatch<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
    workers: Workers,
    batch_size: NonZeroUsize,
) -> Result<JoinHandle<()>, StartThreadError>
where
    S: BatchSubscriber + Send + 'static,
    S::Message: Send + 'static,
    S::Batch: 'static,
    H: BatchHandler<S::Batch, C, St> + 'static,
    C: Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    // See `spawn_dispatch_workers`: the declaration is what the warning advises, and the harness
    // runs every subscription on the test's runtime.
    #[cfg(feature = "poll-diagnostics")]
    if matches!(workers.placement, Placement::Threads) {
        delivery.poll.on_dedicated_threads();
    }
    #[cfg(feature = "testing")]
    let workers = if delivery.hooks.coordinator().is_some() {
        workers.on_runtime()
    } else {
        workers
    };
    if matches!(workers.placement, Placement::Threads) {
        return spawn_batch_threads(
            subscriber, handler, shutdown, name, state, delivery, failure, workers, batch_size,
        );
    }
    let consuming = shutdown.consuming();
    Ok(tokio::spawn(async move {
        let _consuming = consuming;
        // The registration's own batch size, straight to the broker: whatever comes back is the
        // batch the handler sees.
        let mut stream = std::pin::pin!(subscriber.batches(batch_size));
        let mut cancelled = std::pin::pin!(shutdown.cancelled());
        let mut tasks = JoinSet::new();
        // One decode buffer for the whole loop: the sequential path lends the same one to every
        // batch, so the slice a handler reads is allocated once for the subscription. The encode
        // buffer beside it is the same bargain on the way out. A pooled batch runs beside the
        // others and cannot share these, so it takes a pair of its own from `spare`, where the
        // pool keeps what finished workers handed back.
        let mut scratch = <H as BatchHandler<S::Batch, C, St>>::Scratch::default();
        let mut encode = BytesMut::new();
        let mut spare = Vec::new();
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            if tasks.len() >= workers.count.get() {
                // The pool is full: reap a finished worker before polling for more.
                match reap(&mut tasks, cancelled.as_mut()).await {
                    Turn::Delivery(joined) => spare.extend(log_worker_exit(joined)),
                    // A full pool holds a worker, so `Ended` is shutdown's twin here: either
                    // way the drain below joins whatever is still running.
                    Turn::Ended | Turn::Shutdown => break,
                }
                continue;
            }
            // The stream first, the token only where the stream has nothing: see `turn_into`, whose
            // wait this is.
            let pulled = poll_fn(|cx| match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
                Poll::Ready(None) => Poll::Ready(Turn::Ended),
                Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
            })
            .await;
            match pulled {
                Turn::Delivery(Ok(batch)) => {
                    if workers.is_sequential() {
                        // Turbofish: the adapter handlers are generic over the batch context,
                        // so the spawn's own parameter names it.
                        run_batch::<_, _, C, _>(
                            &*handler,
                            batch,
                            &mut scratch,
                            &mut encode,
                            &name,
                            &state,
                            &delivery,
                            &failure,
                        )
                        .await;
                    } else {
                        let handler = Arc::clone(&handler);
                        let name = Arc::clone(&name);
                        let state = Arc::clone(&state);
                        let delivery = Arc::clone(&delivery);
                        let failure = failure.clone();
                        let (mut scratch, mut encode) = spare.pop().unwrap_or_default();
                        tasks.spawn(async move {
                            run_batch::<_, _, C, _>(
                                &*handler,
                                batch,
                                &mut scratch,
                                &mut encode,
                                &name,
                                &state,
                                &delivery,
                                &failure,
                            )
                            .await;
                            (scratch, encode)
                        });
                    }
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
        while let Some(joined) = tasks.join_next().await {
            log_worker_exit(joined);
        }
    }))
}

/// The batch counterpart of [`spawn_dispatch_threads`]: each thread takes whole batches off its
/// ring, with a decode and an encode buffer of its own. Keyed lanes do not apply to batches, so a
/// keyed policy spreads batches as the plain one does.
#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
fn spawn_batch_threads<S, H, C, St>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    delivery: Arc<Delivery<C>>,
    failure: DispatchFailure,
    workers: Workers,
    batch_size: NonZeroUsize,
) -> Result<JoinHandle<()>, StartThreadError>
where
    S: BatchSubscriber + Send + 'static,
    S::Message: Send + 'static,
    S::Batch: 'static,
    H: BatchHandler<S::Batch, C, St> + 'static,
    C: Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    let shared = Arc::new(pool::Shared {
        handler,
        name,
        state,
        delivery,
        failure,
    });
    let threads = Threads::start(&shared.name, workers.count.get(), false, |own| {
        let shared = Arc::clone(&shared);
        let delivery = shared.delivery.on_thread(own);
        let mut scratch = <H as BatchHandler<S::Batch, C, St>>::Scratch::default();
        let mut encode = BytesMut::new();
        async move |batch: S::Batch| {
            run_batch::<_, _, C, _>(
                &*shared.handler,
                batch,
                &mut scratch,
                &mut encode,
                &shared.name,
                &shared.state,
                &delivery,
                &shared.failure,
            )
            .await;
        }
    })?;
    let consuming = shutdown.consuming();
    Ok(tokio::spawn(async move {
        let _consuming = consuming;
        threads
            .feed(
                subscriber.batches(batch_size),
                &shared.name,
                &shutdown,
                |_| None,
            )
            .await;
    }))
}

async fn dispatch<H, M, C, St>(
    handler: &H,
    slot: &mut Slot<M>,
    encode: &mut BytesMut,
    name: &str,
    state: &St,
    delivery: &Delivery<C>,
    failure: &DispatchFailure,
) where
    H: Handler<M, C, St>,
    C: crate::BuildContext<M> + Send + Sync + 'static,
    M: IncomingMessage,
    // The dispatch future is awaited inside a spawned task, so it must be `Send`: the context
    // borrows `&St` across the handler await, which requires `St: Sync`.
    St: Send + Sync,
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
    let (settle, continuations) = {
        let msg = slot.get();
        let cx = C::build(msg);
        // The delivery itself, not its header map: a handler that reads no headers never reaches
        // the broker's accessor, which is a parse and a buffer on most transports.
        let source = FromDelivery(msg);
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
        #[cfg(not(feature = "testing"))]
        let result = observed!(
            delivery,
            AssertUnwindSafe(async { handler.handle(msg, &mut ctx).await }).catch_unwind()
        )
        .await;
        #[cfg(feature = "testing")]
        let panicked = result.is_err();
        // Resolve into a `HandlerOutcome` regardless of whether the handler panicked. `None`
        // means a fail-fast panic tore the service down and left the message unsettled (a broker
        // with redelivery hands it back after the restart).
        let settle = match result {
            Ok(s) => Some(s),
            Err(payload) => {
                let reason = panic_reason(payload.as_ref());
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
                            .signal(name, &format!("handler panicked: {reason}"));
                        None
                    }
                    other => Some(
                        other
                            .settlement()
                            .map_or_else(super::handler::HandlerOutcome::drop, Into::into),
                    ),
                }
            }
        };
        // Drain the matching post-settle hooks BEFORE settling: `ctx` borrows `msg`'s headers, and
        // settling consumes `msg`. The drained futures own their captures. A fail-fast (no
        // settlement) runs no hooks. Most deliveries register none, and those pay the branch alone:
        // the list, its scan and the drop glue of both belong to the deliveries that did register
        // one.
        let continuations = match settle.as_ref() {
            Some(s) if ctx.has_hooks() => Some(ctx.take_hooks_for(s.outcome())),
            _ => None,
        };
        // The harness records what the handler saw and how it settled, BEFORE settling the
        // message: the matching decrement runs in the broker message's `Drop` (during
        // `settle_outcome`, or at the end of this function on the fail-fast path), so the record
        // is in place by the time `drive` wakes. Captured here because `settle_outcome` consumes
        // `msg` and dropping `ctx` clears the decode flag.
        #[cfg(feature = "testing")]
        if let Some(coordinator) = delivery.hooks.coordinator() {
            due_calls = coordinator.record(Record {
                scope_id: delivery.scope_id,
                subscription: delivery.subscription,
                name: name.to_owned(),
                deliveries: vec![Delivered {
                    raw: Bytes::copy_from_slice(msg.payload()),
                    value: ctx.take_carried(),
                    settle: settle.as_ref().map(super::handler::HandlerOutcome::outcome),
                }],
                panicked,
                decode_failed: ctx.took_decode_failed(),
            });
        }
        (settle, continuations)
    };
    if let Some(mut s) = settle {
        // Named for the same reason as the delivery's own future above: the settle path is
        // built where it is polled instead of being copied into this future's state.
        let settling = settle_outcome(slot, s.outcome(), name, delivery, C::build);
        settling.await;
        // Spawn the `and_after` continuation (if any) onto the tracked set so a graceful shutdown
        // drains it. At-most-once: the message is already settled, so a lost or panicking
        // continuation never redelivers it.
        if let Some(after) = s.take_after() {
            delivery.spawn_after(after);
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
            delivery.spawn_after(fut);
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

/// Runs one batch through its handler under panic protection. The handler owns and settles the
/// batch's deliveries, so a panic there has already consumed them: the panic policy can only tear
/// the service down (`fail_fast`) or be logged and skipped. Per-element settlement is out of scope
/// (see the batch decode path for per-element decode handling). Ungated `after_settle` hooks run
/// once the batch has settled (per-element outcomes make a gated hook ill-defined on a batch).
// The loop hands this everything one batch needs, the buffers it lends included: see
// spawn_dispatch_workers.
#[allow(clippy::too_many_arguments)]
async fn run_batch<H, Batch, C, St>(
    handler: &H,
    batch: Batch,
    scratch: &mut H::Scratch,
    encode: &mut BytesMut,
    name: &str,
    state: &St,
    delivery: &Delivery<C>,
    failure: &DispatchFailure,
) where
    H: BatchHandler<Batch, C, St>,
    C: Send + Sync + 'static,
    St: Send + Sync,
{
    let batch = H::take(batch);
    // A batch spans many deliveries, so its context carries only subscription-scoped data, built
    // from its first delivery, or from the batch itself where the handler lends its values; the
    // shared app state is threaded the same way as on the single-message path. A batch with
    // nothing in it has nothing to settle and nothing to build a context from.
    let Some(cx) = batch.context() else { return };
    let empty = HeaderMap::new();
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
    let mut batch = batch;
    #[cfg(feature = "testing")]
    let unsettled = watcher.as_ref().map(|_| batch.unsettled());
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
            AssertUnwindSafe(async { handler.handle_batch(batch, scratch, &mut ctx).await })
                .catch_unwind()
        ),
    )
    .await;
    #[cfg(not(feature = "testing"))]
    let result = observed!(
        delivery,
        AssertUnwindSafe(async { handler.handle_batch(batch, scratch, &mut ctx).await })
            .catch_unwind()
    )
    .await;
    match result {
        Ok(()) => {
            // As on the single-message path: a batch that registered no hook pays the branch.
            if ctx.has_hooks() {
                for fut in ctx.take_settle_hooks() {
                    delivery.spawn_after(fut);
                }
            }
        }
        Err(payload) => {
            let reason = panic_reason(payload.as_ref());
            error!(
                target: "ruststream::dispatch",
                subscription = %name,
                panic = %reason,
                "batch handler panicked",
            );
            #[cfg(feature = "testing")]
            if let (Some(coordinator), Some(deliveries)) = (&watcher, unsettled) {
                due_calls.extend(coordinator.record(Record {
                    scope_id: delivery.scope_id,
                    subscription: delivery.subscription,
                    name: name.to_owned(),
                    deliveries,
                    panicked: true,
                    decode_failed: false,
                }));
            }
            if failure.policies.panic == FailurePolicy::FailFast {
                failure
                    .shutdown
                    .signal(name, &format!("batch handler panicked: {reason}"));
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
mod fixtures;
#[cfg(all(test, feature = "testing"))]
mod harness_tests;
mod pool;
mod settle;
#[cfg(test)]
mod tests;
mod threads;
mod workers;
