//! The tokio half of the dispatch: the tasks a subscription's loop runs in, its worker pool and
//! its dedicated threads, all driving the shared delivery body of `ruststream-runtime`.

use std::future::poll_fn;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use bytes::BytesMut;
use futures::Stream;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tracing::error;

use ruststream_runtime::DeliverySlot as Slot;
use ruststream_runtime::{
    BatchHandler, DispatchFailure, Handler, Placement, Turn, Workers, dispatch, placed, run_batch,
    run_sequential,
};

pub(crate) use crate::detach::Hosted;
use crate::shutdown::Shutdown;
use crate::{BatchSubscriber, IncomingMessage, Subscriber};
pub(crate) use threads::StartThreadError;
use threads::Threads;

/// Spawns a task that drives `subscriber` through `handler` until `shutdown` is triggered or the
/// stream terminates: [`run_sequential`] on the app's runtime, counted as consuming while it runs.
pub(crate) fn spawn_dispatch<S, H, C, St>(
    subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C>>,
    failure: DispatchFailure<Shutdown>,
) -> JoinHandle<()>
where
    S: Subscriber + Send + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    let consuming = shutdown.consuming();
    tokio::spawn(run_sequential(
        subscriber, handler, shutdown, name, state, hosted, failure, consuming,
    ))
}

/// Waits for whichever comes first: a finished worker off `tasks`, or `shutdown`.
///
/// For a full pool, what a turn of the sequential loop is for a subscriber: in the same order and
/// over the same pinned token future, a worker that has already finished is reaped without the
/// token's waiter list being touched. `Ended` answers an empty set, which a full pool cannot be.
///
/// # Cancel safety
///
/// Cancel-safe, like a turn of the sequential loop: a `JoinSet` keeps its finished workers across
/// polls, and the registration a parked poll made with the waiter list outlives that poll.
pub(super) async fn reap<Wait, Done>(
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

/// Spawns a task that drives `subscriber` through `handler` with `workers.count()` long-lived
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
    hosted: Arc<Hosted<C>>,
    failure: DispatchFailure<Shutdown>,
    workers: Workers,
) -> Result<JoinHandle<()>, StartThreadError>
where
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    let workers = placed(workers, &hosted.delivery);
    if workers.is_sequential() {
        return Ok(spawn_dispatch(
            subscriber, handler, shutdown, name, state, hosted, failure,
        ));
    }
    if matches!(workers.placement(), Placement::Threads) {
        return spawn_dispatch_threads(
            subscriber, handler, shutdown, name, state, hosted, failure, workers,
        );
    }
    Ok(pool::spawn_dispatch_pool(
        subscriber, handler, shutdown, name, state, hosted, failure, workers,
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
    hosted: Arc<Hosted<C>>,
    failure: DispatchFailure<Shutdown>,
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
        hosted,
        failure,
    });
    let threads = Threads::start(
        &shared.name,
        workers.count().get(),
        workers.by_key(),
        |own| {
            let shared = Arc::clone(&shared);
            let delivery = shared.hosted.delivery.on_thread();
            let host = shared.hosted.host.on_thread(own);
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
                    &host,
                    &shared.failure,
                )
                .await;
            }
        },
    )?;
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

pub(super) fn lane_of(key: &[u8], lanes: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    // The modulo keeps the value below `lanes`, which is a usize.
    #[allow(clippy::cast_possible_truncation)]
    {
        (hasher.finish() % lanes as u64) as usize
    }
}

/// What a finished worker handed back, or nothing and a log line where it failed.
pub(super) fn log_worker_exit<Done>(joined: Result<Done, JoinError>) -> Option<Done> {
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
/// With a non-sequential `workers` policy, up to `workers.count()` batches are in flight at once,
/// each in its own task; keyed lanes do not apply at batch granularity (the macro rejects
/// `by_key` on batch forms), so a keyed policy degrades to the plain pool.
#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
pub(crate) fn spawn_batch_dispatch<S, H, C, St>(
    subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C>>,
    failure: DispatchFailure<Shutdown>,
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
    let workers = placed(workers, &hosted.delivery);
    if matches!(workers.placement(), Placement::Threads) {
        return spawn_batch_threads(
            subscriber, handler, shutdown, name, state, hosted, failure, workers, batch_size,
        );
    }
    let shared = pool::Shared {
        handler,
        name,
        state,
        hosted,
        failure,
    };
    let consuming = shutdown.consuming();
    Ok(tokio::spawn(async move {
        let _consuming = consuming;
        batch_loop(subscriber, &shared, &shutdown, workers, batch_size).await;
    }))
}

/// The loop of a batch subscription on the app's runtime: one batch at a time where the policy
/// is sequential, up to `workers.count()` at once on a pool of workers otherwise.
async fn batch_loop<S, H, C, St>(
    mut subscriber: S,
    shared: &pool::Shared<H, St, C>,
    shutdown: &Shutdown,
    workers: Workers,
    batch_size: NonZeroUsize,
) where
    S: BatchSubscriber + Send + 'static,
    S::Message: Send + 'static,
    S::Batch: 'static,
    H: BatchHandler<S::Batch, C, St> + 'static,
    C: Send + Sync + 'static,
    St: Send + Sync + 'static,
{
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
        if tasks.len() >= workers.count().get() {
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
            Turn::Delivery(Ok(batch)) if workers.is_sequential() => {
                // Turbofish: the adapter handlers are generic over the batch context, so the
                // spawn's own parameter names it.
                run_batch::<_, _, C, _, _, _>(
                    &*shared.handler,
                    batch,
                    &mut scratch,
                    &mut encode,
                    &shared.name,
                    &shared.state,
                    &shared.hosted.delivery,
                    &shared.hosted.host,
                    &shared.failure,
                )
                .await;
            }
            Turn::Delivery(Ok(batch)) => {
                let buffers = spare.pop().unwrap_or_default();
                tasks.spawn(pooled_batch::<_, _, C, _>(shared, batch, buffers));
            }
            Turn::Delivery(Err(err)) => {
                error!(
                    target: "ruststream::dispatch",
                    error = %err,
                    "subscriber stream error",
                );
            }
            Turn::Ended => {
                shutdown.stream_ended(&shared.name);
                break;
            }
            Turn::Shutdown => break,
        }
    }
    while let Some(joined) = tasks.join_next().await {
        log_worker_exit(joined);
    }
}

/// One batch of a pooled subscription, run on a worker of its own with the buffers it was lent,
/// which it hands back for the next batch.
fn pooled_batch<H, Batch, C, St>(
    shared: &pool::Shared<H, St, C>,
    batch: Batch,
    (mut scratch, mut encode): (H::Scratch, BytesMut),
) -> impl Future<Output = (H::Scratch, BytesMut)> + use<H, Batch, C, St>
where
    H: BatchHandler<Batch, C, St>,
    C: Send + Sync + 'static,
    St: Send + Sync,
{
    let handler = Arc::clone(&shared.handler);
    let name = Arc::clone(&shared.name);
    let state = Arc::clone(&shared.state);
    let hosted = Arc::clone(&shared.hosted);
    let failure = shared.failure.clone();
    async move {
        run_batch::<_, _, C, _, _, _>(
            &*handler,
            batch,
            &mut scratch,
            &mut encode,
            &name,
            &state,
            &hosted.delivery,
            &hosted.host,
            &failure,
        )
        .await;
        (scratch, encode)
    }
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
    hosted: Arc<Hosted<C>>,
    failure: DispatchFailure<Shutdown>,
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
        hosted,
        failure,
    });
    let threads = Threads::start(&shared.name, workers.count().get(), false, |own| {
        let shared = Arc::clone(&shared);
        let delivery = shared.hosted.delivery.on_thread();
        let host = shared.hosted.host.on_thread(own);
        let mut scratch = <H as BatchHandler<S::Batch, C, St>>::Scratch::default();
        let mut encode = BytesMut::new();
        async move |batch: S::Batch| {
            run_batch::<_, _, C, _, _, _>(
                &*shared.handler,
                batch,
                &mut scratch,
                &mut encode,
                &shared.name,
                &shared.state,
                &delivery,
                &host,
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

#[cfg(test)]
mod fixtures;
mod pool;
#[cfg(test)]
mod tests;
mod threads;
