//! The tokio half of the dispatch: the tasks a subscription's loop runs in, its worker pool and
//! its dedicated threads, all driving the shared delivery body of `ruststream-runtime`.

use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Poll;

use bytes::BytesMut;
use ruststream_core::diag::error;
use tokio::task::JoinHandle;

use ruststream_core::{BatchSlot, BatchStream};
use ruststream_runtime::DeliverySlot as Slot;
use ruststream_runtime::{
    BatchHandler, DispatchFailure, Handler, HeadersOnDemand, Placement, RING, RetryCopies,
    TakenBatch, Turn, Workers, dispatch, key_index, placed, run_batch, run_sequential,
};

pub(crate) use crate::detach::{Hosted, on_thread};
use crate::follow::spawn_followed;
use crate::shutdown::Shutdown;
use crate::{BatchSubscriber, IncomingMessage, Subscriber};
pub(crate) use threads::StartThreadError;
use threads::Threads;
use watch::{Part, Watch};

/// Spawns a task that drives `subscriber` through `handler` until `shutdown` is triggered or the
/// stream terminates: [`run_sequential`] on the app's runtime, counted as consuming while it runs
/// and failing the service if it panics outside the handler.
pub(crate) fn spawn_dispatch<S, H, C, St, Retry>(
    subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C, Retry>>,
    failure: DispatchFailure<Shutdown>,
) -> JoinHandle<()>
where
    Retry: RetryCopies<C>,
    S: Subscriber + Send + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: crate::BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    let guards = (
        Watch::new(&shutdown, &name, Part::Loop),
        shutdown.consuming(),
    );
    spawn_followed(run_sequential(
        subscriber,
        handler,
        shutdown,
        name,
        state,
        hosted,
        failure,
        guards,
        HeadersOnDemand,
        (),
        BytesMut::new(),
    ))
}

/// Spawns a task that drives `subscriber` through `handler` with `workers.count()` long-lived
/// workers: a pool that hands each delivery to a free worker, or with `by_key` workers that hand it
/// to the worker its key hashes to (see [`pool`]).
///
/// Sequential policies delegate to [`spawn_dispatch`]. On shutdown the stream stops being
/// polled and the workers drain what they hold; if the app's `shutdown_timeout` aborts this task,
/// the workers abort with it.
// The parts are independent and each spawn site passes its own; bundling them into a struct
// would hide that.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_dispatch_workers<S, H, C, St, Retry>(
    subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C, Retry>>,
    failure: DispatchFailure<Shutdown>,
    workers: Workers,
) -> Result<JoinHandle<()>, StartThreadError>
where
    Retry: RetryCopies<C>,
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
fn spawn_dispatch_threads<S, H, C, St, Retry>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C, Retry>>,
    failure: DispatchFailure<Shutdown>,
    workers: Workers,
) -> Result<JoinHandle<()>, StartThreadError>
where
    Retry: RetryCopies<C>,
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
        &shutdown,
        |own| {
            let shared = Arc::clone(&shared);
            let delivery = shared.hosted.delivery.on_thread();
            let host = on_thread(&shared.hosted.host, own);
            // One encode buffer per thread, for the reason the sequential loop has one.
            let mut encode = BytesMut::new();
            async move |msg: S::Message| {
                let mut slot = Slot::new(msg);
                dispatch(
                    &shared.handler,
                    &mut slot,
                    &mut encode,
                    &shared.name,
                    &shared.state,
                    &delivery,
                    &host,
                    &shared.failure,
                    HeadersOnDemand,
                )
                .await;
            }
        },
    )?;
    let guards = (
        Watch::new(&shutdown, &shared.name, Part::Loop),
        shutdown.consuming(),
    );
    Ok(spawn_followed(async move {
        let _guards = guards;
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

/// Spawns a task that drives `subscriber` through a batch `handler`, one
/// [`BatchSubscriber::batches`] item per invocation, until `shutdown` is triggered or the stream
/// terminates. The handler owns the batch's deliveries and settles each of them.
///
/// With a non-sequential `workers` policy, up to `workers.count()` batches are in flight at once,
/// each on a worker started with the subscription (see [`batch_pool`]); keyed workers do not apply at batch granularity (the macro rejects
/// `by_key` on batch forms), so a keyed policy degrades to the plain pool.
#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
pub(crate) fn spawn_batch_dispatch<S, H, C, St, Retry>(
    subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C, Retry>>,
    failure: DispatchFailure<Shutdown>,
    workers: Workers,
    batch_size: NonZeroUsize,
) -> Result<JoinHandle<()>, StartThreadError>
where
    Retry: RetryCopies<C>,
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
    let guards = (
        Watch::new(&shutdown, &shared.name, Part::Loop),
        shutdown.consuming(),
    );
    Ok(spawn_followed(async move {
        let _guards = guards;
        if workers.is_sequential() {
            batch_loop(subscriber, &shared, &shutdown, batch_size).await;
        } else {
            batch_pool::run(subscriber, Arc::new(shared), &shutdown, workers, batch_size).await;
        }
    }))
}

/// The loop of a sequential batch subscription: one batch at a time, run on the loop's own task
/// (a pool of workers is [`batch_pool`]).
async fn batch_loop<S, H, C, St, Retry>(
    mut subscriber: S,
    shared: &pool::Shared<H, St, C, Retry>,
    shutdown: &Shutdown,
    batch_size: NonZeroUsize,
) where
    Retry: RetryCopies<C>,
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
    // One slot, one taken batch and one decode buffer for the whole loop: every batch borrows the
    // same ones, so what a batch is collected and read in is allocated once for the subscription.
    // The encode buffer beside them is the same bargain on the way out.
    let mut slot = <S::Batch as BatchSlot>::with_capacity(batch_size);
    let mut taken = <H as BatchHandler<S::Batch, C, St>>::taken(batch_size);
    let mut scratch = <H as BatchHandler<S::Batch, C, St>>::Scratch::default();
    let mut encode = BytesMut::new();
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        // The stream first, the token only where the stream has nothing: see `turn_into`, whose
        // wait this is.
        let pulled = poll_fn(|cx| match stream.as_mut().poll_fill(cx, &mut slot) {
            Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
            Poll::Ready(None) => Poll::Ready(Turn::Ended),
            Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        })
        .await;
        match pulled {
            Turn::Delivery(Ok(())) => {
                // Turbofish: the adapter handlers are generic over the batch context, so the
                // spawn's own parameter names it.
                run_batch::<_, _, C, _, _, _>(
                    &shared.handler,
                    &mut slot,
                    &mut taken,
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
            Turn::Delivery(Err(err)) => {
                // A failed fill leaves the slot empty; taking what a broker left behind and
                // dropping it keeps that true.
                <H as BatchHandler<S::Batch, C, St>>::load(&mut taken, &mut slot);
                taken.discard();
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
}

/// The batch counterpart of [`spawn_dispatch_threads`]: each thread takes whole batches off its
/// ring, with a decode and an encode buffer of its own. Keyed workers do not apply to batches, so a
/// keyed policy spreads batches as the plain one does.
#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
fn spawn_batch_threads<S, H, C, St, Retry>(
    mut subscriber: S,
    handler: Arc<H>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C, Retry>>,
    failure: DispatchFailure<Shutdown>,
    workers: Workers,
    batch_size: NonZeroUsize,
) -> Result<JoinHandle<()>, StartThreadError>
where
    Retry: RetryCopies<C>,
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
    let count = workers.count().get();
    // A slot per place a batch can wait: a full ring and a batch in hand for each thread, and the
    // one being filled.
    let most = count * (RING + 1) + 1;
    let stock = slots::Stock::<S::Batch>::new(most);
    let threads = Threads::start(&shared.name, count, false, &shutdown, |own| {
        let shared = Arc::clone(&shared);
        let stock = Arc::clone(&stock);
        let delivery = shared.hosted.delivery.on_thread();
        let host = on_thread(&shared.hosted.host, own);
        let mut taken = <H as BatchHandler<S::Batch, C, St>>::taken(batch_size);
        let mut scratch = <H as BatchHandler<S::Batch, C, St>>::Scratch::default();
        let mut encode = BytesMut::new();
        async move |mut slot: S::Batch| {
            run_batch::<_, _, C, _, _, _>(
                &shared.handler,
                &mut slot,
                &mut taken,
                &mut scratch,
                &mut encode,
                &shared.name,
                &shared.state,
                &delivery,
                &host,
                &shared.failure,
            )
            .await;
            stock.give(slot);
        }
    })?;
    let guards = (
        Watch::new(&shutdown, &shared.name, Part::Loop),
        shutdown.consuming(),
    );
    Ok(spawn_followed(async move {
        let _guards = guards;
        threads
            .feed(
                slots::Filled::new(subscriber.batches(batch_size), stock, batch_size, most),
                &shared.name,
                &shutdown,
                |_| None,
            )
            .await;
    }))
}

mod batch_pool;
#[cfg(test)]
mod fixtures;
mod pool;
mod slots;
#[cfg(test)]
mod tests;
mod threads;
mod watch;
