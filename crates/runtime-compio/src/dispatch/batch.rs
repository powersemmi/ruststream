//! Batch subscriptions: one `BatchSubscriber::batches` item per invocation on core 0, or whole
//! batches handed to workers on other cores.

use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use bytes::BytesMut;
use compio::runtime::JoinHandle;
use ruststream_core::diag::error;
use ruststream_core::{BatchSlot, BatchStream, BatchSubscriber};
use ruststream_runtime::{
    BatchHandler, DispatchFailure, RING, RetryCopies, TakenBatch, Turn, Workers, placed, run_batch,
};

use super::slots::{Filled, Stock};
use super::workers::{read_then_join, start_workers};
use super::{DispatchUnit, Fabric, Hosted, Shared, StartUnitError};
use crate::spawn::spawn;
use crate::{Stop, yield_now};

/// Spawns a task that drives `subscriber` through a batch `handler`, one batch per invocation.
///
/// It runs until `stop` is raised or the stream terminates. The handler owns the batch's
/// deliveries and settles each of them; the items are those of [`BatchSubscriber::batches`].
///
/// With a non-sequential `workers` policy the batches are handed to workers on `cores`, as whole
/// batches through the rings, each worker with a decode and an encode buffer of its own; keyed workers
/// do not apply at batch granularity, so a keyed policy spreads batches as the plain one does.
///
/// # Errors
///
/// [`StartUnitError`] when a core a worker was to start on has ended.
#[allow(clippy::too_many_arguments)] // See spawn_dispatch_workers.
pub fn spawn_batch_dispatch<S, H, C, St, Retry>(
    subscriber: S,
    handler: Arc<H>,
    stop: Stop,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C, Retry>>,
    failure: DispatchFailure<Stop>,
    workers: Workers,
    batch_size: NonZeroUsize,
    fabric: &Fabric,
    cores: &[usize],
) -> Result<JoinHandle<()>, StartUnitError>
where
    Retry: RetryCopies<C> + 'static,
    S: BatchSubscriber + 'static,
    S::Message: Send + 'static,
    S::Batch: Send + 'static,
    H: BatchHandler<S::Batch, C, St> + 'static,
    C: Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    let workers = placed(workers, &hosted.delivery);
    let shared = Arc::new(Shared {
        handler,
        name,
        state,
        hosted,
        failure,
    });
    if workers.is_sequential() {
        return Ok(spawn(async move {
            batch_loop(subscriber, &shared, &stop, batch_size).await;
        }));
    }
    let count = workers.count().get();
    // A slot per place a batch can wait: a full ring and a batch in hand for each worker, and the
    // one being filled.
    let most = count * (RING + 1) + 1;
    let stock = Stock::<S::Batch>::new(most);
    let for_workers = Arc::clone(&shared);
    let returns = Arc::clone(&stock);
    let (feed, crew) = start_workers(
        fabric,
        &shared.name,
        DispatchUnit::of(workers.placement()),
        &cores[..count],
        false,
        move || {
            let shared = Arc::clone(&for_workers);
            let stock = Arc::clone(&returns);
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
                    &shared.hosted.delivery,
                    &shared.hosted.host,
                    &shared.failure,
                )
                .await;
                stock.give(slot);
            }
        },
    )?;
    let mut subscriber = subscriber;
    Ok(spawn(async move {
        read_then_join(
            feed,
            crew,
            Filled::new(subscriber.batches(batch_size), stock, batch_size, most),
            &shared.name,
            &stop,
            |_| None,
        )
        .await;
    }))
}

/// The loop of a sequential batch subscription: one batch at a time, run on the loop's own task.
async fn batch_loop<S, H, C, St, Retry>(
    mut subscriber: S,
    shared: &Shared<H, St, C, Retry>,
    stop: &Stop,
    batch_size: NonZeroUsize,
) where
    Retry: RetryCopies<C>,
    S: BatchSubscriber,
    H: BatchHandler<S::Batch, C, St>,
    C: Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    use ruststream_runtime::{Halt as _, ShutdownWait as _};
    // The registration's own batch size, straight to the broker: whatever comes back is the
    // batch the handler sees.
    let mut stream = pin!(subscriber.batches(batch_size));
    let mut cancelled = pin!(stop.cancelled());
    // One slot, one taken batch and one decode buffer for the whole loop, and the encode buffer
    // beside them: allocated once for the subscription.
    let mut slot = <S::Batch as BatchSlot>::with_capacity(batch_size);
    let mut taken = <H as BatchHandler<S::Batch, C, St>>::taken(batch_size);
    let mut scratch = <H as BatchHandler<S::Batch, C, St>>::Scratch::default();
    let mut encode = BytesMut::new();
    loop {
        if stop.is_cancelled() {
            break;
        }
        let pulled = poll_fn(|cx| match stream.as_mut().poll_fill(cx, &mut slot) {
            Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
            Poll::Ready(None) => Poll::Ready(Turn::Ended),
            Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
        })
        .await;
        match pulled {
            Turn::Delivery(Ok(())) => {
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
                yield_now().await;
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
                // A broker that keeps failing delivers no batch, which is where the loop yields;
                // without this the core would never leave the loop.
                yield_now().await;
            }
            Turn::Ended => {
                stop.stream_ended(&shared.name);
                break;
            }
            Turn::Shutdown => break,
        }
    }
}
