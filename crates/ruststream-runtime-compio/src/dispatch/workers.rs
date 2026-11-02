//! `workers(n)` and `threads(n)`: a subscription's deliveries handled by lanes on other cores,
//! each fed through one ring of `ruststream-runtime` (their hand-off is described there).
//!
//! This half starts the lanes, one job each through the [`Fabric`], and runs the subscription's
//! loop on core 0, reading the stream through the rings' feed.

use std::fmt::Display;
use std::sync::Arc;

use bytes::BytesMut;
use compio::runtime::{self, JoinHandle};
use futures::Stream;
use futures::channel::oneshot;
use ruststream_core::{BuildContext, IncomingMessage, Subscriber};
use ruststream_runtime::DeliverySlot as Slot;
use ruststream_runtime::{
    DispatchFailure, Feed, Handler, HeadersOnDemand, RetryCopies, Workers, dispatch, open_rings,
    placed,
};
use tracing::error;

use super::{Fabric, Hosted, Shared, StartLaneError, spawn_dispatch};
use crate::{Stop, yield_now};

/// The lanes a loop started, each reporting when its work has ended.
pub(super) struct Crew(Vec<oneshot::Receiver<()>>);

impl Crew {
    pub(super) async fn join(&mut self) {
        for ended in &mut self.0 {
            if ended.await.is_err() {
                error!(target: "ruststream::dispatch", "a lane ended abnormally");
            }
        }
    }
}

/// Opens the rings of a subscription and starts a lane on each of `cores`, running what `each`
/// builds there. The loop's end of the rings and the crew come back for the caller to run.
///
/// Everything that can fail happens here, before the subscription's loop runs: a core that is
/// gone refuses the subscription.
pub(super) fn start_lanes<Item, Each, Handle>(
    fabric: &Fabric,
    subscription: &str,
    cores: &[usize],
    keyed: bool,
    each: Each,
) -> Result<(Feed<Item>, Crew), StartLaneError>
where
    Item: Send + 'static,
    Each: Fn() -> Handle + Send + Clone + 'static,
    Handle: AsyncFnMut(Item) + 'static,
{
    let mut crew = Vec::with_capacity(cores.len());
    let mut places = cores.iter().copied();
    let feed = open_rings(cores.len(), keyed, |lane| {
        let core = places.next().unwrap_or(0);
        let (ended, reported) = oneshot::channel();
        crew.push(reported);
        let each = each.clone();
        fabric.run_on(core, subscription, move || {
            runtime::spawn(async move {
                // Built on the core and never leaving it.
                let mut handle = each();
                lane.work(async move |item| {
                    handle(item).await;
                    // The core's turn for its continuations, hooks and timers.
                    yield_now().await;
                })
                .await;
                let _ = ended.send(());
            })
        })
    })?;
    Ok((feed, Crew(crew)))
}

/// Spawns a task that drives `subscriber` through `handler` with `workers.count()` lanes.
///
/// The lanes run over the shared cores for `workers(n)` and on cores of their own for
/// `threads(n)`, `cores` naming where each runs (see [`Layout`](super::Layout)). With `by_key`
/// the lane is picked by hashing the delivery's key on the loop's core.
///
/// A sequential policy is [`spawn_dispatch`]. On shutdown the stream stops being polled and the
/// lanes drain what their rings hold; dropping the returned handle drops the rings, and the lanes
/// end once they have drained them.
///
/// # Errors
///
/// [`StartLaneError`] when a core a lane was to start on has ended.
// The parts are independent and each spawn site passes its own; bundling them into a struct
// would hide that.
#[allow(clippy::too_many_arguments)]
pub fn spawn_dispatch_workers<S, H, C, St, Retry>(
    mut subscriber: S,
    handler: Arc<H>,
    stop: Stop,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C, Retry>>,
    failure: DispatchFailure<Stop>,
    workers: Workers,
    fabric: &Fabric,
    cores: &[usize],
) -> Result<JoinHandle<()>, StartLaneError>
where
    Retry: RetryCopies<C> + 'static,
    S: Subscriber + 'static,
    S::Message: IncomingMessage + Send + Sync + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    let workers = placed(workers, &hosted.delivery);
    if workers.is_sequential() {
        return Ok(spawn_dispatch(
            subscriber, handler, stop, name, state, hosted, failure,
        ));
    }
    let shared = Arc::new(Shared {
        handler,
        name,
        state,
        hosted,
        failure,
    });
    let lanes = Arc::clone(&shared);
    let (feed, crew) = start_lanes(
        fabric,
        &shared.name,
        &cores[..workers.count().get()],
        workers.by_key(),
        move || {
            let shared = Arc::clone(&lanes);
            // One encode buffer per lane, for the reason the sequential loop has one.
            let mut encode = BytesMut::new();
            async move |msg: S::Message| {
                let mut slot = Slot::new(msg);
                dispatch(
                    &shared.handler,
                    &mut slot,
                    &mut encode,
                    &shared.name,
                    &shared.state,
                    &shared.hosted.delivery,
                    &shared.hosted.host,
                    &shared.failure,
                    HeadersOnDemand,
                )
                .await;
            }
        },
    )?;
    Ok(runtime::spawn(async move {
        read_then_join(
            feed,
            crew,
            subscriber.stream(),
            &shared.name,
            &stop,
            <S::Message as IncomingMessage>::partition_key,
        )
        .await;
    }))
}

/// Lets go of the rings when the loop's task ends, however it ends, so a lane parked on an empty
/// ring is woken to find the ring abandoned: a loop dropped by its handle takes its lanes down
/// with it, once they have drained what they hold.
struct LetGo<Item>(Feed<Item>);

impl<Item> Drop for LetGo<Item> {
    fn drop(&mut self) {
        self.0.let_go();
    }
}

/// Reads `stream` into the rings until it ends or `stop` fires, then lets the lanes drain what
/// their rings hold and waits for them.
pub(super) async fn read_then_join<Item, Items, Failure>(
    feed: Feed<Item>,
    mut crew: Crew,
    stream: Items,
    name: &str,
    stop: &Stop,
    key: fn(&Item) -> Option<&[u8]>,
) where
    Items: Stream<Item = Result<Item, Failure>>,
    Failure: Display,
{
    let mut feed = LetGo(feed);
    feed.0.read(stream, name, stop, key).await;
    feed.0.let_go();
    crew.join().await;
}
