//! `workers(n)` and `threads(n)`: a subscription's deliveries handled by workers on other cores,
//! each fed through one ring of `ruststream-runtime` (their hand-off is described there).
//!
//! This half starts the workers, one job each through the [`Fabric`], and runs the subscription's
//! loop on core 0, reading the stream through the rings' feed.

use std::any::type_name;
use std::fmt::Display;
use std::sync::Arc;

use bytes::BytesMut;
use compio::runtime::JoinHandle;
use futures::Stream;
use futures::channel::oneshot;
use ruststream_core::diag::error;
use ruststream_core::{BuildContext, IncomingMessage, Places, Subscriber};
use ruststream_runtime::DeliverySlot as Slot;
use ruststream_runtime::{
    DispatchFailure, Feed, Handler, HeadersOnDemand, RetryCopies, Workers, dispatch, open_rings,
    placed,
};

use super::{Budget, DispatchUnit, Fabric, Hosted, Shared, StartUnitError, spawn_dispatch};
use crate::spawn::spawn;
use crate::{Stop, yield_now};

/// The workers a loop started, each reporting when its work has ended.
pub(super) struct Crew {
    ended: Vec<oneshot::Receiver<()>>,
    unit: DispatchUnit,
}

impl Crew {
    pub(super) async fn join(&mut self, subscription: &str, message: &str) {
        let word = self.unit.word();
        for ended in &mut self.ended {
            if ended.await.is_err() {
                error!(target: "ruststream::dispatch", subscription, message, "a {word} ended abnormally");
            }
        }
    }
}

/// Opens the rings of a subscription and starts a worker on each of `cores`, running what `each`
/// builds there. The loop's end of the rings and the crew come back for the caller to run.
///
/// Everything that can fail happens here, before the subscription's loop runs: a core that is
/// gone refuses the subscription.
pub(super) fn start_workers<Item, Each, Handle>(
    fabric: &Fabric,
    subscription: &str,
    unit: DispatchUnit,
    cores: &[usize],
    keyed: bool,
    each: Each,
) -> Result<(Feed<Item>, Crew), StartUnitError>
where
    Item: Send + 'static,
    Each: Fn() -> Handle + Send + Clone + 'static,
    Handle: AsyncFnMut(Item) + 'static,
{
    let mut crew = Vec::with_capacity(cores.len());
    let mut places = cores.iter().copied();
    let feed = open_rings(cores.len(), keyed, |worker| {
        let core = places.next().unwrap_or(0);
        let (ended, reported) = oneshot::channel();
        crew.push(reported);
        let each = each.clone();
        let alive = fabric.worker_guard();
        fabric.run_on(core, subscription, unit, move || {
            spawn(async move {
                let _alive = alive;
                // Built on the core and never leaving it.
                let mut handle = each();
                let mut budget = Budget::default();
                worker
                    .work(async move |item| {
                        handle(item).await;
                        // The core's turn for its continuations, hooks and timers, after a run.
                        if budget.spend() {
                            yield_now().await;
                        }
                    })
                    .await;
                let _ = ended.send(());
            })
        })
    })?;
    Ok((feed, Crew { ended: crew, unit }))
}

/// Spawns a task that drives `subscriber` through `handler` with `workers.count()` workers.
///
/// The workers run over the shared cores for `workers(n)` and on cores of their own for
/// `threads(n)`, `cores` naming where each runs (see [`Layout`](super::Layout)). With `by_key`
/// the worker is picked by hashing the delivery's key on the loop's core.
///
/// A sequential policy is [`spawn_dispatch`]. On shutdown the stream stops being polled and the
/// workers drain what their rings hold; dropping the returned handle drops the rings, and the workers
/// end once they have drained them.
///
/// # Errors
///
/// [`StartUnitError`] when a core a worker was to start on has ended.
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
) -> Result<JoinHandle<()>, StartUnitError>
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
    let for_workers = Arc::clone(&shared);
    let (feed, crew) = start_workers(
        fabric,
        &shared.name,
        DispatchUnit::of(workers.placement()),
        &cores[..workers.count().get()],
        workers.by_key(),
        move || {
            let shared = Arc::clone(&for_workers);
            // One encode buffer per worker, for the reason the sequential loop has one.
            let mut encode = BytesMut::new();
            async move |msg: S::Message| {
                let mut slot = Slot::new(msg);
                dispatch(
                    &shared.handler,
                    &mut slot,
                    &mut encode,
                    // A refusal boxes its message where there is a heap: the loop keeps no places.
                    Places::none(),
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
    Ok(spawn(async move {
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

/// Lets go of the rings when the loop's task ends, however it ends, so a worker parked on an empty
/// ring is woken to find the ring abandoned: a loop dropped by its handle takes its workers down
/// with it, once they have drained what they hold.
struct LetGo<Item>(Feed<Item>);

impl<Item> Drop for LetGo<Item> {
    fn drop(&mut self) {
        self.0.let_go();
    }
}

/// Reads `stream` into the rings until it ends or `stop` fires, then lets the workers drain what
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
    crew.join(name, type_name::<Item>()).await;
}
