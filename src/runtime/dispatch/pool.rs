//! The worker pool behind `workers(n)`: a fixed set of long-lived workers per subscription.
//!
//! The loop polls the subscription and hands what it pulls to a worker, which dispatches the
//! delivery to its end: handler, settle and continuations. The workers and whatever feeds them are
//! made when the loop starts, so a delivery allocates nothing on its way to one and takes no
//! reference count: a worker borrows what the loop shares for as long as it runs.
//!
//! Research (#417): how the loop feeds its workers and where they run are chosen once per pool
//! from the environment (see [`research`]), so one binary measures every variant:
//!
//! - [`handoff`]: a bounded channel per worker, the loop hands a delivery to a free one (#438);
//! - [`queue`]: one bounded MPMC queue the loop keeps topped up and the workers drain, parking
//!   only on empty; the loop wakes a worker only when it knows one is parked;
//! - [`rings`]: a bounded SPSC ring per worker, the loop pushes to the least loaded one (or the
//!   one a key hashes to), the worker drains its ring and parks only on empty;
//! - the whole subscription, loop and workers, on one pinned thread.
//!
//! The workers of the first three run as tasks of the app's runtime or each on a thread of its own
//! with a current-thread runtime ([`placement`]); under the test harness they always run on the
//! test's runtime.

mod handoff;
mod placement;
mod queue;
mod research;
mod rings;

use std::sync::Arc;

use bytes::BytesMut;
use tokio::task::JoinHandle;

use super::{
    Delivery, DispatchFailure, Handler, LocalHandler, Shutdown, Slot, Workers, dispatch,
    dispatch_local,
};
use crate::{BuildContext, IncomingMessage, Subscriber};

use placement::{Member, Placement};
use research::{Feed, Knobs};

/// What the loop shares with every worker it starts, behind one reference count taken once per
/// worker.
struct Shared<Body, State, Cx> {
    handler: Arc<Body>,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery<Cx>>,
    failure: DispatchFailure,
}

impl<Body, State, Cx> Shared<Body, State, Cx> {
    /// Dispatches one delivery to its end, writing a reply through the worker's `encode` buffer.
    async fn handle<Message>(&self, msg: Message, encode: &mut BytesMut)
    where
        Message: IncomingMessage,
        Body: Handler<Message, Cx, State>,
        Cx: BuildContext<Message> + Send + Sync + 'static,
        State: Send + Sync,
    {
        let mut slot = Slot::new(msg);
        dispatch(
            &*self.handler,
            &mut slot,
            encode,
            &self.name,
            &self.state,
            &self.delivery,
            &self.failure,
        )
        .await;
    }
}

impl<Body, State, Cx> Shared<Body, State, Cx> {
    /// [`Shared::handle`] for a [`LocalHandler`], whose future stays on the thread that polls it.
    // The `!Send` path is a prototype reached from a unit test only, not from a registration.
    #[cfg_attr(not(test), allow(dead_code))]
    async fn handle_local<Message>(&self, msg: Message, encode: &mut BytesMut)
    where
        Message: IncomingMessage,
        Body: LocalHandler<Message, Cx, State>,
        Cx: BuildContext<Message> + Send + Sync + 'static,
        State: Send + Sync,
    {
        let mut slot = Slot::new(msg);
        dispatch_local(
            &*self.handler,
            &mut slot,
            encode,
            &self.name,
            &self.state,
            &self.delivery,
            &self.failure,
        )
        .await;
    }
}

/// The workers a loop started. Dropping it aborts them: a loop aborted by the shutdown timeout
/// takes its workers down with it.
struct Crew(Vec<Member>);

impl Crew {
    /// Waits for every worker to finish, logging the ones that failed.
    async fn join(mut self) {
        for worker in std::mem::take(&mut self.0) {
            worker.join().await;
        }
    }
}

impl Drop for Crew {
    fn drop(&mut self) {
        for worker in &mut self.0 {
            worker.abort();
        }
    }
}

/// Whether this pool must keep every task on the runtime it was given: under the test harness,
/// whose clock and quiescence wait reach that runtime only.
fn stays_on_runtime<Cx>(delivery: &Delivery<Cx>) -> bool {
    #[cfg(feature = "testing")]
    if delivery.hooks.coordinator().is_some() {
        return true;
    }
    #[cfg(not(feature = "testing"))]
    let _ = delivery;
    false
}

/// Spawns the loop of a subscription with `workers.count` workers: a pool that hands each
/// delivery to a free worker, or, with `by_key`, lanes that hand it to the worker its key hashes
/// to.
// See `spawn_dispatch_workers`: each part is the registration's own.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_dispatch_pool<Sub, Body, Cx, State>(
    subscriber: Sub,
    handler: Arc<Body>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery<Cx>>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    Sub: Subscriber + Send + 'static,
    Sub::Message: Send + Sync + 'static,
    Body: Handler<Sub::Message, Cx, State> + 'static,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    let knobs = Knobs::from_env();
    let on_runtime = stays_on_runtime(&delivery);
    let shared = Arc::new(Shared {
        handler,
        name,
        state,
        delivery,
        failure,
    });
    let placement = if (knobs.pinned || workers.dedicated) && !on_runtime {
        Placement::Pinned
    } else {
        Placement::Runtime
    };
    // `threads(n)` is the ring feed on dedicated threads, whatever the environment says.
    let feed = if workers.dedicated {
        Feed::Rings
    } else {
        knobs.feed
    };
    let knobs = if workers.dedicated {
        knobs.dedicated()
    } else {
        knobs
    };
    match feed {
        // The loop and its workers on one thread of their own: the loop's thread runs the workers
        // as its own tasks. Dropping the member (an abort by the shutdown timeout) cancels them.
        Feed::Whole if !on_runtime => tokio::spawn(async move {
            let pinned = Placement::Pinned.start(0, move || {
                handoff::run(subscriber, shared, shutdown, workers, Placement::Runtime)
            });
            pinned.join().await;
        }),
        Feed::Handoff | Feed::Whole => tokio::spawn(handoff::run(
            subscriber, shared, shutdown, workers, placement,
        )),
        Feed::Queue if !workers.by_key => tokio::spawn(queue::run(
            subscriber, shared, shutdown, workers, placement, knobs,
        )),
        // A shared queue cannot keep a key on one worker; keyed lanes take a ring each.
        Feed::Queue | Feed::Rings => tokio::spawn(rings::run(
            subscriber, shared, shutdown, workers, placement, knobs,
        )),
    }
}

/// Research (#417): spawns the loop of a subscription whose `count` workers run on dedicated
/// threads with a [`LocalHandler`], whose future need not be `Send`.
// The `!Send` path is a prototype reached from a unit test only, not from a registration.
#[cfg_attr(not(test), allow(dead_code))]
// See `spawn_dispatch_workers`: each part is the registration's own.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_dispatch_threads_local<Sub, Body, Cx, State>(
    subscriber: Sub,
    handler: Arc<Body>,
    shutdown: Shutdown,
    name: Arc<str>,
    state: Arc<State>,
    delivery: Arc<Delivery<Cx>>,
    failure: DispatchFailure,
    workers: Workers,
) -> JoinHandle<()>
where
    Sub: Subscriber + Send + 'static,
    Sub::Message: Send + Sync + 'static,
    Body: LocalHandler<Sub::Message, Cx, State> + 'static,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    let knobs = Knobs::from_env().dedicated();
    let shared = Arc::new(Shared {
        handler,
        name,
        state,
        delivery,
        failure,
    });
    tokio::spawn(rings::run_local(
        subscriber, shared, shutdown, workers, knobs,
    ))
}
