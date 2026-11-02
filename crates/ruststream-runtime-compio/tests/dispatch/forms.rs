//! What every loop a subscription can run under does at the edges of its stream: an error in it,
//! the end of it, and the shutdown, each form on its own cores.

use std::mem;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::thread;

use compio::runtime::JoinHandle;
use futures::channel::mpsc;
use futures::{Stream, StreamExt, stream};
use ruststream_core::{BatchStream, BatchSubscriber, Settle, Subscriber};
use ruststream_runtime::{BatchHandler, Context, SendHost, Workers};
use ruststream_runtime_compio::dispatch::{
    Fabric, Layout, Mount, spawn_batch_dispatch, spawn_dispatch_workers,
};
use ruststream_runtime_compio::{Stop, SubscriptionEnd, Tasks};
use thiserror::Error;

use crate::support::{
    Msg, Seen, collect, failure, hosted, nonzero, on_cores, on_cores_stopping, reporting,
};

#[derive(Debug, Error)]
#[error("subscriber stream failed")]
struct Fault;

/// How long a stream goes on after its items.
#[derive(Clone, Copy)]
enum Tail {
    /// It ends: the consumer the server cancelled.
    Ends,
    /// It waits for more that never come.
    Waits,
}

/// A subscriber over a script of stream items.
struct Scripted {
    items: Vec<Result<Msg, Fault>>,
    tail: Tail,
}

impl Scripted {
    fn ended() -> Self {
        Self {
            items: Vec::new(),
            tail: Tail::Ends,
        }
    }

    /// One delivery and then nothing: the loop is parked on an empty subscription.
    fn quiet() -> Self {
        Self {
            items: vec![Ok(Msg::numbered(0, None))],
            tail: Tail::Waits,
        }
    }

    /// A long backlog, every delivery ready at once: only the signal can stop a loop early.
    fn saturated() -> Self {
        Self {
            items: (0..BACKLOG)
                .map(|position| Ok(Msg::numbered(position, None)))
                .collect(),
            tail: Tail::Waits,
        }
    }

    /// An error first, so the loop has to survive it to reach any delivery.
    fn after_a_fault(count: usize) -> Self {
        let mut items = vec![Err(Fault)];
        items.extend((0..count).map(|position| Ok(Msg::numbered(position, None))));
        Self {
            items,
            tail: Tail::Ends,
        }
    }

    fn items(&mut self) -> impl Stream<Item = Result<Msg, Fault>> + Send + 'static {
        let tail = match self.tail {
            Tail::Ends => stream::empty().left_stream(),
            Tail::Waits => stream::pending().right_stream(),
        };
        stream::iter(mem::take(&mut self.items)).chain(tail)
    }
}

const BACKLOG: usize = 10_000;

impl Subscriber for Scripted {
    type Message = Msg;
    type Error = Fault;

    fn stream(&mut self) -> impl Stream<Item = Result<Msg, Fault>> + Send + '_ {
        self.items()
    }
}

impl BatchSubscriber for Scripted {
    type Batch = Vec<Msg>;

    fn batches(
        &mut self,
        _size: NonZeroUsize,
    ) -> impl BatchStream<Vec<Msg>, Error = Fault> + Send + '_ {
        // A batch closes as soon as it holds one delivery, so a subscription that goes quiet
        // hands over the delivery it has rather than waiting for the rest.
        Poured(Box::pin(self.items().map(|item| item.map(|msg| vec![msg]))))
    }
}

struct Poured<Items>(Pin<Box<Items>>);

impl<Items> BatchStream<Vec<Msg>> for Poured<Items>
where
    Items: Stream<Item = Result<Vec<Msg>, Fault>>,
{
    type Error = Fault;

    fn poll_fill(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        slot: &mut Vec<Msg>,
    ) -> Poll<Option<Result<(), Fault>>> {
        self.0
            .as_mut()
            .poll_next(cx)
            .map(|item| item.map(|batch| batch.map(|batch| slot.extend(batch))))
    }
}

/// Reports every delivery of a batch and settles it, as the batch adapters do.
struct ReportingBatches(mpsc::UnboundedSender<Seen>);

impl BatchHandler<Vec<Msg>, (), ()> for ReportingBatches {
    type Scratch = ();
    type Taken = Vec<Msg>;

    fn taken(size: NonZeroUsize) -> Vec<Msg> {
        Vec::with_capacity(size.get())
    }

    fn load(taken: &mut Vec<Msg>, slot: &mut Vec<Msg>) {
        taken.append(slot);
    }

    fn handle_batch<Host: SendHost<()>>(
        &self,
        batch: &mut Vec<Msg>,
        _scratch: &mut (),
        _ctx: &mut Context<'_, (), ()>,
        _host: &Host,
    ) -> impl Future<Output = ()> + Send {
        let seen = self.0.clone();
        async move {
            for msg in batch.drain(..) {
                let _ = seen.unbounded_send(Seen {
                    position: msg.position(),
                    key: msg.key.clone(),
                    thread: thread::current().id(),
                });
                msg.ack().await.expect("the test delivery settles");
            }
        }
    }
}

/// The forms a delivery loop runs under, the dedicated cores included.
const FORMS: [Workers; 5] = [
    Workers::sequential(),
    Workers::pool(nonzero_two()),
    Workers::keyed(nonzero_two()),
    Workers::threads(nonzero_two()),
    Workers::threads_keyed(nonzero_two()),
];

/// The forms of a batch loop; keyed lanes do not apply at batch granularity.
const BATCH_FORMS: [Workers; 3] = [
    Workers::sequential(),
    Workers::pool(nonzero_two()),
    Workers::threads(nonzero_two()),
];

const fn nonzero_two() -> NonZeroUsize {
    NonZeroUsize::new(2).expect("two")
}

/// How the loop under test is started.
#[derive(Clone, Copy, Debug)]
enum Shape {
    Deliveries,
    Batches,
}

fn start(
    shape: Shape,
    form: Workers,
    subscriber: Scripted,
    (fabric, stop, tasks): (&Fabric, &Stop, Tasks),
    seen: mpsc::UnboundedSender<Seen>,
) -> JoinHandle<()> {
    let layout = Layout::plan(
        3,
        &[Mount {
            name: "orders",
            workers: form,
        }],
    )
    .expect("fits");
    match shape {
        Shape::Deliveries => spawn_dispatch_workers(
            subscriber,
            reporting(seen),
            stop.clone(),
            Arc::from("orders"),
            Arc::new(()),
            hosted(tasks),
            failure(stop),
            form,
            fabric,
            layout.cores(0),
        ),
        Shape::Batches => spawn_batch_dispatch::<_, _, (), _, _>(
            subscriber,
            Arc::new(ReportingBatches(seen)),
            stop.clone(),
            Arc::from("orders"),
            Arc::new(()),
            hosted(tasks),
            failure(stop),
            form,
            nonzero(1),
            fabric,
            layout.cores(0),
        ),
    }
    .expect("the dispatch starts")
}

fn forms(shape: Shape) -> &'static [Workers] {
    match shape {
        Shape::Deliveries => &FORMS,
        Shape::Batches => &BATCH_FORMS,
    }
}

const SHAPES: [Shape; 2] = [Shape::Deliveries, Shape::Batches];

/// Runs one loop, signals shutdown once its first delivery was handled, and reports the positions
/// it handled before it stopped.
fn handled_before_shutdown(shape: Shape, form: Workers, subscriber: Scripted) -> Vec<usize> {
    on_cores(3, async |fabric, stop, tasks| {
        let (seen, mut arrived) = mpsc::unbounded();
        let joined = start(shape, form, subscriber, (&fabric, &stop, tasks), seen);
        // One delivery in, so the loop is running rather than about to start.
        let first = collect(&mut arrived, 1).await;
        stop.cancel();
        joined.await.expect("the loop does not panic");
        let mut handled = vec![first[0].position];
        // The loop and its handlers are gone with the task, so the channel ends where they did.
        while let Some(seen) = arrived.next().await {
            handled.push(seen.position);
        }
        handled
    })
}

#[test]
fn a_stream_error_does_not_stop_any_loop() {
    for shape in SHAPES {
        for &form in forms(shape) {
            let handled = on_cores(3, async |fabric, stop, tasks| {
                let (seen, mut arrived) = mpsc::unbounded();
                let joined = start(
                    shape,
                    form,
                    Scripted::after_a_fault(2),
                    (&fabric, &stop, tasks),
                    seen,
                );
                let mut handled: Vec<usize> = collect(&mut arrived, 2)
                    .await
                    .iter()
                    .map(|seen| seen.position)
                    .collect();
                // The script ends, so the loop terminates on its own rather than on shutdown.
                joined.await.expect("the loop does not panic");
                handled.sort_unstable();
                handled
            });
            assert_eq!(handled, [0, 1], "{shape:?} under {form:?}");
        }
    }
}

#[test]
fn shutdown_stops_a_loop_whose_subscription_never_runs_dry() {
    for shape in SHAPES {
        for &form in forms(shape) {
            let handled = handled_before_shutdown(shape, form, Scripted::saturated());
            assert!(
                handled.len() < BACKLOG,
                "{shape:?} under {form:?} drained the subscription: {} deliveries",
                handled.len()
            );
            let mut ordered = handled.clone();
            ordered.sort_unstable();
            assert_eq!(
                ordered,
                (0..handled.len()).collect::<Vec<_>>(),
                "{shape:?} under {form:?} lost a delivery or handled one twice"
            );
        }
    }
}

#[test]
fn shutdown_wakes_a_loop_parked_on_an_empty_subscription() {
    for shape in SHAPES {
        for &form in forms(shape) {
            let handled = handled_before_shutdown(shape, form, Scripted::quiet());
            assert_eq!(handled, [0], "{shape:?} under {form:?}");
        }
    }
}

/// Runs the loop until it ends on its own under `end`, and hands back the stop it ran on.
fn ended_under(shape: Shape, form: Workers, end: SubscriptionEnd) -> Stop {
    let stop = Stop::new(end);
    on_cores_stopping(3, &stop, async |fabric, stop, tasks| {
        let (seen, _arrived) = mpsc::unbounded();
        start(
            shape,
            form,
            Scripted::ended(),
            (&fabric, &stop, tasks),
            seen,
        )
        .await
        .expect("the loop does not panic");
    });
    stop
}

#[test]
fn a_stream_end_is_logged_by_default_and_leaves_the_service_running() {
    for shape in SHAPES {
        for &form in forms(shape) {
            let stop = ended_under(shape, form, SubscriptionEnd::Log);
            // `on_cores` raises the stop when the run is over; a failure is what a stream end
            // would have recorded.
            assert_eq!(stop.failure(), None, "{shape:?} under {form:?}");
        }
    }
}

#[test]
fn a_stream_end_under_fail_fast_names_the_subscription() {
    for shape in SHAPES {
        for &form in forms(shape) {
            let stop = ended_under(shape, form, SubscriptionEnd::FailFast);
            let reason = stop.failure().unwrap_or_default();
            assert!(
                reason.starts_with("orders: "),
                "{shape:?} under {form:?} must name the subscription, got {reason:?}"
            );
        }
    }
}
