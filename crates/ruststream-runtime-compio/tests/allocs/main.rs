//! What a delivery costs in allocations on core 0 and across the hand-off to another core.
//!
//! Each core is a thread whose whole run is measured; running the same scenario with two
//! backlogs leaves, in the difference, what the extra deliveries cost. The measured threads are
//! started by hand, the subject being the threads' work.

use std::convert::Infallible;
use std::future::ready;
use std::iter;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use allocation_counter::measure;
use compio::runtime;
use futures::channel::oneshot;
use futures::{Stream, stream};
use ruststream_core::{AckError, HeaderMap, IncomingMessage, Settle, Subscriber};
use ruststream_runtime::{
    Context, Delivery, DispatchFailure, FailurePolicies, Handler, HandlerOutcome, NoRetry, Workers,
};
use ruststream_runtime_compio::dispatch::{Fabric, Hosted, Layout, Mount, spawn_dispatch_workers};
use ruststream_runtime_compio::{Stop, Tasks, test_runtime, time};

/// What a hand-off to another core may cost per delivery: compio's cross-thread wake queue takes a
/// block of 31 slots for every 31 wakes, and a delivery wakes a parked core at most once.
const HAND_OFF_BOUND: f64 = 1.0 / 31.0;

/// Far longer than a run: the timer never fires while deliveries are counted.
const WAITING: Duration = Duration::from_hours(1);

struct Msg {
    headers: HeaderMap,
    key: Option<&'static [u8]>,
}

impl IncomingMessage for Msg {
    fn payload(&self) -> &[u8] {
        b"payload"
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.key
    }
}

impl Settle for Msg {
    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }
}

/// A backlog made before the measuring starts, then silence.
struct Backlog(Option<Vec<Msg>>);

impl Subscriber for Backlog {
    type Message = Msg;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Msg, Infallible>> + Send + '_ {
        let backlog = self.0.take().expect("opened once").into_iter().map(Ok);
        futures::StreamExt::chain(stream::iter(backlog), stream::pending())
    }
}

/// Counts deliveries and says so once the last one is handled.
struct Counting {
    handled: AtomicUsize,
    total: usize,
    last: Mutex<Option<oneshot::Sender<()>>>,
}

impl Handler<Msg, (), ()> for Counting {
    fn handle(
        &self,
        _msg: &Msg,
        _ctx: &mut Context<'_, (), ()>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        if self.handled.fetch_add(1, Ordering::Relaxed) + 1 == self.total
            && let Some(last) = self.last.lock().ok().and_then(|mut last| last.take())
        {
            let _ = last.send(());
        }
        ready(HandlerOutcome::ack())
    }
}

/// Every allocation the cores of a run made while `total` deliveries went through `workers`.
fn allocations_of(total: usize, workers: Workers, keyed: bool, timer_pending: bool) -> u64 {
    let count = 3;
    let layout = Layout::plan(
        count,
        &[Mount {
            name: "orders",
            workers,
        }],
    )
    .expect("fits");
    let fabric = Fabric::open(count);
    let stop = Stop::default();
    let key: Option<&'static [u8]> = keyed.then_some(&b"k"[..]);
    let backlog: Vec<Msg> = iter::repeat_with(|| Msg {
        headers: HeaderMap::new(),
        key,
    })
    .take(total)
    .collect();
    let (last, finished) = oneshot::channel();
    let handler = Arc::new(Counting {
        handled: AtomicUsize::new(0),
        total,
        last: Mutex::new(Some(last)),
    });
    thread::scope(|scope| {
        let mut others = Vec::new();
        for core in 1..count {
            let (fabric, stop) = (fabric.clone(), stop.clone());
            others.push(scope.spawn(move || {
                let runtime = test_runtime().expect("a runtime");
                measure(|| runtime.block_on(fabric.serve(core, &stop))).count_total
            }));
        }
        let runtime = test_runtime().expect("a runtime");
        let main = measure(|| {
            runtime.block_on(async {
                if timer_pending {
                    // A `retry_after` copy waiting: a timer in the wheel, and so a compio timer
                    // pending, for the whole run.
                    runtime::spawn(time::sleep(WAITING)).detach();
                }
                let looped = spawn_dispatch_workers(
                    Backlog(Some(backlog)),
                    handler,
                    stop.clone(),
                    Arc::from("orders"),
                    Arc::new(()),
                    Arc::new(Hosted::<(), NoRetry>::new(
                        Delivery::unscoped(),
                        None,
                        Tasks::default(),
                    )),
                    DispatchFailure::new(FailurePolicies::default(), stop.clone()),
                    workers,
                    &fabric,
                    layout.cores(0),
                )
                .expect("the workers start");
                finished.await.expect("the last delivery is handled");
                stop.cancel();
                drop(looped);
            });
        })
        .count_total;
        others
            .into_iter()
            .map(|core| core.join().expect("a core"))
            .sum::<u64>()
            + main
    })
}

/// The allocations the extra deliveries of a longer run cost, per delivery.
fn per_delivery(workers: Workers, keyed: bool, timer_pending: bool) -> f64 {
    const SHORT: usize = 2_000;
    const LONG: usize = 6_000;
    // A first run warms the allocator and the thread machinery out of the comparison.
    let _ = allocations_of(SHORT, workers, keyed, timer_pending);
    let short = allocations_of(SHORT, workers, keyed, timer_pending);
    let long = allocations_of(LONG, workers, keyed, timer_pending);
    println!("{workers:?}: {short} allocations for {SHORT}, {long} for {LONG}");
    #[allow(clippy::cast_precision_loss)]
    {
        (long as f64 - short as f64) / (LONG - SHORT) as f64
    }
}

#[test]
fn a_delivery_allocates_nothing_on_core_zero_or_across_the_hand_off() {
    let two = NonZeroUsize::new(2).expect("two");
    let sequential = per_delivery(Workers::sequential(), false, false);
    let threads = per_delivery(Workers::threads(two), false, false);
    let keyed = per_delivery(Workers::threads_keyed(two), true, false);
    println!(
        "per delivery: sequential {sequential}, threads(2) {threads}, threads(2) by_key {keyed}"
    );
    assert!(
        sequential < 0.01,
        "sequential allocates {sequential} per delivery"
    );
    assert!(
        threads < HAND_OFF_BOUND,
        "threads(2) allocates {threads} per delivery"
    );
    assert!(
        keyed < HAND_OFF_BOUND,
        "threads(2, by_key) allocates {keyed} per delivery"
    );
}

#[test]
fn a_delivery_allocates_nothing_while_a_timer_waits_on_the_core() {
    let two = NonZeroUsize::new(2).expect("two");
    let sequential = per_delivery(Workers::sequential(), false, true);
    let threads = per_delivery(Workers::threads(two), false, true);
    let keyed = per_delivery(Workers::threads_keyed(two), true, true);
    println!(
        "per delivery, timer pending: sequential {sequential}, threads(2) {threads}, \
         threads(2) by_key {keyed}"
    );
    assert!(
        sequential < 0.01,
        "sequential allocates {sequential} per delivery with a timer pending"
    );
    assert!(
        threads < HAND_OFF_BOUND,
        "threads(2) allocates {threads} per delivery with a timer pending"
    );
    assert!(
        keyed < HAND_OFF_BOUND,
        "threads(2, by_key) allocates {keyed} per delivery with a timer pending"
    );
}
