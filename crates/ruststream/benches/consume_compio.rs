// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    // The app runs on the one core of the measured run, the thread that drives it.
    clippy::future_not_send
)]
//! Consuming a small JSON body on the compio runner: the scenario of `consume_json`, with the app
//! on one compio core and the local memory bus in place of the tokio in-memory broker.
//!
//! The app is a service's own: the compio `RustStream`, started on the calling runtime as its
//! only core, over a `LocalMemoryBroker` that holds the whole backlog. A run is measured three
//! times, over one delivery, over [`MESSAGES`] and over twice as many, and the per-delivery figure
//! is the difference of the two longer totals divided by [`MESSAGES`], as in the tokio scenarios.
//! The service is built the way a compio service ships: `compio`, `memory`, `macros` and `json`,
//! with no tokio runner and nothing of `testing`.

use std::convert::Infallible;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use compio::runtime::Runtime;
use gungraun::{
    Callgrind, Dhat, DhatMetric, EntryPoint, LibraryBenchmarkConfig, library_benchmark,
    library_benchmark_group, main,
};
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::{AppInfo, RunningApp, RustStream};
use ruststream::{Broker, OutgoingMessage, Publisher, subscriber};
use serde::Deserialize;
use tokio::sync::Notify;

/// Deliveries per measured run, as in the tokio scenarios: `RUSTSTREAM_BENCH_MESSAGES` at build
/// time overrides it.
const MESSAGES: usize = messages(option_env!("RUSTSTREAM_BENCH_MESSAGES"));

const fn messages(configured: Option<&str>) -> usize {
    let Some(configured) = configured else {
        return 1_000;
    };
    let bytes = configured.as_bytes();
    let mut count = 0;
    let mut at = 0;
    while at < bytes.len() {
        assert!(
            bytes[at].is_ascii_digit(),
            "RUSTSTREAM_BENCH_MESSAGES must be a positive number of deliveries"
        );
        count = count * 10 + (bytes[at] - b'0') as usize;
        at += 1;
    }
    assert!(
        count > 0,
        "RUSTSTREAM_BENCH_MESSAGES must be a positive number of deliveries"
    );
    count
}

/// The bus holds the longest backlog at once: the queue is filled before the drain is measured.
const CAPACITY: usize = (2 * MESSAGES).next_power_of_two();

type Bus = LocalMemoryBroker<CAPACITY>;

/// The subscription the backlog is published to.
const INPUT: &str = "orders";

/// What starting the service and handling the first delivery allocate once; a delivery after it
/// allocates nothing. #691 added two: the live-worker count and the name a panicked loop reports.
const COLD_BLOCKS: u64 = 45;

/// The payload: two integer fields, so a decode allocates nothing.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

const BODY: &[u8] = br#"{"id":1000000,"quantity":37}"#;

/// Counts deliveries down and wakes the benchmark body when the last one has been handled, as
/// the tokio scenarios' latch does.
#[derive(Clone, Debug, Default)]
struct Latch(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Latch {
    fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

#[subscriber(LocalMemorySource::new("orders"))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

/// A service ready to start, its bus and the backlog it will take.
struct Pending {
    runtime: Runtime,
    latch: Latch,
    bus: Bus,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

fn app(messages: usize) -> Pending {
    let latch = Latch::default();
    let state = latch.clone();
    let bus = Bus::new();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(bus.clone(), |b| b.include(consume));
    Pending {
        runtime: Runtime::new().expect("a compio runtime"),
        latch,
        bus,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the app starts")),
        messages,
    }
}

/// Starts the service in the first region, fills its queue outside both, and drains the queue in
/// the second.
fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        bus,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    // The backlog is published off the runtime, so the app's loop takes none of it before the
    // drain: the bus needs no executor to take a publish.
    futures::executor::block_on(async {
        let connected = bus.connect().await.expect("the bus connects in process");
        let publisher = connected.publisher();
        for _ in 0..messages {
            publisher
                .publish(OutgoingMessage::new(INPUT, BODY), None)
                .await
                .expect("the bus holds the backlog");
        }
    });
    assert_eq!(
        latch.remaining(),
        messages,
        "the queue was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    drop(running);
}

/// The measured region: everything this runs is counted, nothing around it is. A frame of its
/// own whose result goes through `black_box`, as the tokio scenarios' `measure`.
#[inline(never)]
fn measure<T>(body: impl FnOnce() -> T) -> T {
    black_box(body())
}

const REGION: &str = "*consume_compio::measure*";

/// Callgrind and DHAT inside the measured region; the longest run is held to the cold
/// allocations alone.
fn config() -> LibraryBenchmarkConfig {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()))
        .hard_limits([(DhatMetric::TotalBlocks, COLD_BLOCKS)]);
    let mut config = LibraryBenchmarkConfig::default();
    config.tool(callgrind).tool(dhat);
    config
}

#[library_benchmark(config = config())]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(pending: Pending) {
    start_and_drain(pending);
}

library_benchmark_group!(name = consume_compio; benchmarks = service);
main!(library_benchmark_groups = consume_compio);
