//! Shared parts of the framework's own benchmarks: what is measured, and how the measurement is
//! kept to the framework's code.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service and queue setups, the latch a handler counts deliveries down on, and the measurement
//! configuration.
//!
//! A scenario runs the real service: the app a user writes, started through
//! [`RustStream::start`], driven by the in-process
//! [`MemoryBroker`](ruststream::memory::MemoryBroker). What comes out is what a message costs in
//! this crate's own code, which is what the gate holds and what the published table reports.
//! What the framework costs over a broker's own client is measured where that client is a real
//! one, in the broker crates.
//!
//! # Steady state and cold start
//!
//! Starting a service costs what it costs once: the connect, the subscription, the first
//! allocations behind them, and whatever the first delivery touches for the first time. Dividing
//! that over the messages of a run would report it as a per-message price it is not.
//!
//! So every scenario is measured twice in the same binary, over [`MESSAGES`] deliveries and over
//! twice as many, and the two totals are read as a line:
//!
//! ```text
//! per message = (total(2M) - total(M)) / M
//! cold        = total(M) - M * per message
//! ```
//!
//! Everything that happens once is in both totals and cancels in the subtraction, so the
//! per-message figure is the steady state and the remainder is the cold start, reported on its
//! own. No warm-up run is needed, and nothing has to be switched off part way through - which
//! matters for DHAT, whose counting cannot be toggled at all.
//!
//! What a body measures is therefore the start and the drain, in two regions, with the queue
//! filled between them and never counted: producing the messages is not what the scenario is
//! about.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. The setup - which builds an app and fills a queue through the very same framework
//! frames - is therefore not in the number, and neither is the teardown. Everything inside the
//! region is counted: the dispatcher, the codec, the in-memory transport, and tokio's share of
//! driving them.
//!
//! Two ways of writing this down do not work, and both fail silently.
//!
//! Restricting collection to the framework's own frames (`--toggle-collect=*ruststream*`, the
//! obvious reading of "measure only our code") is the first. Callgrind's toggle is a toggle:
//! entering a matching frame flips collection, so a framework function calling another framework
//! function switches counting back off one frame deeper. What comes out is the parity of the
//! nesting rather than the framework's work - here it dropped the whole JSON decode from a
//! scenario that had a layer in its stack and reported that as a 40 percent saving. The same
//!
//! Toggling on the benchmark function, which is the harness default, is the second. A body that
//! hands a closure to a generic function - `block_on` in every scenario here - makes the compiler
//! spell that closure's type, and the benchmark function's own name inside it, into the name of
//! every instance it reaches. Those nested names match the toggle too, and collection comes out
//! inverted: four of ten scenarios reported the cost of the process exit and nothing else.
//! [`measure`] avoids it by being the only frame that carries its name.
//!
//! DHAT is pointed at the same frame, as a filter on the stack rather than as a toggle. The
//! number to read is `Total blocks` - allocations per run, not bytes.
//!
//! # Reading the numbers
//!
//! Instruction counts are exact and repeat to the digit between runs on one binary. They do move
//! by a fraction of a percent when unrelated code in the same binary changes what the optimizer
//! inlines, which is why the gate sits at two percent and not at zero.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::fmt::Write;
use std::future::{Future, ready};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use std::convert::Infallible;

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublisher};
use ruststream::runtime::{BrokerScope, Identity, RunningApp};
use ruststream::{HeaderMap, Lend, OutgoingMessage, PairError, PublishPolicy, Publisher};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

/// The name every scenario delivers on. A handler names it in its own `#[subscriber(..)]`
/// attribute, which takes a literal.
pub const INPUT: &str = "orders";

/// The publish policy of a transport that only reads the payload: what every broker crate that
/// packs the body into a frame of its own declares.
///
/// It is here rather than in a scenario file because it is the counterpart of the in-memory
/// broker for the publish side: the bus keeps what it is handed and pays for a buffer per
/// message, while this one reads and keeps nothing, which is what the framework's own path costs
/// with nothing of a transport in it.
#[derive(Debug, Clone, Copy)]
pub struct SinkPublish;

impl PublishPolicy<ConnectedMemoryBroker> for SinkPublish {
    type Live = Sink;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<Sink, PairError>> {
        ready(Ok(Sink))
    }
}

/// The live half of [`SinkPublish`]: it reads the message and answers `Ok`, so the number is the
/// framework's path to a transport rather than any transport's own work.
#[derive(Debug, Clone, Copy)]
pub struct Sink;

impl Publisher for Sink {
    type Payload = Lend;
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        let (name, payload, headers) = msg.into_parts();
        black_box((name.len(), payload, headers.len()));
        ready(Ok(()))
    }
}

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run.
///
/// The default is large enough that the fixed cost of entering and leaving the region is lost in
/// the per-message number and small enough that a scenario stays under a second of valgrind time.
/// `RUSTSTREAM_BENCH_MESSAGES` at build time overrides it (`just bench 5000`) for a
/// steadier number at the price of a longer run; the published document is measured at the
/// default, and the allocation limits scale with the count through [`config`].
pub const MESSAGES: usize = messages(option_env!("RUSTSTREAM_BENCH_MESSAGES"));

/// The count a run measures when nothing names one.
const DEFAULT_MESSAGES: usize = 1_000;

/// The configured count, or the default; a value that is not a positive number is a build error
/// naming the variable, so a typo cannot silently measure the default.
const fn messages(configured: Option<&str>) -> usize {
    let Some(text) = configured else {
        return DEFAULT_MESSAGES;
    };
    let bytes = text.as_bytes();
    let mut count = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        let digit = bytes[index];
        assert!(
            digit.is_ascii_digit(),
            "RUSTSTREAM_BENCH_MESSAGES must be a positive number of deliveries"
        );
        count = count * 10 + (digit - b'0') as usize;
        index += 1;
    }
    assert!(
        count > 0,
        "RUSTSTREAM_BENCH_MESSAGES must be a positive number of deliveries"
    );
    count
}

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice the default count of deliveries) is held to, so the run
/// fails when the path allocates more than it does today. That is what turns "no allocation on
/// the hot path" into something CI can hold the code to. The instruction limit is relative, so
/// it needs a baseline to compare against (`--save-baseline` on the branch below,
/// `--baseline` on the pull request); without one the run only reports.
///
/// On a consume scenario `steady` is zero and `cold` is all there is. On a publish scenario it
/// is what the message costs the transport underneath, with nothing of the framework's above it.
/// Either way both numbers are floors the code is held to rather than budgets it may spend, so a
/// number that goes up is a defect and a number that goes down is lowered here in the same
/// change.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries.
///
/// A batch handler allocates per batch, so a per-delivery figure for it would be a fraction; it
/// states its floor over a round number of deliveries instead, and the limit scales from there.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`].
///
/// The division rounds up, so a rate stated over a number of deliveries the run is not a
/// multiple of never trips on the rounding.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// The same configuration without the allocation gate, for a path that is measured and reported
/// but not held to a number.
pub fn config_ungated() -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config.tool(callgrind()).tool(dhat());
    config
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
///
/// It has to be a frame of its own whose name appears nowhere else in the binary, which is why
/// the body goes in a closure here rather than in the benchmark function (see the module notes).
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    body()
}

/// DHAT with a stack window deep enough to reach the benchmark function.
///
/// An allocation counts when the benchmark's own frame is in its stack, and valgrind records
/// only the innermost frames - twelve by default. A publish from inside a dispatched handler sits
/// far deeper than that, so with the default window the framework's allocations were dropped
/// from the count and the publish path reported a flat zero per message: an allocation-free
/// publish that was nothing of the kind.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: one thread means one order of execution, and the same instruction
/// count on every run.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// A runtime with worker threads, which is what a service runs on.
///
/// The instruction counts keep the current-thread runtime above, and price one class of work at
/// a fraction of what it is: a counter shared between a publishing thread and a dispatching one
/// is the same single instruction to callgrind that an uncontended counter in L1 is. What the
/// hand-over between two cores actually costs is visible by wall clock and nowhere else, so the
/// scenario that measures it runs here.
pub fn worker_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("multi-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state, which is how a service shares anything with its
/// handlers. Every scenario signals through one of these, so what it costs is in every published
/// number alike.
///
/// What a delivery pays for it is one relaxed decrement and the branch that reads it; the waiter
/// is a single future for the whole run, woken once, when the last delivery lands. A signal that
/// created and dropped a future per delivery would put its own machinery in the per-message
/// number, which is the framework's number to report.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    ///
    /// Relaxed: nothing is published through the counter, and the wake itself is what orders the
    /// handler's writes against the waiter.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    ///
    /// One `Notified` for the run: the counter is read before waiting and after the wake, and the
    /// wake comes once, from the delivery that brought the count to zero.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// A JSON body with the two fields a handler reads, padded with fields it ignores until it
/// reaches roughly `size` bytes. A zero pad leaves the bare object.
///
/// Every delivery of a run carries the same bytes, and that is the point: a body whose numbers
/// grew with the message index would cost a digit more to parse - and to print again on the
/// publish paths - in the second half of a run, and the two-point method would read that growth
/// as a steeper per-message cost and a negative cold start.
pub fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    while body.len() + 2 < size {
        write!(body, ",\"f{field}\":\"{field:016}\"").expect("writing to a String");
        field += 1;
    }
    body.push('}');
    body.into_bytes()
}

/// Publishes `count` bodies under `name` with a fixed set of headers on each.
pub fn fill_with_headers(
    publisher: &MemoryPublisher,
    runtime: &Runtime,
    name: &str,
    count: usize,
    headers: &[(&'static str, &str)],
) {
    let body = json_body(0);
    runtime.block_on(async move {
        for _ in 0..count {
            let mut map = HeaderMap::new();
            for (key, value) in headers {
                map.insert(Str::from_static(key), (*value).to_owned());
            }
            publisher
                .publish(OutgoingMessage::new(name, &body).with_headers(map), None)
                .await
                .expect("in-process publish");
        }
    });
}

/// Publishes `count` bodies under `name`, through the broker's own publisher.
///
/// Part of every setup, never of a measured region: the deliveries are in the queue before the
/// body runs, so what the body pays for is delivery, not production.
pub fn fill(publisher: &MemoryPublisher, runtime: &Runtime, name: &str, count: usize, size: usize) {
    let body = json_body(size);
    runtime.block_on(async move {
        for _ in 0..count {
            publisher
                .publish(OutgoingMessage::new(name, &body), None)
                .await
                .expect("in-process publish");
        }
    });
}

/// A service that is built but not started, and what its queue will hold.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that a scenario with a layer stack or a
/// state of its own hands over the same type as every other one; the one indirect call it adds
/// lands in the cold number and nowhere else.
pub struct Pending {
    pub runtime: Runtime,
    pub latch: Latch,
    pub broker: MemoryBroker,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    pub messages: usize,
    pub size: usize,
    /// Handler calls to wait for, which is the message count except where a delivery is handled
    /// more than once.
    pub expected: usize,
    /// The header contract every delivery carries, empty where a scenario reads none.
    pub headers: &'static [(&'static str, &'static str)],
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<MemoryBroker, Identity, (), Latch>;

/// Builds a one-handler service on a fresh in-memory broker, ready to be started by the body.
pub fn pending(messages: usize, size: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    pending_on(runtime(), messages, size, mount)
}

/// The same on a runtime the scenario chose, for the one that needs worker threads.
pub fn pending_on(
    runtime: Runtime,
    messages: usize,
    size: usize,
    mount: impl FnOnce(Mount<'_>),
) -> Pending {
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), mount);
    built(runtime, latch, broker, app, messages, size)
}

/// The same with a header contract on every delivery, for the scenarios that read one.
pub fn pending_with_headers(
    messages: usize,
    headers: &'static [(&'static str, &'static str)],
    mount: impl FnOnce(Mount<'_>),
) -> Pending {
    Pending {
        headers,
        ..pending(messages, 0, mount)
    }
}

/// The same for a scenario whose application type is its own - a layer stack, a state of its own -
/// and which therefore builds the app itself.
pub fn built<Layers, State, Pipeline, Phase>(
    runtime: Runtime,
    latch: Latch,
    broker: MemoryBroker,
    app: RustStream<Layers, State, Pipeline, Phase>,
    messages: usize,
    size: usize,
) -> Pending
where
    Layers: Send + 'static,
    State: Send + Sync + 'static,
    Pipeline: 'static,
    Phase: 'static,
{
    Pending {
        runtime,
        latch,
        broker,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
        size,
        expected: messages,
        headers: &[],
    }
}

impl Pending {
    /// Starts the service and fills its queue, for a caller that measures neither - the
    /// wall-clock runs, whose harness times the closure it is given rather than a region.
    #[must_use]
    pub fn ready(self) -> Ready {
        let running = (self.start)(&self.runtime);
        self.latch.expect(self.expected);
        if self.headers.is_empty() {
            fill(
                &self.broker.publisher(),
                &self.runtime,
                INPUT,
                self.messages,
                self.size,
            );
        } else {
            fill_with_headers(
                &self.broker.publisher(),
                &self.runtime,
                INPUT,
                self.messages,
                self.headers,
            );
        }
        Ready {
            runtime: self.runtime,
            latch: self.latch,
            _app: running,
        }
    }

    /// Starts the service and hands the body what it needs to fill the queue itself.
    ///
    /// The scenarios above fill before the timing, because production is not what they measure.
    /// This one cannot: on a runtime with worker threads the handlers drain the queue while it is
    /// being filled, so a pre-filled queue would leave the timing covering whatever was left.
    #[must_use]
    pub fn started(self) -> Started {
        let running = (self.start)(&self.runtime);
        self.latch.expect(self.expected);
        Started {
            publisher: self.broker.publisher(),
            body: json_body(self.size),
            messages: self.messages,
            runtime: self.runtime,
            latch: self.latch,
            _app: running,
        }
    }
}

/// A started service whose queue the measured body fills itself.
pub struct Started {
    pub runtime: Runtime,
    latch: Latch,
    publisher: MemoryPublisher,
    body: Vec<u8>,
    messages: usize,
    // Held so the service outlives the run.
    _app: RunningApp,
}

impl Started {
    /// Publishes the run's messages and waits for the last one to be handled.
    ///
    /// The publish runs on the thread that calls this and the handler on a worker thread, so what
    /// the timing covers is a delivery travelling from one core to another.
    pub fn publish_and_drain(&self) {
        self.runtime.block_on(async {
            for _ in 0..self.messages {
                self.publisher
                    .publish(OutgoingMessage::new(INPUT, &self.body), None)
                    .await
                    .expect("in-process publish");
            }
            self.latch.drained().await;
        });
    }
}

/// A started service with its queue already full.
pub struct Ready {
    pub runtime: Runtime,
    pub latch: Latch,
    // Held so the service outlives the run.
    _app: RunningApp,
}

/// Starts the service, fills its queue, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first region is the cold
/// start - connect, subscription, the allocations behind them - and the second is the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        broker,
        start,
        messages,
        size,
        expected,
        headers,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(expected);
    if headers.is_empty() {
        fill(&broker.publisher(), &runtime, INPUT, messages, size);
    } else {
        fill_with_headers(&broker.publisher(), &runtime, INPUT, messages, headers);
    }
    assert_eq!(
        latch.remaining(),
        expected,
        "the queue was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    drop(running);
}

// A benchmark measures what ships. With the harness feature on, every delivery records what the
// handler saw and every handler call runs inside a task-local scope, so a number taken with it
// compiled in is not the production path.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);
