//! Shared parts of the framework's own benchmarks: what is measured, and how the measurement is
//! kept to the framework's code.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service and queue setups, the latch a handler counts deliveries down on, and the measurement
//! configuration.
//!
//! Every scenario is a pair. One half runs the real service - the app a user writes, started
//! through [`RustStream::start`], driven by the in-process
//! [`MemoryBroker`](ruststream::memory::MemoryBroker). The other half is a hand-written loop over
//! the same broker queue, doing the same decode, the same `black_box` read and the same
//! settlement, with no framework between the queue and the body. The difference between the two
//! is what the framework costs per message.
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
//! region is counted, on both halves of a pair alike: the dispatcher, the codec, the in-memory
//! transport, and tokio's share of driving them.
//!
//! Two ways of writing this down do not work, and both fail silently.
//!
//! Restricting collection to the framework's own frames (`--toggle-collect=*ruststream*`, the
//! obvious reading of "measure only our code") is the first. Callgrind's toggle is a toggle:
//! entering a matching frame flips collection, so a framework function calling another framework
//! function switches counting back off one frame deeper. What comes out is the parity of the
//! nesting rather than the framework's work - here it dropped the whole JSON decode from a
//! scenario that had a layer in its stack and reported that as a 40 percent saving. The same
//! pattern cannot be fair to the hand-written half either, whose decode sits in the benchmark's
//! own frame rather than in one of ours.
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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use std::convert::Infallible;

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::memory::prelude::*;
use ruststream::memory::{MemoryBroker, MemoryPublisher, MemoryRequester, MemorySubscriber};
use ruststream::runtime::{BrokerScope, Identity, RunningApp};
use ruststream::{HeaderMap, OutgoingMessage, Publisher};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

/// The name every scenario delivers on. A handler names it in its own `#[subscriber(..)]`
/// attribute, which takes a literal.
pub const INPUT: &str = "orders";

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

/// Deliveries per measured run. Large enough that the fixed cost of entering and leaving the
/// region is lost in the per-message number, small enough that a scenario stays under a second
/// of valgrind time.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `allocations` is the number of heap blocks the scenario allocates today. It is a hard limit:
/// the run fails when the path allocates more than it does now, which is what turns "no
/// allocation on the hot path" into something CI can hold the code to. The instruction limit is
/// relative, so it needs a baseline to compare against (`--save-baseline` on `main`,
/// `--baseline` on the pull request); without one the run only reports.
pub fn config(allocations: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, allocations)]));
    config
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
/// from the count and its publish path reported zero allocations against a hand-written loop's
/// five per message.
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

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state, which is how a service shares anything with its
/// handlers. The hand-written half calls the same methods, so both halves pay for the signal.
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
    headers: &[(&str, &str)],
) {
    let body = json_body(0);
    runtime.block_on(async move {
        for _ in 0..count {
            let mut map = HeaderMap::new();
            for (key, value) in headers {
                map.insert(*key, (*value).to_owned());
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
    let runtime = runtime();
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
}

/// A started service with its queue already full.
pub struct Ready {
    pub runtime: Runtime,
    pub latch: Latch,
    // Held so the service outlives the run.
    _app: RunningApp,
}

/// Starts the service, fills its queue, and drains it: the framework half of every pair.
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

/// The hand-written side before it opens its subscription: the broker, the runtime and what the
/// queue will hold.
///
/// The subscription opens inside the body, which is where the framework half starts its service,
/// so both halves pay their cold start in the same place.
pub struct Feed {
    pub runtime: Runtime,
    pub broker: MemoryBroker,
    pub messages: usize,
    pub size: usize,
    pub headers: &'static [(&'static str, &'static str)],
}

/// A broker whose queue will hold `messages` bodies of `size` bytes.
pub fn feed(messages: usize, size: usize) -> Feed {
    Feed {
        runtime: runtime(),
        broker: MemoryBroker::new(),
        messages,
        size,
        headers: &[],
    }
}

/// The same with a header contract on every delivery.
pub fn feed_with_headers(
    messages: usize,
    headers: &'static [(&'static str, &'static str)],
) -> Feed {
    Feed {
        headers,
        ..feed(messages, 0)
    }
}

impl Feed {
    /// Opens the subscription inside a measured region, the way the framework half starts its
    /// service there, and returns it with the queue already filled.
    pub fn subscribed(&self) -> MemorySubscriber {
        let subscriber = measure(|| self.broker.subscribe(INPUT));
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
        subscriber
    }

    /// A requester on the same broker, for the round-trip scenario.
    pub fn requester(&self) -> MemoryRequester {
        self.broker.requester()
    }

    /// A publisher on the same broker, for what a hand-written loop sends on.
    pub fn publisher(&self) -> MemoryPublisher {
        self.broker.publisher()
    }
}

/// Publishes `body` under `name` with `headers`, the way a handler's publish leaves the process.
pub async fn send_by_hand(
    publisher: &MemoryPublisher,
    name: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) {
    let mut map = HeaderMap::new();
    for (key, value) in headers {
        map.insert(*key, (*value).to_owned());
    }
    publisher
        .publish(OutgoingMessage::new(name, body).with_headers(map), None)
        .await
        .expect("an in-process publish");
}

// A benchmark measures what ships. With the harness feature on, every delivery records what the
// handler saw and every handler call runs inside a task-local scope, so a number taken with it
// compiled in is not the production path.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);
