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
//! The setup fills the queue before the measured region opens and the body drains it, so a
//! scenario measures steady-state delivery, never the first connect or the subscription open.
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
/// handlers. The hand-written half calls the same methods, so both halves pay for the signal -
/// though only the framework's call sits inside a collected frame, which is worth a handful of
/// instructions per message in its column.
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
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// A JSON body with the two fields a handler reads, padded with fields it ignores until it
/// reaches roughly `size` bytes. A zero pad leaves the bare object.
pub fn json_body(id: u64, size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{id},\"quantity\":{}", id % 97);
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
    runtime.block_on(async move {
        for index in 0..count {
            let body = json_body(index as u64, 0);
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
    runtime.block_on(async move {
        for index in 0..count {
            let body = json_body(index as u64, size);
            publisher
                .publish(OutgoingMessage::new(name, &body), None)
                .await
                .expect("in-process publish");
        }
    });
}

/// A started service whose queue is already full, with the latch its handler counts down.
pub struct Service {
    pub runtime: Runtime,
    pub latch: Latch,
    // Held so the service outlives the measured region; the handle is dropped with it.
    _app: RunningApp,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<MemoryBroker, Identity, (), Latch>;

/// Starts a one-handler service on a fresh in-memory broker and fills its queue with `messages`
/// bodies of `size` bytes.
pub fn service(messages: usize, size: usize, mount: impl FnOnce(Mount<'_>)) -> Service {
    started(messages, mount, |publisher, runtime| {
        fill(publisher, runtime, INPUT, messages, size);
    })
}

/// The same with a header contract on every delivery, for the scenarios that read one.
pub fn service_with_headers(
    messages: usize,
    headers: &[(&str, &str)],
    mount: impl FnOnce(Mount<'_>),
) -> Service {
    started(messages, mount, |publisher, runtime| {
        fill_with_headers(publisher, runtime, INPUT, messages, headers);
    })
}

fn started(
    messages: usize,
    mount: impl FnOnce(Mount<'_>),
    fill: impl FnOnce(&MemoryPublisher, &Runtime),
) -> Service {
    let runtime = runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), mount);
    let app = runtime.block_on(app.start()).expect("the service starts");
    filled(
        runtime,
        latch,
        app,
        &broker.publisher(),
        messages,
        |publisher, runtime| {
            fill(publisher, runtime);
        },
    )
}

/// Arms the latch and fills the queue behind a service that is already running.
///
/// The scenarios whose application type is their own - a layer stack, a state of their own -
/// build and start the app themselves and finish here.
pub fn filled(
    runtime: Runtime,
    latch: Latch,
    app: RunningApp,
    publisher: &MemoryPublisher,
    messages: usize,
    fill: impl FnOnce(&MemoryPublisher, &Runtime),
) -> Service {
    latch.expect(messages);
    fill(publisher, &runtime);
    assert_eq!(
        latch.remaining(),
        messages,
        "the queue was consumed while it was being filled, so the measured region would be short"
    );
    Service {
        runtime,
        latch,
        _app: app,
    }
}

/// Drains the service inside the measured region. Every framework half of a pair is this call.
pub fn drain(service: &Service) {
    measure(|| service.runtime.block_on(service.latch.drained()));
}

/// The hand-written side: the subscription, a publisher for what the loop sends on, a requester
/// for the round-trip scenario, and the count.
///
/// The subscription is opened before the queue is filled, because an in-memory subscription
/// receives what is published after it, exactly as a broker's does.
pub struct Queue {
    pub runtime: Runtime,
    pub subscriber: MemorySubscriber,
    pub publisher: MemoryPublisher,
    pub requester: MemoryRequester,
    pub messages: usize,
}

/// A subscription with `messages` bodies of `size` bytes already in it.
pub fn queue(messages: usize, size: usize) -> Queue {
    let queue = queue_unfilled(messages);
    fill(&queue.publisher, &queue.runtime, INPUT, messages, size);
    queue
}

/// The same with a header contract on every delivery.
pub fn queue_with_headers(messages: usize, headers: &[(&str, &str)]) -> Queue {
    let queue = queue_unfilled(messages);
    fill_with_headers(&queue.publisher, &queue.runtime, INPUT, messages, headers);
    queue
}

/// A subscription with nothing in it yet: the round-trip scenario publishes its input from the
/// measured body, one request at a time.
pub fn queue_unfilled(messages: usize) -> Queue {
    let runtime = runtime();
    let broker = MemoryBroker::new();
    let subscriber = broker.subscribe(INPUT);
    Queue {
        runtime,
        subscriber,
        publisher: broker.publisher(),
        requester: broker.requester(),
        messages,
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
