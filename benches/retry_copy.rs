// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! The cold path: a delivery that answers `retry_after` and the copy that comes back. Measured
//! and reported, never gated - a retry is not on the steady-state path, and its cost is dominated
//! by the republish the fallback makes. It has no hand-written twin.

mod common;

use std::convert::Infallible;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Latch, MESSAGES, Order, Service};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::MemoryBroker;
use ruststream::memory::prelude::*;

/// The scenario's application state: the latch every scenario has, plus the number of deliveries
/// still allowed to ask for a redelivery.
#[derive(Debug)]
struct Retrying {
    latch: Latch,
    budget: AtomicUsize,
}

impl Retrying {
    fn take_budget(&self) -> bool {
        self.budget
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok()
    }
}

/// Asks the first `budget` deliveries to come back and acks everything after that, so the run
/// holds exactly one retry per published message and ends whichever order the copies arrive in.
///
/// Reading the retry count off the delivery would be the natural way to write this, and it does
/// not work here: the in-memory broker redelivers natively (`nack_after`) and counts nothing, so
/// neither the framework's retry header nor a `max_attempts` cap ever sees the copy.
#[subscriber("orders")]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Retrying>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    let state = ctx.state();
    state.latch.arrived();
    if state.take_budget() {
        return HandlerOutcome::retry_after(Duration::ZERO);
    }
    HandlerOutcome::ack()
}

/// Every published message is handled twice: once as it arrives, once as the copy its retry
/// brought back, so the latch waits for twice the published count.
fn app(messages: usize) -> Service {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = Retrying {
        latch: latch.clone(),
        budget: AtomicUsize::new(messages),
    };
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), |b| {
            b.include(consume);
        });
    let app = runtime.block_on(app.start()).expect("the service starts");
    let service = common::filled(
        runtime,
        latch,
        app,
        &broker.publisher(),
        messages,
        |publisher, runtime| {
            common::fill(publisher, runtime, common::INPUT, messages, 0);
        },
    );
    service.latch.expect(messages * 2);
    service
}

#[library_benchmark(config = common::config_ungated())]
#[bench::once(app(MESSAGES))]
fn service(app: Service) {
    common::drain(&app);
}

library_benchmark_group!(name = retry_copy; benchmarks = service);
main!(library_benchmark_groups = retry_copy);
