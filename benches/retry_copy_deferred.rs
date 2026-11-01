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
//! The cold path the runtime owns: a delivery that answers `retry_after` with a delay, on a
//! transport with no delayed redelivery of its own. The runtime drops the original and publishes
//! a copy through the registration's retry slot once the delay has passed on its timer; the copy
//! comes back and is acked. Measured and reported, never gated: a retry is not on the
//! steady-state path.
//!
//! The transport is the in-memory broker seen through a descriptor whose deliveries leave a delay
//! to the runtime, the way a broker with no delayed redelivery does; the copies address the
//! subscription itself.

mod common;

use std::convert::Infallible;
use std::future::ready;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Latch, MESSAGES, Order, Pending};
use futures::{Stream, StreamExt};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryMessage, MemorySubscriber};
use ruststream::{
    AckError, AddressedCopies, RedeliveryAddress, RedeliveryAddressed, Settle, Subscribe,
    Subscriber, SubscriptionSource,
};

/// The delay every retry asks for: long enough to need a timer, short enough not to hold the run.
const DELAY: Duration = Duration::from_millis(1);

/// A subscription whose deliveries leave a delayed retry to the runtime.
#[derive(Debug, Clone)]
struct Delayed {
    name: &'static str,
}

impl SubscriptionSource<ConnectedMemoryBroker> for Delayed {
    type Subscriber = DelayedSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<Self::Subscriber, MemoryError> {
        Ok(DelayedSubscriber(connected.subscribe(self.name).await?))
    }
}

impl RedeliveryAddressed<ConnectedMemoryBroker> for Delayed {
    // The subject the subscription reads is where a copy of its delivery goes.
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> {
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}

/// The in-memory subscriber, its deliveries wrapped.
struct DelayedSubscriber(MemorySubscriber);

impl Subscriber for DelayedSubscriber {
    type Message = DelayedMessage;
    type Error = <MemorySubscriber as Subscriber>::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream().map(|item| item.map(DelayedMessage))
    }
}

/// An in-memory delivery that redelivers nothing natively: no `nack_after`, no count of its own.
struct DelayedMessage(MemoryMessage);

impl IncomingMessage for DelayedMessage {
    fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }
}

impl Settle for DelayedMessage {
    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
        self.0.ack()
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        self.0.nack(requeue)
    }
}

/// The scenario's application state: the latch every scenario has, plus the number of deliveries
/// still allowed to ask for a retry.
#[derive(Debug)]
struct Retrying {
    latch: Latch,
    budget: AtomicUsize,
}

impl Retrying {
    fn take_budget(&self) -> bool {
        self.budget
            .try_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok()
    }
}

/// Asks the first `budget` deliveries to come back after [`DELAY`] and acks everything after
/// that, so the run holds exactly one copy per published message.
#[subscriber(Delayed { name: "orders" })]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Retrying>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    let state = ctx.state();
    state.latch.arrived();
    if state.take_budget() {
        return HandlerOutcome::retry_after(DELAY);
    }
    HandlerOutcome::ack()
}

/// Every published message is handled twice: once as it arrives, once as the copy its retry
/// published.
fn app(messages: usize) -> Pending {
    let runtime = common::runtime();
    let latch = Latch::default();
    let broker = MemoryBroker::new();
    let state = Retrying {
        latch: latch.clone(),
        budget: AtomicUsize::new(messages),
    };
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), |b| b.include(consume));
    let mut pending = common::built(runtime, latch, broker, app, messages, 0);
    pending.expected = messages * 2;
    pending
}

#[library_benchmark(config = common::config_ungated())]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = retry_copy_deferred; benchmarks = service);
main!(library_benchmark_groups = retry_copy_deferred);
