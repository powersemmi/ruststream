//! Redelivery on request, written without the `macros` feature: `retry_after` brings a delivery
//! back after a delay, per element on a batch, and `max_attempts` with `dead_letter` ends a
//! message that keeps coming back. The tests are the ones of `examples/retry.rs`.
//!
//! ```text
//! cargo test --example manual_retry --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::collections::HashSet;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use ruststream::memory::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Payment {
    id: u64,
}

// `#[outgoing(name = "payments")]` by hand: the type fixes its destination.
impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

/// Stands in for the bank's ledger: which payments have cleared.
#[derive(Default)]
struct Ledger {
    cleared: Mutex<HashSet<u64>>,
}

impl Ledger {
    fn clear(&self, id: u64) {
        if let Ok(mut cleared) = self.cleared.lock() {
            cleared.insert(id);
        }
    }

    fn is_cleared(&self, id: u64) -> bool {
        self.cleared
            .lock()
            .is_ok_and(|cleared| cleared.contains(&id))
    }
}

mod retry_after {
    use std::convert::Infallible;
    use std::error::Error;
    use std::future::ready;
    use std::sync::Arc;
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Ledger, Payment};

    // --8<-- [start:retry_after]
    const RECHECK: Duration = Duration::from_secs(5);

    struct Reconcile;

    impl Handle<Payment, (), (), (), Arc<Ledger>> for Reconcile {
        fn handle(
            &self,
            payment: &Payment,
            _outs: &(),
            ctx: &mut Context<'_, (), Arc<Ledger>>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            if !ctx.state().is_cleared(payment.id) {
                // The bank has not cleared it yet: an immediate redelivery would only spin.
                return ready(Err(HandlerOutcome::retry_after(RECHECK)));
            }
            tracing::info!(payment.id, "reconciled");
            ready(Ok(()))
        }
    }
    // --8<-- [end:retry_after]

    fn app() -> impl App<State = Arc<Ledger>> {
        RustStream::new(AppInfo::new("payments", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Ledger::default())))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(subscriber("payments", Reconcile).build())
            })
    }

    #[tokio::test(start_paused = true)]
    async fn a_pending_payment_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let ledger = Arc::new(Ledger::default());
        let tb = TestApp::with_state(app(), |_| Arc::clone(&ledger)).await?;
        tb.message(&Payment { id: 7 }).publish().await?;
        tb.broker::<MemoryBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        ledger.clear(7);
        tb.advance(RECHECK).await?;
        tb.broker::<MemoryBroker>()
            .subscriber("payments")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

mod batch_retry_after {
    use std::convert::Infallible;
    use std::error::Error;
    use std::future::ready;
    use std::sync::Arc;
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Ledger, Payment};

    // --8<-- [start:batch_retry_after]
    const RECHECK: Duration = Duration::from_secs(30);

    /// One outcome per element: a cleared payment is acked, a pending one alone comes back.
    struct Reconcile;

    impl Handle<[Payment], (), (), (), Arc<Ledger>> for Reconcile {
        fn handle(
            &self,
            payments: &[Payment],
            _outs: &(),
            ctx: &mut Context<'_, (), Arc<Ledger>>,
        ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
            ready(Err(payments
                .iter()
                .map(|payment| {
                    if ctx.state().is_cleared(payment.id) {
                        HandlerOutcome::ack()
                    } else {
                        HandlerOutcome::retry_after(RECHECK)
                    }
                })
                .collect()))
        }
    }

    fn app(broker: MemoryBroker<Retaining>) -> impl App<State = Arc<Ledger>> {
        RustStream::new(AppInfo::new("payments", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Ledger::default())))
            .with_broker(broker, |b| {
                b.include(
                    subscriber("payments", Reconcile)
                        .batch(nonzero!(2))
                        .start_at(MemoryPosition::start())
                        .build(),
                )
            })
    }
    // --8<-- [end:batch_retry_after]

    #[tokio::test(start_paused = true)]
    async fn only_the_pending_payment_comes_back() -> Result<(), Box<dyn Error>> {
        // The log already holds both payments, so the subscription reads them as one batch.
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(16)));
        let publisher = broker.publisher();
        for id in [1, 2] {
            publisher.message(&Payment { id }).publish().await?;
        }
        let ledger = Arc::new(Ledger::default());
        ledger.clear(1);
        let tb = TestApp::with_state(app(broker), |_| Arc::clone(&ledger)).await?;
        tb.settle().await?;

        tb.advance(RECHECK).await?;
        let batches = tb
            .broker::<MemoryBroker<Retaining>>()
            .subscriber("payments")
            .assert_called(2)
            .batches::<Payment>();
        assert_eq!(batches[1], [Payment { id: 2 }]);
        Ok(())
    }
}

mod declaration {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Payment;

    // --8<-- [start:declaration]
    /// The gateway rejects this payment every time, so on its own it would never stop coming back.
    struct Charge;

    impl Handle<Payment> for Charge {
        fn handle(
            &self,
            payment: &Payment,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::warn!(payment.id, "gateway refused the charge");
            ready(Err(HandlerOutcome::retry()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            // Three deliveries, then the payment goes to the dead-letter subject.
            b.include(subscriber("payments", Charge).build())
                .max_attempts(nonzero!(3u32))
                .dead_letter("payments.dead")
        })
    }
    // --8<-- [end:declaration]

    #[tokio::test]
    async fn the_third_failure_goes_to_the_dead_letter_subject() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Payment { id: 7 }).publish().await?;

        let broker = tb.broker::<MemoryBroker>();
        broker.subscriber("payments").assert_called(3);
        broker
            .published::<Payment>("payments.dead")
            .assert_called_once()
            .with(&Payment { id: 7 });
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
