//! The service of `examples/transactional_publisher.rs` written without the `macros` feature.
//!
//! ```text
//! cargo test --example manual_transactional_publisher --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::ready;

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Confirmed {
    id: u64,
}

impl OutgoingDestination for Confirmed {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.confirmed";
}

impl MessageHeaders for Confirmed {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Payment {
    id: u64,
    amount: u64,
}

impl OutgoingDestination for Payment {
    type Form = CallerName;
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Debit {
    id: u64,
    amount: u64,
}

impl OutgoingDestination for Debit {
    type Form = FixedName;

    const DESTINATION: &'static str = "ledger.debits";
}

impl MessageHeaders for Debit {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Credit {
    id: u64,
    amount: u64,
}

impl OutgoingDestination for Credit {
    type Form = FixedName;

    const DESTINATION: &'static str = "ledger.credits";
}

impl MessageHeaders for Credit {
    type Contract = NoHeaders;
}

// --8<-- [start:batch]
/// Every confirmation of a batch becomes visible at once, on commit.
struct Confirm;

impl Handle<[Order], Vec<Confirmed>> for Confirm {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmed>, Vec<HandlerOutcome>>> {
        ready(Ok(orders
            .iter()
            .map(|order| Confirmed { id: order.id })
            .collect()))
    }
}
// --8<-- [end:batch]

// --8<-- [start:scope]
struct Ledger;

impl OutSlot for Ledger {
    const NAME: &'static str = "Ledger";
    type Destination = Reads;
}

impl PublishedThrough<Ledger> for Debit {}
impl PublishedThrough<Ledger> for Credit {}

/// Both entries of a payment, or neither.
struct Book;

impl<L> Handle<Payment, (), Outs<(L,)>> for Book
where
    L: OutEntry<Ledger, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        payment: &Payment,
        outs: &Outs<(L,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(scope) = outs.get(Ledger).begin().await else {
            return Err(HandlerOutcome::retry());
        };
        let (id, amount) = (payment.id, payment.amount);
        let debit = scope.message(&Debit { id, amount }).publish().await;
        let credit = scope.message(&Credit { id, amount }).publish().await;
        if debit.is_err() || credit.is_err() {
            if let Err(err) = scope.abort().await {
                tracing::warn!(payment.id, %err, "payments: aborting the ledger transaction failed");
            }
            return Err(HandlerOutcome::retry());
        }
        scope.commit().await.map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:scope]

// --8<-- [start:app]
fn app(broker: MemoryBroker) -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        // One transaction per batch, committed before the batch is acked.
        b.include(
            subscriber("orders", Confirm)
                .reply()
                .batch(nonzero!(64))
                .build(),
        )
        .out_reply(TransactionalPublish)
        .transactional()
        .include(subscriber("payments", Book).build())
        .out(Ledger, TransactionalPublish)
    })
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Confirmed, Credit, Debit, Order, Payment, app};

    #[tokio::test]
    async fn a_batch_is_confirmed_in_one_transaction() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();
        let tb = TestApp::start(app(broker)).await?;
        for id in [1, 2, 3] {
            publisher
                .message(&Order { id })
                .to("orders")
                .publish()
                .await?;
        }
        tb.settle().await?;

        let handle = tb.broker::<MemoryBroker>();
        handle
            .subscriber("orders")
            .assert_called_once()
            .assert_batch_sizes(&[3]);
        let confirmed = handle.published::<Confirmed>("orders.confirmed");
        assert_eq!(
            confirmed.assert_called(3).decoded(),
            [
                Confirmed { id: 1 },
                Confirmed { id: 2 },
                Confirmed { id: 3 }
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_payment_books_both_entries() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(MemoryBroker::new())).await?;
        let handle = tb.broker::<MemoryBroker>();
        handle
            .message(&Payment { id: 9, amount: 120 })
            .to("payments")
            .publish()
            .await?;

        handle
            .published::<Debit>("ledger.debits")
            .assert_called_once()
            .with(&Debit { id: 9, amount: 120 });
        handle
            .published::<Credit>("ledger.credits")
            .assert_called_once()
            .with(&Credit { id: 9, amount: 120 });
        handle
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
