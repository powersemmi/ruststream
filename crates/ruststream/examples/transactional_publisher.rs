//! `TransactionalPublisher`: one broker transaction per batch of replies, or a scope a handler
//! opens on its slot. The scenarios are tests.
//!
//! ```text
//! cargo test --example transactional_publisher --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct Confirmed {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
    amount: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger.debits")]
struct Debit {
    id: u64,
    amount: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger.credits")]
struct Credit {
    id: u64,
    amount: u64,
}

// --8<-- [start:batch]
/// Every confirmation of a batch becomes visible at once, on commit.
#[subscriber("orders", reply)]
async fn confirm(orders: &[Order]) -> Vec<Confirmed> {
    orders
        .iter()
        .map(|order| Confirmed { id: order.id })
        .collect()
}
// --8<-- [end:batch]

// --8<-- [start:scope]
#[derive(OutSlot)]
#[publishes(Debit, Credit)]
struct Ledger;

/// Both entries of a payment, or neither.
#[subscriber("payments")]
async fn book(
    payment: &Payment,
    Out(ledger): Out<impl TransactionalPublisher, Ledger>,
) -> HandlerOutcome {
    let Ok(scope) = ledger.begin().await else {
        return HandlerOutcome::retry();
    };
    let (id, amount) = (payment.id, payment.amount);
    let debit = scope.message(&Debit { id, amount }).publish().await;
    let credit = scope.message(&Credit { id, amount }).publish().await;
    if debit.is_err() || credit.is_err() {
        if let Err(err) = scope.abort().await {
            tracing::warn!(payment.id, %err, "payments: aborting the ledger transaction failed");
        }
        return HandlerOutcome::retry();
    }
    match scope.commit().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:scope]

// --8<-- [start:app]
fn app(broker: MemoryBroker) -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        // One transaction per batch, committed before the batch is acked.
        b.include(confirm.batch(nonzero!(64)))
            .out_reply(TransactionalPublish)
            .transactional()
            .include(book)
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
