//! The service of `examples/owned_transactions.rs` written without the `macros` feature.
//!
//! ```text
//! cargo test --example manual_owned_transactions --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Transfer {
    id: u64,
    amount: u64,
}

impl OutgoingDestination for Transfer {
    type Form = CallerName;
}

impl MessageHeaders for Transfer {
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

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Audited {
    id: u64,
}

impl OutgoingDestination for Audited {
    type Form = FixedName;

    const DESTINATION: &'static str = "ledger.audit";
}

impl MessageHeaders for Audited {
    type Contract = NoHeaders;
}

// --8<-- [start:handler]
struct Ledger;

impl OutSlot for Ledger {
    const NAME: &'static str = "Ledger";
    type Destination = Reads;
}

impl PublishedThrough<Ledger> for Debit {}
impl PublishedThrough<Ledger> for Credit {}
impl PublishedThrough<Ledger> for Audited {}

/// The audit record and the entries settle apart: two transactions open on one slot.
struct Book;

impl<L> Handle<Transfer, (), Outs<(L,)>> for Book
where
    L: OutEntry<Ledger, Wire: OwnedTransactions>,
{
    async fn handle(
        &self,
        transfer: &Transfer,
        outs: &Outs<(L,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let ledger = outs.get(Ledger);
        let Ok(mut audit) = ledger.transaction().await else {
            return Err(HandlerOutcome::retry());
        };
        let Ok(mut entries) = ledger.transaction().await else {
            return Err(HandlerOutcome::retry());
        };
        let id = transfer.id;
        if audit.message(&Audited { id }).publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        if transfer.amount == 0 {
            // Nothing to book: the entries go, the audit record stays.
            if entries.abort().await.is_err() || audit.commit().await.is_err() {
                return Err(HandlerOutcome::retry());
            }
            return Ok(());
        }
        let amount = transfer.amount;
        let debit = entries.message(&Debit { id, amount }).publish().await;
        let credit = entries.message(&Credit { id, amount }).publish().await;
        if debit.is_err()
            || credit.is_err()
            || entries.commit().await.is_err()
            || audit.commit().await.is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("transfers", Book).build())
            .out(Ledger, Publish)
            .build();
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

    use super::{Audited, Credit, Debit, Transfer, app};

    #[tokio::test]
    async fn a_transfer_books_its_entries_and_its_audit() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Transfer { id: 3, amount: 50 })
            .to("transfers")
            .publish()
            .await?;

        broker
            .published::<Debit>("ledger.debits")
            .assert_called_once()
            .with(&Debit { id: 3, amount: 50 });
        broker
            .published::<Credit>("ledger.credits")
            .assert_called_once()
            .with(&Credit { id: 3, amount: 50 });
        broker
            .published::<Audited>("ledger.audit")
            .assert_called_once()
            .with(&Audited { id: 3 });
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_transfer_keeps_only_its_audit() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Transfer { id: 4, amount: 0 })
            .to("transfers")
            .publish()
            .await?;

        broker
            .published::<Debit>("ledger.debits")
            .assert_not_called();
        broker
            .published::<Credit>("ledger.credits")
            .assert_not_called();
        broker
            .published::<Audited>("ledger.audit")
            .assert_called_once()
            .with(&Audited { id: 4 });
        broker
            .subscriber("transfers")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
