//! The subscriber assertions for a value a delivery already holds: a broker whose client decodes
//! a delivery itself lends its handler that value, which never was bytes. Every scenario is a test
//! of its own, the way a service writes it. The subscription that carries rows lives in
//! `examples/support/carried_rows.rs`, the part a broker crate ships.
//!
//! ```text
//! cargo test --example testing_subscriber_carried --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

#[path = "support/carried_rows.rs"]
mod carried_rows;

use ruststream::memory::prelude::*;

/// Bytes published as themselves: the payload the row subscription reads a row out of.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

mod with_value {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Wire;
    use super::carried_rows::{Row, Rows};

    // --8<-- [start:with_value]
    #[subscriber(Rows::new("users"))]
    async fn greet(user: &Row) -> HandlerOutcome {
        tracing::info!(user.id, "greeted {}", user.name);
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("users", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(greet);
        })
    }

    #[tokio::test]
    async fn the_handler_got_the_row() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Wire(b"7:alice".to_vec()))
            .to("users")
            .publish()
            .await?;

        broker
            .subscriber("users")
            .assert_called_once()
            .with_value(&Row::new(7, "alice"))
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:with_value]
}

mod received_values {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Wire;
    use super::carried_rows::{Row, Rows};

    // --8<-- [start:received_values]
    #[subscriber(Rows::new("users"))]
    async fn greet(user: &Row) -> HandlerOutcome {
        tracing::info!(user.id, "greeted {}", user.name);
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("users", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(greet);
        })
    }

    #[tokio::test]
    async fn the_handler_got_every_row() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        for row in ["7:alice", "8:bob"] {
            broker
                .message(&Wire(row.as_bytes().to_vec()))
                .to("users")
                .publish()
                .await?;
        }

        assert_eq!(
            broker.subscriber("users").received_values::<Row>(),
            [Row::new(7, "alice"), Row::new(8, "bob")],
        );
        Ok(())
    }
    // --8<-- [end:received_values]
}

fn main() {
    println!("the scenarios are tests: cargo test --example testing_subscriber_carried");
}
