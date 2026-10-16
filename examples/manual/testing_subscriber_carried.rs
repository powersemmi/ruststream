//! The subscriber assertions for a value a delivery already holds, against services written
//! without the `macros` feature. The test bodies are the ones of
//! `examples/testing_subscriber_carried.rs`: the harness reads the same records whichever way the
//! service was declared.
//!
//! ```text
//! cargo test --example manual_testing_subscriber_carried --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

#[path = "../support/carried_rows.rs"]
mod carried_rows;

use std::convert::Infallible;

use ruststream::memory::prelude::*;

/// Bytes published as themselves: the payload the row subscription reads a row out of. What
/// `#[derive(Outgoing, Serialized)]` writes, by hand.
struct Wire(Vec<u8>);

impl Serialized for Wire {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Wire {
    type Wire = SerializedWire;
}

impl OutgoingDestination for Wire {
    type Form = CallerName;
}

impl MessageHeaders for Wire {
    type Contract = NoHeaders;
}

mod with_value {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Wire;
    use super::carried_rows::{Row, Rows};

    // --8<-- [start:with_value]
    struct Greet;

    impl Handle<Row> for Greet {
        fn handle(
            &self,
            user: &Row,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(user.id, "greeted {}", user.name);
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("users", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber(Rows::new("users"), Greet).build());
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
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Wire;
    use super::carried_rows::{Row, Rows};

    // --8<-- [start:received_values]
    struct Greet;

    impl Handle<Row> for Greet {
        fn handle(
            &self,
            user: &Row,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(user.id, "greeted {}", user.name);
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("users", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber(Rows::new("users"), Greet).build());
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
    println!("the scenarios are tests: cargo test --example manual_testing_subscriber_carried");
}
