//! The service of `examples/subscriber_handler.rs` written without the `macros` feature: the body
//! is an `impl Handle` of a named type, and `subscriber(source, body).build()` is what `include`
//! mounts.
//!
//! ```text
//! cargo test --example manual_subscriber_handler --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

mod handler {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:handler]
    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "accepted");
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

    #[tokio::test]
    async fn the_handler_takes_each_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:handler]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
