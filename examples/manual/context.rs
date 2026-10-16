//! The services of `examples/context.rs` written without the `macros` feature: the context is a
//! parameter of `Handle::handle`, and a broker field is read off it by its key.
//!
//! ```text
//! cargo test --example manual_context --no-default-features --features testing,memory,json
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

mod reading {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    // --8<-- [start:handler]
    /// What the service read off one delivery, published on `receipts`.
    #[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
    struct Receipt {
        order: u64,
        channel: String,
        request: Option<String>,
    }

    // What `#[derive(Outgoing)]` with `#[outgoing(name = "receipts")]` writes.
    impl OutgoingDestination for Receipt {
        type Form = FixedName;

        const DESTINATION: &'static str = "receipts";
    }

    impl MessageHeaders for Receipt {
        type Contract = NoHeaders;
    }

    /// The context is the last parameter of `handle`, always in reach.
    struct Receive;

    impl Handle<Order, Receipt> for Receive {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
            let request = ctx
                .headers()
                .get("x-request-id")
                .map(|id| String::from_utf8_lossy(id).into_owned());
            ready(Ok(Receipt {
                order: order.id,
                channel: ctx.name().to_owned(),
                request,
            }))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Receive).reply().build());
        })
    }

    #[tokio::test]
    async fn the_handler_reads_the_channel_and_a_header() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-7");
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt {
                order: 7,
                channel: "orders".to_owned(),
                request: Some("req-7".to_owned()),
            });
        Ok(())
    }
    // --8<-- [end:handler]
}

mod field {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    // --8<-- [start:ctx]
    /// Where an order sits in the broker's log, published on `located`.
    #[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
    struct Located {
        order: u64,
        sequence: usize,
    }

    // What `#[derive(Outgoing)]` with `#[outgoing(name = "located")]` writes.
    impl OutgoingDestination for Located {
        type Form = FixedName;

        const DESTINATION: &'static str = "located";
    }

    impl MessageHeaders for Located {
        type Contract = NoHeaders;
    }

    /// The broker's per-delivery context is the fourth parameter of `Handle`; the body reads
    /// one field of it by its key.
    struct Locate;

    impl Handle<Order, Located, (), MemoryContext> for Locate {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            ctx: &mut Context<'_, MemoryContext>,
        ) -> impl Future<Output = Result<Located, HandlerOutcome>> {
            let MemoryPosition::Sequence(sequence) = ctx.context(Position) else {
                return ready(Err(HandlerOutcome::drop()));
            };
            ready(Ok(Located {
                order: order.id,
                sequence,
            }))
        }
    }

    fn app() -> impl App {
        // A retaining broker numbers its messages; its deliveries carry the position.
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(16)));
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            b.include(subscriber("orders", Locate).reply().build());
        })
    }

    #[tokio::test]
    async fn the_handler_takes_the_position_as_a_parameter() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker<Retaining>>();
        broker
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;
        broker
            .message(&Order { id: 8 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Located>("located")
            .assert_called(2)
            .with(&Located {
                order: 8,
                sequence: 1,
            });
        Ok(())
    }
    // --8<-- [end:ctx]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
