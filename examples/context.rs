//! The per-delivery context: a handler that reads the channel and a header off `Context`, and a
//! handler that takes one broker field as a parameter with `Ctx<K>`. Each scenario is a test.
//!
//! ```text
//! cargo test --example context --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::Outgoing;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

mod reading {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    // --8<-- [start:handler]
    /// What the service read off one delivery, published on `receipts`.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "receipts")]
    struct Receipt {
        order: u64,
        channel: String,
        request: Option<String>,
    }

    #[subscriber("orders", reply)]
    async fn receive(order: &Order, ctx: &mut Context<'_>) -> Receipt {
        let request = ctx
            .headers()
            .get("x-request-id")
            .map(|id| String::from_utf8_lossy(id).into_owned());
        Receipt {
            order: order.id,
            channel: ctx.name().to_owned(),
            request,
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(receive);
        })
    }
    // --8<-- [end:handler]

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
}

mod field {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    // --8<-- [start:ctx]
    /// Where an order sits in the broker's log, published on `located`.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "located")]
    struct Located {
        order: u64,
        sequence: usize,
    }

    /// `Ctx<Position>` hands the body this delivery's position: one field of the broker's
    /// per-delivery context, with no `&mut Context` parameter.
    #[subscriber("orders", reply)]
    async fn locate(order: &Order, Ctx(at): Ctx<Position>) -> Result<Located, HandlerOutcome> {
        let MemoryPosition::Sequence(sequence) = at else {
            return Err(HandlerOutcome::drop());
        };
        Ok(Located {
            order: order.id,
            sequence,
        })
    }

    fn app() -> impl App {
        // A retaining broker numbers its messages; its deliveries carry the position.
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(16)));
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            b.include(locate);
        })
    }
    // --8<-- [end:ctx]

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
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
