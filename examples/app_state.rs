//! Shared state: one value produced by `on_startup`, and any of its fields taken by a handler as
//! `State<T>`. The scenario is a test.
//!
//! ```text
//! cargo test --example app_state --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the test that uses it exists only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    items: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
    total: u64,
}

mod any_struct {
    use std::convert::Infallible;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    // --8<-- [start:state]
    /// A price list, loaded once at startup.
    #[derive(Clone)]
    struct Prices {
        per_item: u64,
    }

    /// The application state: each field is available to a handler as `State<FieldType>`.
    #[derive(FromRef)]
    struct AppState {
        prices: Prices,
    }

    #[subscriber("orders", reply)]
    async fn price(order: &Order, State(prices): State<Prices>) -> Receipt {
        Receipt {
            order_id: order.id,
            total: order.items * prices.per_item,
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .on_startup(async move |()| {
                Ok::<_, Infallible>(AppState {
                    prices: Prices { per_item: 25 },
                })
            })
            .with_broker(MemoryBroker::new(), |b| {
                b.include(price).out_reply(Publish);
            })
    }

    #[tokio::test]
    async fn the_handler_reads_the_price_list() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, items: 3 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt {
                order_id: 7,
                total: 75,
            });
        Ok(())
    }
    // --8<-- [end:state]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
