//! The service of `examples/app_state.rs` written without the `macros` feature: the state writes
//! its `FromRef` impls, and the handler resolves `State<T>` itself. The test body is the same.
//!
//! ```text
//! cargo test --example manual_app_state --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the test that uses it exists only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    items: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Receipt {
    order_id: u64,
    total: u64,
}

// `#[outgoing(name = "receipts")]` by hand: the type fixes its destination.
impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

mod any_struct {
    use std::convert::Infallible;
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    // --8<-- [start:state]
    /// A price list, loaded once at startup.
    #[derive(Clone)]
    struct Prices {
        per_item: u64,
    }

    /// The application state.
    struct AppState {
        prices: Prices,
    }

    // What `#[derive(FromRef)]` writes for each field: `State<Prices>` resolves through it.
    impl FromRef<AppState> for Prices {
        fn from_ref(state: &AppState) -> Self {
            state.prices.clone()
        }
    }

    struct Price;

    // The state type is the last parameter of `Handle`; the mount checks it against the app's.
    impl Handle<Order, Receipt, (), (), AppState> for Price {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            ctx: &mut Context<'_, (), AppState>,
        ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
            let prices = Prices::from_ref(ctx.state());
            ready(Ok(Receipt {
                order_id: order.id,
                total: order.items * prices.per_item,
            }))
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
                b.include(subscriber("orders", Price).reply().build())
                    .out_reply(Publish);
            })
    }
    // --8<-- [end:state]

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
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
