//! What a handler returns settles its delivery: a `HandlerOutcome` names the settlement, `()`
//! acks, and a `Result` acks or drops by its variant. Each scenario is a test.
//!
//! ```text
//! cargo test --example handler_outcome --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream::memory::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    sku: String,
}

impl Order {
    fn new(id: u64, sku: &str) -> Self {
        Self {
            id,
            sku: sku.to_owned(),
        }
    }
}

mod outcome {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::Order;

    // --8<-- [start:outcome]
    /// Stands in for a stock service whose first reservation finds the shelf locked.
    struct Warehouse {
        locked: AtomicBool,
    }

    impl Warehouse {
        fn try_reserve(&self, _sku: &str) -> bool {
            !self.locked.swap(false, Ordering::SeqCst)
        }
    }

    #[subscriber("orders")]
    async fn reserve(order: &Order, ctx: &mut Context<'_, (), Arc<Warehouse>>) -> HandlerOutcome {
        if order.sku.is_empty() {
            // Nothing to reserve, and a redelivery would not change that.
            return HandlerOutcome::drop();
        }
        if !ctx.state().try_reserve(&order.sku) {
            // A passing condition: the broker brings the order back.
            return HandlerOutcome::retry();
        }
        HandlerOutcome::ack()
    }
    // --8<-- [end:outcome]

    fn app() -> impl App<State = Arc<Warehouse>> {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .on_startup(async move |()| {
                Ok::<_, Infallible>(Arc::new(Warehouse {
                    locked: AtomicBool::new(true),
                }))
            })
            .with_broker(MemoryBroker::new(), |b| {
                b.include(reserve);
            })
    }

    #[tokio::test]
    async fn each_order_settles_by_what_the_body_returned() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Order::new(1, "")).publish().await?;
        tb.message(&Order::new(2, "lamp")).publish().await?;

        // The empty order is dropped; the locked one comes back once and is acked.
        assert_eq!(
            tb.broker::<MemoryBroker>().subscriber("orders").outcomes(),
            [Outcome::Drop, Outcome::Nack, Outcome::Ack],
        );
        Ok(())
    }
}

mod unit {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:unit]
    /// A body with nothing to decide returns nothing, and the delivery is acked.
    #[subscriber("orders")]
    async fn audit(order: &Order) {
        tracing::info!(order.id, sku = %order.sku, "seen");
    }
    // --8<-- [end:unit]

    fn app() -> impl App {
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(audit);
        })
    }

    #[tokio::test]
    async fn a_body_returning_nothing_acks() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Order::new(7, "lamp")).publish().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

mod result {
    use std::error::Error;
    use std::fmt;

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::Order;

    // --8<-- [start:result]
    #[derive(Debug)]
    struct UnknownSku;

    impl fmt::Display for UnknownSku {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("unknown sku")
        }
    }

    impl Error for UnknownSku {}

    fn price_of(sku: &str) -> Result<u64, UnknownSku> {
        match sku {
            "lamp" => Ok(40),
            _ => Err(UnknownSku),
        }
    }

    /// `Ok` acks and `Err` drops, so `?` is how the body gives up on an order.
    #[subscriber("orders")]
    async fn price(order: &Order) -> Result<(), UnknownSku> {
        let cents = price_of(&order.sku)?;
        tracing::info!(order.id, cents, "priced");
        Ok(())
    }
    // --8<-- [end:result]

    fn app() -> impl App {
        RustStream::new(AppInfo::new("pricing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(price);
        })
    }

    #[tokio::test]
    async fn an_error_drops_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Order::new(1, "lamp")).publish().await?;
        tb.message(&Order::new(2, "sofa")).publish().await?;

        assert_eq!(
            tb.broker::<MemoryBroker>().subscriber("orders").outcomes(),
            [Outcome::Ack, Outcome::Drop],
        );
        Ok(())
    }
}

mod result_outcome {
    use std::error::Error;
    use std::fmt;
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::Order;

    // --8<-- [start:result_outcome]
    #[derive(Debug)]
    struct UnknownSku;

    impl fmt::Display for UnknownSku {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("unknown sku")
        }
    }

    impl Error for UnknownSku {}

    fn stock_of(sku: &str) -> Result<u32, UnknownSku> {
        match sku {
            "lamp" => Ok(3),
            "vase" => Ok(0),
            _ => Err(UnknownSku),
        }
    }

    /// `Ok` carries the settlement the body chose, and `Err` drops.
    #[subscriber("orders")]
    async fn ship(order: &Order) -> Result<HandlerOutcome, UnknownSku> {
        if stock_of(&order.sku)? == 0 {
            // Restocking takes an hour: ask for the order back then.
            return Ok(HandlerOutcome::retry_after(Duration::from_secs(3600)));
        }
        Ok(HandlerOutcome::ack())
    }
    // --8<-- [end:result_outcome]

    fn app() -> impl App {
        RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(ship);
        })
    }

    #[tokio::test]
    async fn the_inner_outcome_settles_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for sku in ["lamp", "vase", "sofa"] {
            tb.message(&Order::new(1, sku)).publish().await?;
        }

        // The vase waits for its hour; the unknown sofa is dropped.
        assert_eq!(
            tb.broker::<MemoryBroker>().subscriber("orders").outcomes(),
            [Outcome::Ack, Outcome::Nack, Outcome::Drop],
        );
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
