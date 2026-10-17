//! Starting and stopping the service under test, against services written without the `macros`
//! feature. The test bodies are the ones of `examples/testing_starting.rs`: the harness starts and
//! stops the same app whichever way the service was declared.
//!
//! ```text
//! cargo test --example manual_testing_starting --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

mod start {
    use std::convert::Infallible;
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    /// What the service loads before it subscribes: the orders it refuses.
    struct Blocklist {
        refused: Vec<u64>,
    }

    struct Screen;

    impl Handle<Order, (), (), (), Blocklist> for Screen {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            ctx: &mut Context<'_, (), Blocklist>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            if ctx.state().refused.contains(&order.id) {
                return ready(Err(HandlerOutcome::drop()));
            }
            ready(Ok(()))
        }
    }

    fn app() -> impl App<State = Blocklist> {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(Blocklist { refused: vec![13] }))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(subscriber("orders", Screen).build());
            })
    }

    // --8<-- [start:start]
    #[tokio::test]
    async fn a_refused_order_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 13 })
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
    // --8<-- [end:start]
}

mod start_live {
    use std::error::Error;
    use std::future::{Future, ready};
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            if order.id == 0 {
                return ready(Err(HandlerOutcome::drop()));
            }
            ready(Ok(()))
        }
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
    }

    /// One body for every mode: only the start call differs.
    async fn accepts_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_an_order(TestApp::start(app()).await?).await
    }

    // --8<-- [start:start-live]
    #[tokio::test]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        accepts_an_order(TestApp::start_live(app()).await?).await
    }

    #[tokio::test]
    async fn against_a_distant_stand() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start_live_within(app(), Duration::from_secs(30)).await?;
        accepts_an_order(tb).await
    }
    // --8<-- [end:start-live]
}

mod with_state {
    use std::collections::HashMap;
    use std::error::Error;

    use ruststream::memory::MemoryPublisher;
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    #[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
    struct Quote {
        order_id: u64,
        cents: u32,
    }

    // `#[outgoing(name = "quotes")]` by hand: the type fixes its destination.
    impl OutgoingDestination for Quote {
        type Form = FixedName;

        const DESTINATION: &'static str = "quotes";
    }

    impl MessageHeaders for Quote {
        type Contract = NoHeaders;
    }

    #[derive(Debug, thiserror::Error)]
    #[error("the catalog is unreachable")]
    struct CatalogError;

    /// The prices, read from the catalog service at startup.
    struct Catalog {
        prices: HashMap<u64, u32>,
    }

    impl Catalog {
        async fn load(url: &str) -> Result<Self, CatalogError> {
            // Stands in for the network call to `url`. No catalog runs beside this example, so the
            // call fails, and so would a test that ran this `on_startup`.
            let _ = url;
            tokio::task::yield_now().await;
            Err(CatalogError)
        }
    }

    struct Pricing {
        catalog: Catalog,
        quotes: MemoryPublisher,
    }

    struct Price;

    impl Handle<Order, (), (), (), Pricing> for Price {
        async fn handle(
            &self,
            order: &Order,
            _outs: &(),
            ctx: &mut Context<'_, (), Pricing>,
        ) -> Result<(), HandlerOutcome> {
            let pricing = ctx.state();
            let Some(&cents) = pricing.catalog.prices.get(&order.id) else {
                return Err(HandlerOutcome::drop());
            };
            let quote = Quote {
                order_id: order.id,
                cents,
            };
            if pricing.quotes.message(&quote).publish().await.is_err() {
                return Err(HandlerOutcome::retry());
            }
            Ok(())
        }
    }

    fn app() -> impl App<State = Pricing> {
        let broker = MemoryBroker::new();
        let quotes = broker.publisher();
        RustStream::new(AppInfo::new("pricing", "0.1.0"))
            .on_startup(async move |()| {
                let catalog = Catalog::load("http://catalog.internal").await?;
                Ok::<_, CatalogError>(Pricing { catalog, quotes })
            })
            .with_broker(broker, |b| {
                b.include(subscriber("orders", Price).build());
            })
    }

    // --8<-- [start:with-state]
    #[tokio::test]
    async fn an_order_is_quoted_from_the_catalog() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |brokers| Pricing {
            catalog: Catalog {
                prices: HashMap::from([(7, 1250)]),
            },
            quotes: brokers.broker::<MemoryBroker>().publisher(),
        })
        .await?;
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .published::<Quote>("quotes")
            .assert_called_once()
            .with(&Quote {
                order_id: 7,
                cents: 1250,
            });
        Ok(())
    }
    // --8<-- [end:with-state]
}

mod shutdown {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::memory::prelude::*;
    use ruststream::runtime::RustStreamError;
    use ruststream::testing::TestApp;

    use super::Order;

    struct Fulfil;

    impl Handle<Order> for Fulfil {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            assert_ne!(order.id, 0, "an order without an id reached fulfilment");
            ready(Ok(()))
        }
    }

    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Fulfil).build());
        })
    }

    // --8<-- [start:shutdown]
    #[tokio::test]
    async fn a_healthy_run_shuts_down_cleanly() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_panic_is_reported_at_shutdown() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 0 })
            .publish()
            .await?;

        assert!(matches!(
            tb.shutdown().await,
            Err(RustStreamError::Dispatch(_))
        ));
        Ok(())
    }
    // --8<-- [end:shutdown]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
