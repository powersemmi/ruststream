//! The services of `examples/app_lifecycle.rs` written without the `macros` feature. The hooks are
//! the same builder calls; only the handler and the message types are written by hand.
//!
//! ```text
//! cargo test --example manual_app_lifecycle --no-default-features --features testing,memory,json
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

mod hooks {
    use std::convert::Infallible;
    use std::error::Error;
    use std::future::ready;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:hooks]
    /// A stand-in for a connection pool: it records what the service did with it.
    struct Pool {
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Pool {
        fn open(log: Arc<Mutex<Vec<&'static str>>>) -> Self {
            let pool = Self { log };
            pool.record("open");
            pool
        }

        fn record(&self, event: &'static str) {
            if let Ok(mut log) = self.log.lock() {
                log.push(event);
            }
        }
    }

    struct Accept;

    // The handler names the app's state type as the last parameter; the mount checks it.
    impl Handle<Order, (), (), (), Pool> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_, (), Pool>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "accepted");
            ready(Ok(()))
        }
    }

    fn app(log: Arc<Mutex<Vec<&'static str>>>) -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            // Before the brokers connect: the value it produces is the application state.
            .on_startup(async move |()| Ok::<_, Infallible>(Pool::open(log)))
            // Once every subscription is open.
            .after_startup(async move |pool: Arc<Pool>| {
                pool.record("ready");
                Ok::<_, Infallible>(())
            })
            // When shutdown begins, while the brokers are still connected.
            .on_shutdown(async move |pool: Arc<Pool>| {
                pool.record("stopping");
                Ok::<_, Infallible>(())
            })
            // After the brokers have shut down.
            .after_shutdown(async move |pool: Arc<Pool>| {
                pool.record("closed");
                Ok::<_, Infallible>(())
            })
            // How long shutdown waits for handlers still running.
            .shutdown_timeout(Duration::from_secs(10))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(subscriber("orders", Accept).build());
            })
    }
    // --8<-- [end:hooks]

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_hooks_bracket_the_run() -> Result<(), Box<dyn Error>> {
        let log = Arc::new(Mutex::new(Vec::new()));
        let tb = TestApp::start(app(Arc::clone(&log))).await?;
        assert_eq!(*log.lock().map_err(|e| e.to_string())?, ["open", "ready"]);

        tb.shutdown().await?;
        assert_eq!(
            *log.lock().map_err(|e| e.to_string())?,
            ["open", "ready", "stopping", "closed"]
        );
        Ok(())
    }
}

mod announce {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    // --8<-- [start:announce]
    #[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
    struct Started {
        service: String,
    }

    // `#[outgoing(name = "services.started")]` by hand: the type fixes its destination.
    impl OutgoingDestination for Started {
        type Form = FixedName;

        const DESTINATION: &'static str = "services.started";
    }

    impl MessageHeaders for Started {
        type Contract = NoHeaders;
    }

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
            // Runs once the subscriptions are open, with a publisher of this broker.
            b.after_startup(Publish, async move |publisher| {
                let started = Started {
                    service: "orders".to_owned(),
                };
                publisher.message(&started).publish().await
            });
        })
    }
    // --8<-- [end:announce]

    #[tokio::test]
    async fn the_service_announces_itself() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MemoryBroker>()
            .published::<Started>("services.started")
            .assert_called_once()
            .with(&Started {
                service: "orders".to_owned(),
            });
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
