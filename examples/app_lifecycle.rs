//! Lifecycle hooks: a resource opened before the brokers connect, used while the service runs, and
//! closed after the brokers shut down; and a broker scope that announces the service once its
//! subscriptions are open. Each scenario is a test.
//!
//! ```text
//! cargo test --example app_lifecycle --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

mod hooks {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use ruststream::memory::prelude::*;

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

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
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
                b.include(accept);
            })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_hooks_bracket_the_run() -> Result<(), Box<dyn Error>> {
        let log = Arc::new(Mutex::new(Vec::new()));
        let running = app(Arc::clone(&log)).start().await?;
        assert_eq!(*log.lock().map_err(|e| e.to_string())?, ["open", "ready"]);

        running.shutdown().await?;
        assert_eq!(
            *log.lock().map_err(|e| e.to_string())?,
            ["open", "ready", "stopping", "closed"]
        );
        Ok(())
    }
    // --8<-- [end:hooks]
}

mod announce {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::Order;

    // --8<-- [start:announce]
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "services.started")]
    struct Started {
        service: String,
    }

    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
            // Runs once the subscriptions are open, with a publisher of this broker.
            b.after_startup(Publish, async move |publisher| {
                let started = Started {
                    service: "orders".to_owned(),
                };
                publisher.message(&started).publish().await
            });
        })
    }

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
    // --8<-- [end:announce]
}

fn main() {
    println!("the scenarios are tests: cargo test --example app_lifecycle");
}
