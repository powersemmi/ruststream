//! A service started in the background with `start` and stopped by its handle, and the same
//! service run with `run_until` until a future of the caller resolves. Each scenario is a test.
//!
//! ```text
//! cargo test --example app_running --features testing,macros,memory,json
//! ```

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

mod start {
    use std::error::Error;
    use std::pin::pin;
    #[cfg(unix)]
    use std::process;
    #[cfg(unix)]
    use std::process::Command;
    use std::time::Duration;

    use futures::StreamExt;
    use ruststream::Subscriber;
    use ruststream::memory::MemorySubscriber;
    use ruststream::memory::prelude::*;
    use ruststream::runtime::{HealthState, SubscriptionEnd};
    #[cfg(unix)]
    use tokio::signal::unix;
    #[cfg(unix)]
    use tokio::signal::unix::SignalKind;
    #[cfg(unix)]
    use tokio::task::{LocalSet, spawn_local};
    use tokio::time::timeout;

    use super::{Order, Receipt};

    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    // --8<-- [start:subscription_end]
    /// The service exists to answer orders, so a subscription whose stream ends while the
    /// service runs stops the service.
    fn app(broker: MemoryBroker) -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .on_subscription_end(SubscriptionEnd::FailFast)
            .with_broker(broker, |b| b.include(issue_receipt).out_reply(Publish))
    }
    // --8<-- [end:subscription_end]

    // --8<-- [start:run]
    /// Runs the service until `SIGINT` or `SIGTERM`, then shuts it down gracefully.
    async fn serve_forever(broker: MemoryBroker) -> Result<(), Box<dyn Error>> {
        app(broker).run().await?;
        Ok(())
    }
    // --8<-- [end:run]

    // --8<-- [start:run_until]
    /// Runs the service until `work` resolves; `work` starts once the service has started.
    async fn serve_until(
        broker: MemoryBroker,
        work: impl Future<Output = ()> + Send,
    ) -> Result<(), Box<dyn Error>> {
        app(broker).run_until(work).await?;
        Ok(())
    }
    // --8<-- [end:run_until]

    // --8<-- [start:start]
    /// Starts the service beside other work of the process, and stops it once that work is done.
    async fn serve(
        broker: MemoryBroker,
        work: impl Future<Output = ()> + Send,
    ) -> Result<(), Box<dyn Error>> {
        // `start` returns once every subscription is open.
        let running = app(broker).start().await?;
        work.await;
        running.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:start]

    // --8<-- [start:shutdown]
    /// Stops a started service: the shutdown hooks run and the handlers in flight finish first.
    async fn stop(running: RunningApp) -> Result<(), Box<dyn Error>> {
        let health = running.health();
        running.shutdown().await?;
        tracing::info!(state = ?health.state(), "stopped");
        Ok(())
    }
    // --8<-- [end:shutdown]

    /// The next receipt a downstream reader sees, or `None` after five seconds.
    async fn next_receipt(receipts: &mut MemorySubscriber) -> Option<Receipt> {
        let mut stream = pin!(receipts.stream());
        match timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(delivery))) => serde_json::from_slice(delivery.payload()).ok(),
            _ => None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serve_stops_the_service_after_the_work() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let mut receipts = broker.subscribe("receipts");
        let orders = broker.publisher();

        let mut receipt = None;
        let work = async {
            let sent = orders
                .message(&Order { id: 7 })
                .to("orders")
                .publish()
                .await;
            if sent.is_ok() {
                receipt = next_receipt(&mut receipts).await;
            }
        };
        serve(broker, work).await?;

        assert_eq!(receipt, Some(Receipt { order_id: 7 }));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_runs_the_service_beside_the_caller() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let mut receipts = broker.subscribe("receipts");
        let orders = broker.publisher();

        // `start` returns once every subscription is open.
        let running = app(broker).start().await?;
        let health = running.health();
        assert_eq!(health.state(), HealthState::Running);

        orders
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;
        assert_eq!(
            next_receipt(&mut receipts).await,
            Some(Receipt { order_id: 7 })
        );

        stop(running).await?;
        assert_eq!(health.state(), HealthState::Stopped);
        Ok(())
    }

    /// Sends `SIGTERM` to this process through the shell (the example takes no libc dependency).
    #[cfg(unix)]
    fn raise_sigterm() -> Result<(), Box<dyn Error>> {
        let status = Command::new("sh")
            .arg("-c")
            .arg(format!("kill -TERM {}", process::id()))
            .status()?;
        assert!(status.success(), "kill -TERM failed: {status:?}");
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_serves_until_a_termination_signal() -> Result<(), Box<dyn Error>> {
        // The run is not `Send`, so it is driven on the test's own thread.
        LocalSet::new().run_until(drive_until_signalled()).await
    }

    #[cfg(unix)]
    async fn drive_until_signalled() -> Result<(), Box<dyn Error>> {
        // A listener registered first replaces the default disposition, so a signal that lands
        // before `run` installs its own handler cannot kill the test binary.
        let _guard = unix::signal(SignalKind::terminate())?;
        let broker = MemoryBroker::new();
        let mut receipts = broker.subscribe("receipts");
        let orders = broker.publisher();

        let mut run = spawn_local(async move {
            serve_forever(broker)
                .await
                .map_err(|error| error.to_string())
        });
        // The service answers before the signal: `run` has started it. An order published
        // before the subscription opens reaches no one, so it is sent again until one is answered.
        let receipt = timeout(Duration::from_secs(30), async {
            loop {
                orders
                    .message(&Order { id: 7 })
                    .to("orders")
                    .publish()
                    .await?;
                let answered = timeout(Duration::from_millis(100), next_receipt(&mut receipts));
                if let Ok(Some(receipt)) = answered.await {
                    return Ok::<_, Box<dyn Error>>(receipt);
                }
            }
        })
        .await??;
        assert_eq!(
            receipt,
            Receipt { order_id: 7 },
            "the order is answered before the signal"
        );

        // `run` installs its signal handler after startup and nothing reports the moment, so the
        // signal is re-sent until the run returns.
        let stopped = timeout(Duration::from_secs(30), async {
            loop {
                raise_sigterm()?;
                if let Ok(joined) = timeout(Duration::from_millis(50), &mut run).await {
                    return Ok::<_, Box<dyn Error>>(joined?);
                }
            }
        })
        .await??;
        assert_eq!(stopped, Ok(()), "the signalled shutdown must be graceful");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_until_stops_when_the_future_resolves() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let mut receipts = broker.subscribe("receipts");
        let orders = broker.publisher();

        // The future runs once the service has started; the service stops when it resolves.
        let mut receipt = None;
        let first_receipt = async {
            let sent = orders
                .message(&Order { id: 7 })
                .to("orders")
                .publish()
                .await;
            if sent.is_ok() {
                receipt = next_receipt(&mut receipts).await;
            }
        };
        serve_until(broker, first_receipt).await?;

        assert_eq!(receipt, Some(Receipt { order_id: 7 }));
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
