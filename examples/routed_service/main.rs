//! A production-shaped RustStream service, assembled from focused modules:
//!
//! - [`domain`] - the wire messages (each derives `JsonSchema` + `Message` for AsyncAPI), the
//!   service error type, and a shared repository.
//! - [`orders`] - a publishing handler that replies and handles transient failures, and a
//!   cancellation handler that retries.
//! - [`payments`] - a charge handler across keyed worker lanes, and a transactional batch settler.
//! - [`observability`] - an application-wide log/timing middleware.
//! - [`routes`] - wires the handlers into two routers, each carrying the metrics consume layer.
//!
//! It exercises the runtime end to end: shared state opened in a startup hook and closed on
//! shutdown, retries (`HandlerResult::retry` / `retry_after`), keyed worker lanes, a transactional
//! batch publisher, app-scope middleware, and Prometheus metrics on both the consume and publish
//! sides. `#[ruststream::app]` generates `main`, giving the service a CLI:
//!
//! ```text
//! # start the service (waits for ctrl-c; logs each delivery, dumps metrics on shutdown)
//! cargo run --example routed_service --features macros,memory,json,metrics,asyncapi,logging -- run
//!
//! # generate the AsyncAPI document for its four channels
//! cargo run --example routed_service --features macros,memory,json,metrics,asyncapi,logging \
//!     -- asyncapi gen --yaml
//! ```

mod domain;
mod observability;
mod orders;
mod payments;
mod routes;

use std::time::Duration;

use ruststream::ServerSpec;
use ruststream::memory::MemoryBroker;
use ruststream::metrics::{Metrics, MetricsPublish};
use ruststream::runtime::{AppInfo, Identity, PublishCons, PublishEnd, RustStream, Stack};

use std::sync::Arc;

use crate::domain::{Repository, ServiceError};
use crate::observability::Observe;

/// Builds the service. The layer stack and the typed application state are both named in the return
/// type: `Observe` wraps every handler, including the router-mounted ones (it is a `BlanketLayer`),
/// with `Identity` as the base; `Repository` is the shared state `on_startup` produces.
#[ruststream::app]
fn app() -> RustStream<Stack<Observe, Identity>, Repository, PublishCons<MetricsPublish, PublishEnd>>
{
    let metrics = Metrics::new().expect("create metrics registry");
    // A second handle for the shutdown dump; `Metrics` is cheap to clone (shared registry).
    let metrics_dump = metrics.clone();

    RustStream::new(AppInfo::new("orders-service", "0.1.0"))
        // AsyncAPI records the broker this service connects to in production.
        .server(
            "production",
            ServerSpec::new("nats://orders.internal:4222", "nats"),
        )
        // App-wide: log and time every delivery, on every channel and router.
        .layer(Observe)
        // Publish-side metric: counts every reply that leaves the process.
        .publish_layer(metrics.publish_layer())
        // Open the shared repository before brokers connect; the produced value becomes the typed
        // app state, shared with every handler.
        .on_startup(|()| async move { Repository::open().await })
        // Close the repository after brokers stop, then dump the final metrics.
        .after_shutdown(move |repo: Arc<Repository>| async move {
            repo.close().await;
            match metrics_dump.export() {
                Ok(text) => tracing::info!("final metrics:\n{text}"),
                Err(e) => tracing::warn!(error = %e, "could not export metrics"),
            }
            Ok::<(), ServiceError>(())
        })
        // Bound the post-shutdown drain of in-flight handlers.
        .shutdown_timeout(Duration::from_secs(10))
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(routes::orders(b.broker(), &metrics));
            b.include_router(routes::payments(b.broker(), &metrics));
        })
}
