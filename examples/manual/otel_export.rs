//! The service of `examples/otel_export.rs` without the `macros` feature: the handler is a named
//! type that reads the business instruments off the context; `main` is the same.
//!
//! ```text
//! cargo run --example manual_otel_export --no-default-features \
//!     --features testing,memory,json,otel -- run
//! cargo test --example manual_otel_export --no-default-features \
//!     --features testing,memory,json,otel
//! ```
//!
//! Point `otlp_endpoint` at a running collector (`docker run -p 4317:4317 otel/opentelemetry-collector`)
//! to see the spans and metrics arrive; without one the exporter retries quietly in the
//! background while the service keeps working.

use std::convert::Infallible;
use std::error::Error;
use std::future::ready;
use std::process::ExitCode;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use ruststream::memory::prelude::*;
use ruststream::otel::Otel;
use ruststream::runtime::cli::run_main;
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

// --8<-- [start:business_metric]
/// The service's own instruments, built once at startup against the global meter, so they
/// export through the same pipeline as the dispatch metrics.
#[derive(Clone)]
struct OrderMetrics {
    accepted: Counter<u64>,
}

// A hand-written handler reads the state whole off the context, so it needs no `FromRef`.
#[derive(Clone)]
struct AppState {
    metrics: OrderMetrics,
}

struct Accept;

// The state is the last axis of `Handle`; the mount checks it against the app's.
impl Handle<Order, (), (), (), AppState> for Accept {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ctx.state()
            .metrics
            .accepted
            .add(1, &[KeyValue::new("region", "eu")]);
        ready(Ok(()))
    }
}
// --8<-- [end:business_metric]

// --8<-- [start:app]
// `use<>`: the app keeps its own handles to the instruments, not a borrow of `otel`.
fn app(otel: &Otel) -> impl App + use<> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Per delivery: consumed, processing time, outcomes, in flight, queue time.
        .layer(otel.consume_layer())
        // Per publish: sent, operation time, payload size, the publish-time stamp.
        .publish_layer(otel.publish_layer())
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                metrics: OrderMetrics {
                    accepted: global::meter("orders")
                        .u64_counter("orders_accepted")
                        .build(),
                },
            })
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build());
        })
}
// --8<-- [end:app]

// --8<-- [start:main]
// Written by hand rather than by `#[ruststream::app]`: the exporters batch in the background,
// so `main` flushes them once the app has drained.
fn main() -> Result<ExitCode, Box<dyn Error>> {
    let otel = Otel::builder()
        .service_name("orders")
        .otlp_endpoint("http://localhost:4317")
        .messaging_system("memory")
        .attribute("deployment.environment", "dev")
        .init()?;

    let code = run_main(|| app(&otel));
    // Ships the last spans and metric points; dropping `Otel` does not.
    otel.shutdown()?;
    Ok(code)
}
// --8<-- [end:main]

// `main` exports to a collector; this test attaches in-memory providers to the same app instead.
#[cfg(test)]
mod tests {
    use std::error::Error;

    use opentelemetry::global;
    use opentelemetry_sdk::metrics::data::{ResourceMetrics, ScopeMetrics};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use ruststream::memory::prelude::*;
    use ruststream::otel::Otel;
    use ruststream::testing::TestApp;

    use super::{Order, app};

    #[tokio::test]
    async fn the_dispatch_and_business_metrics_are_exported() -> Result<(), Box<dyn Error>> {
        let exporter = InMemoryMetricExporter::default();
        let meters = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        // What `init` does for the business instruments: the global meter is this provider.
        global::set_meter_provider(meters.clone());
        let otel = Otel::builder()
            .messaging_system("memory")
            .attach(SdkTracerProvider::default(), meters.clone());

        let tb = TestApp::start(app(&otel)).await?;
        let orders = tb.broker::<MemoryBroker>();
        orders
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;
        orders.subscriber("orders").assert_called_once();

        meters.force_flush()?;
        let batches = exporter.get_finished_metrics()?;
        let names: Vec<String> = batches
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .map(|metric| metric.name().to_owned())
            .collect();
        for name in [
            "messaging.client.consumed.messages",
            "ruststream.messages.processed",
            "orders_accepted",
        ] {
            assert!(
                names.iter().any(|n| n == name),
                "`{name}` not exported: {names:?}"
            );
        }
        Ok(())
    }
}
