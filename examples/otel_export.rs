//! Export traces and metrics to an OpenTelemetry collector: the `otel` feature end to end.
//!
//! ```text
//! cargo run --example otel_export --features otel,macros,memory,json -- run
//! ```
//!
//! Point `otlp_endpoint` at a running collector (`docker run -p 4317:4317 otel/opentelemetry-collector`)
//! to see the spans and metrics arrive; without one the exporter retries quietly in the
//! background while the service keeps working.

use std::convert::Infallible;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use ruststream::memory::MemoryBroker;
use ruststream::otel::Otel;
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, State};
use ruststream::{FromRef, subscriber};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    #[allow(dead_code)] // Deserialized for schema realism; the example only counts orders.
    id: u64,
}

// --8<-- [start:business_metric]
/// The service's business instruments: one storage object, built once at startup against the
/// global meter `Otel::init` installed, so everything in it rides the same OTLP pipeline as the
/// framework's dispatch metrics.
#[derive(Clone)]
struct OrderMetrics {
    accepted: Counter<u64>,
}

#[derive(Clone, FromRef)]
struct AppState {
    metrics: OrderMetrics,
}

#[subscriber("orders")]
async fn accept(order: &Order, State(metrics): State<OrderMetrics>) -> HandlerResult {
    metrics.accepted.add(1, &[KeyValue::new("region", "eu")]);
    let _ = order;
    HandlerResult::Ack
}
// --8<-- [end:business_metric]

// --8<-- [start:init]
#[ruststream::app]
fn app() -> impl App {
    // Installs the global tracer + meter providers, the OTLP exporters, and the tracing bridge;
    // every span the propagation module opens and every metric below now exports.
    let otel = Otel::builder()
        .service_name("orders-svc")
        .otlp_endpoint("http://localhost:4317")
        .messaging_system("memory")
        .attribute("deployment.environment", "dev")
        .init()
        .expect("otel init failed");

    RustStream::new(AppInfo::new("orders-svc", "0.1.0"))
        // per-delivery metrics: consumed, process duration, outcomes, in-flight, queue time
        .layer(otel.consume_layer())
        // per-publish metrics: sent, operation duration, payload size, queue-time stamp
        .publish_layer(otel.publish_layer())
        // business instruments: built once against the global meter, shared as typed state
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                metrics: OrderMetrics {
                    accepted: global::meter("orders-svc")
                        .u64_counter("orders_accepted")
                        .build(),
                },
            })
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
}
// --8<-- [end:init]
