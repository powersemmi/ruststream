//! The OpenTelemetry example written without the `macros` feature: the handler is a named type
//! that reads the business instruments off the context, and the entry point is the same
//! hand-written `main` the original already needs for the final flush.
//!
//! ```text
//! cargo run --example manual_otel_export --no-default-features --features otel,memory,json -- run
//! ```
//!
//! Point `otlp_endpoint` at a running collector (`docker run -p 4317:4317 otel/opentelemetry-collector`)
//! to see the spans and metrics arrive; without one the exporter retries quietly in the
//! background while the service keeps working.

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use ruststream::memory::prelude::*;
use ruststream::otel::Otel;
use ruststream::runtime::cli::run_main;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
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

/// `#[derive(FromRef)]` exists so a `State<OrderMetrics>` parameter can project this field out
/// of the app state. A hand-written handler reads the state whole off the context, so there is
/// no projection to declare and no derive to replace.
#[derive(Clone)]
struct AppState {
    metrics: OrderMetrics,
}

struct Accept;

impl Handle<Order, (), (), (), AppState> for Accept {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ctx.state()
            .metrics
            .accepted
            .add(1, &[KeyValue::new("region", "eu")]);
        // Deserialized for schema realism; the example only counts orders.
        let _ = order.id;
        ready(Ok(()))
    }
}

// The typed `AppState` is the last axis of the body's own `impl Handle`, so `subscriber(source,
// body)` mounts it unchanged: the mount reads that state off the impl and checks it against the
// app's.
// --8<-- [end:business_metric]

// --8<-- [start:init]
// `use<>` opts out of capturing the `otel` borrow: the layers `Arc`-share the instruments, so
// the built app owns its half and `main` keeps the other for the final flush.
fn app(otel: &Otel) -> impl App + use<> {
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
            b.include(subscriber("orders", Accept).build());
        })
}

// The entry point is hand-written with or without the attribute: the macro's generated main ends
// at run_main, and the exporters batch in the background, so flushing them needs one call after
// the app has drained.
fn main() -> ExitCode {
    // Installs the global tracer + meter providers, the OTLP exporters, and the tracing bridge;
    // every span the propagation module opens and every metric the service records now exports.
    let otel = Otel::builder()
        .service_name("orders-svc")
        .otlp_endpoint("http://localhost:4317")
        .messaging_system("memory")
        .attribute("deployment.environment", "dev")
        .init()
        .expect("otel init failed");

    let code = run_main(|| app(&otel));
    // Ships the last buffered spans and metric points to the collector; dropping `Otel` does not.
    otel.shutdown().expect("otel shutdown failed");
    code
}
// --8<-- [end:init]
