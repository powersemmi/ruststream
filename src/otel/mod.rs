//! OpenTelemetry integration (`otel` feature): W3C trace-context propagation, OTLP export, and
//! per-handler dispatch metrics.
//!
//! One story in two submodules:
//!
//! - [`propagation`] carries the W3C trace context across brokers and opens plain `tracing`
//!   spans per delivery. It needs no SDK setup at all: [`OpenTelemetry::consume_layer`] and
//!   [`OpenTelemetry::propagation`] work with whatever `tracing` subscriber the app installs.
//! - [`export`] is the SDK half: [`Otel::builder`] builds OTLP exporters, installs the tracer
//!   and meter providers as the process globals, bridges `tracing` spans into them, and hands
//!   out the dispatch-metrics middleware pair ([`Otel::consume_layer`] / [`Otel::publish_layer`]).
//!
//! Every public item is re-exported here; the submodules organize the source, not the API.
//!
//! # Propagation
//!
//! The consume layer goes on the application, the propagation transform on the reply wiring.
//! For each delivery the layer reads the incoming `traceparent`, opens a span for the handler
//! and records the consumer's context on the working headers; the transform copies
//! `traceparent` and `tracestate` onto every reply, so a downstream service sees the consumer
//! span as the reply's parent. A delivery without `traceparent` starts a sampled root trace. A
//! handler reads the working value like any header, `ctx.headers().get_str("traceparent")`,
//! and `for_batch(otel.propagation())` puts the same transform on a batch reply.
//!
//! ```
//! # #[cfg(all(feature = "otel", feature = "macros", feature = "memory", feature = "json"))]
//! # mod demo {
//! use ruststream::memory::prelude::*;
//! use ruststream::otel::OpenTelemetry;
//! use serde::{Deserialize, Serialize};
//!
//! # #[derive(Deserialize)]
//! # struct Request {
//! #     id: u64,
//! # }
//! # #[derive(Serialize, Outgoing)]
//! # struct Response {
//! #     ok: bool,
//! # }
//! #[subscriber("requests", publish("responses"))]
//! async fn respond(req: &Request) -> Response {
//!     Response { ok: req.id != 0 }
//! }
//!
//! fn app() -> impl App {
//!     let otel = OpenTelemetry::new();
//!     RustStream::new(AppInfo::new("responder", "0.1.0"))
//!         .layer(otel.consume_layer())
//!         .with_broker(MemoryBroker::new(), |b| {
//!             b.include(respond)
//!                 .out_reply(Publish)
//!                 .transform(otel.propagation());
//!         })
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! # Export and the metrics inventory
//!
//! [`OtelBuilder::init`] builds the OTLP exporters, installs the tracer and meter providers as
//! the process globals and turns on the `tracing` bridge, so the spans propagation opens are
//! exported with no further wiring. Two more middleware record the dispatch metrics,
//! [`Otel::consume_layer`] and [`Otel::publish_layer`]. The instruments are labelled per handler
//! (`messaging.destination.name`) and named by the messaging semantic conventions or in the
//! `ruststream.*` namespace:
//!
//! | Instrument | Kind | What it measures |
//! |---|---|---|
//! | `messaging.client.consumed.messages` | counter | deliveries received |
//! | `messaging.process.duration` | histogram | handler processing time |
//! | `ruststream.messages.processed` | counter, `outcome` attribute | settlements: `ack`, `nack_requeue`, `nack_drop`, `retry_after` |
//! | `ruststream.messages.in_flight` | up-down counter | deliveries inside handlers |
//! | `ruststream.message.queue_time` | histogram | publish-to-handler lag, from the stamped publish-time header |
//! | `ruststream.messages.decode_failures` | counter | payloads the codec rejected |
//! | `ruststream.messages.panics` | counter | handler invocations that panicked |
//! | `messaging.client.sent.messages` | counter, `error.type` on failure | publishes |
//! | `messaging.client.operation.duration` | histogram | the publish operation |
//! | `ruststream.message.payload.size` | histogram | published payload sizes |
//! | `ruststream.batch.size` | histogram | decoded batch sizes handed to batch handlers |
//! | `ruststream.app.state` | observable gauge | the lifecycle state, from [`Otel::observe_health`] |
//!
//! Business instruments need no wiring of their own: build them once at startup against the
//! global meter, share them through the typed state, and they export through the same pipeline.
//! Call [`Otel::shutdown`] after the app's graceful shutdown to flush the last spans and points.
//! [`OtelBuilder::tracing_bridge`] leaves the bridge to your own subscriber stack, and
//! [`OtelBuilder::messaging_system`] stamps the system attribute the broker-agnostic core cannot
//! derive. A ready-made Grafana dashboard over this inventory lives in
//! [`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana), and
//! `examples/otel_export.rs` in the repository is the feature end to end.

pub mod export;
pub mod propagation;

pub use export::{
    Otel, OtelBuilder, OtelConsumeHandler, OtelConsumeLayer, OtelInitError, OtelPublishLayer,
    PUBLISH_TIME_HEADER,
};
pub use propagation::{OpenTelemetry, OpenTelemetryHandler, OpenTelemetryLayer, TracePropagation};
