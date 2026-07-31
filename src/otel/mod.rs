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

pub mod export;
pub mod propagation;

pub use export::{
    Otel, OtelBuilder, OtelConsumeHandler, OtelConsumeLayer, OtelInitError, OtelPublishLayer,
    PUBLISH_TIME_HEADER,
};
pub use propagation::{OpenTelemetry, OpenTelemetryHandler, OpenTelemetryLayer, TracePropagation};
