//! OpenTelemetry trace-context propagation, OTLP export and per-handler dispatch metrics.
//!
//! Re-exported as [`ruststream::otel`](https://docs.rs/ruststream/latest/ruststream/otel/).

pub mod export;
pub mod propagation;

pub use export::{
    Otel, OtelBuilder, OtelConsumeHandler, OtelConsumeLayer, OtelInitError, OtelPublishLayer,
    PUBLISH_TIME_HEADER,
};
pub use propagation::{OpenTelemetry, OpenTelemetryHandler, OpenTelemetryLayer, TracePropagation};
