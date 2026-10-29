//! OpenTelemetry integration (`otel` feature): W3C trace-context propagation, OTLP export, and
//! per-handler dispatch metrics.
//!
//! Every public item is re-exported here; the two submodules organize the source, not the API.
//!
//! - [Observability][observability]: logs, metrics and traces of a running service, and the
//!   feature behind each.
//! - [Propagation][propagation]: [`OpenTelemetry::consume_layer`] opens a span per delivery and
//!   [`OpenTelemetry::propagation`] carries its trace context onto the replies; no SDK setup.
//! - [Export][export]: [`Otel::builder`] installs the OTLP exporters and the `tracing` bridge,
//!   and [`Otel::consume_layer`] and [`Otel::publish_layer`] record the dispatch metrics listed
//!   on [`Otel`].
//!
//! [observability]: https://powersemmi.github.io/ruststream/latest/components/observability/
//! [propagation]: https://powersemmi.github.io/ruststream/latest/components/observability/otel-propagation/
//! [export]: https://powersemmi.github.io/ruststream/latest/components/observability/otel-export/

#[doc(inline)]
pub use ruststream_observability::otel::*;
