//! The observability layers of [RustStream](https://docs.rs/ruststream): Prometheus metrics
//! (`metrics`), the `tracing` subscriber a service installs (`logging`) and the OpenTelemetry
//! propagation, export and dispatch metrics (`otel`), each behind its feature.
//!
//! Services use them through the `ruststream` facade, as
//! [`ruststream::metrics`](https://docs.rs/ruststream/latest/ruststream/metrics/),
//! [`ruststream::logging`](https://docs.rs/ruststream/latest/ruststream/logging/) and
//! [`ruststream::otel`](https://docs.rs/ruststream/latest/ruststream/otel/), whose overviews teach
//! them.

#[cfg(any(feature = "metrics", feature = "otel"))]
mod by_name;
#[cfg(feature = "logging")]
pub mod logging;
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "otel")]
pub mod otel;
