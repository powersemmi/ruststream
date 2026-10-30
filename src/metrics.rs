//! Prometheus metrics for the consume and publish paths (`metrics` feature).
//!
//! - [Prometheus][prometheus]: one [`Metrics`] collector hands out a consume [`Layer`](crate::runtime::Layer) and a
//!   [`PublishLayer`](crate::runtime::PublishLayer) over one
//!   [`Registry`](https://docs.rs/prometheus/0.14/prometheus/struct.Registry.html), and [`Metrics::export`] renders it for a
//!   `/metrics` route; [`Metrics`] lists the series.
//!
//! [prometheus]: https://powersemmi.github.io/ruststream/latest/components/observability/metrics/

#[doc(inline)]
pub use ruststream_observability::metrics::*;
