//! Colored, `RUST_LOG`-driven console logging built on
//! [`tracing_subscriber`](https://docs.rs/tracing-subscriber/0.3/tracing_subscriber/) (`logging`
//! feature).
//!
//! - [Logging][logging]: [`init`] installs the console logger, and [`Logging`] tunes its
//!   defaults; the generated `run` command calls [`init`] itself.
//! - [TracingLayer][tracing-layer]: [`TracingLayer`](crate::runtime::layers::TracingLayer) logs
//!   every delivery and its outcome as `tracing` events.
//!
//! [logging]: https://powersemmi.github.io/ruststream/latest/components/observability/logging/
//! [tracing-layer]: https://powersemmi.github.io/ruststream/latest/components/observability/tracing-layer/

#[doc(inline)]
pub use ruststream_observability::logging::*;
