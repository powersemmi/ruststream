//! Prometheus metrics for consume and publish paths.
//!
//! A single [`Metrics`] object owns the counters and the [`Registry`] they are
//! registered in, and hands out two middleware: a static consume-side [`Layer`] and a publish-side
//! [`PublishLayer`]. Both share the same registry, so one [`Metrics::export`] renders the whole
//! picture. The registry is the global default unless you pass your own.
//!
//! HTTP exposition is the user's concern: call [`export`](Metrics::export) and serve the string from
//! your own axum / actix / hyper handler, or push it to a gateway.
//!
//! # Examples
//!
//! ```
//! use ruststream::metrics::Metrics;
//! use ruststream::runtime::{AppInfo, RustStream};
//!
//! # fn build() -> Result<(), prometheus::Error> {
//! let metrics = Metrics::new()?;
//! let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
//!     .layer(metrics.consume_layer())
//!     .publish_layer(metrics.publish_layer());
//! # let _ = app;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};

use crate::runtime::{
    BlanketLayer, Context, Handler, HandlerOutcome, HandlerResult, Layer, Outgoing, PublishLayer,
    PublishNext, PublishPipeline,
};

/// Default histogram buckets (seconds) for handler duration.
const DURATION_BUCKETS: &[f64] = &[
    0.000_5, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

struct Inner {
    registry: Registry,
    consumed: IntCounterVec,
    consume_duration: HistogramVec,
    published: IntCounterVec,
}

/// Collects consume and publish metrics into a shared Prometheus registry.
///
/// Cheap to clone (shares one registry and counter set). Hand [`consume_layer`](Self::consume_layer)
/// to [`RustStream::layer`](crate::runtime::RustStream::layer) and
/// [`publish_layer`](Self::publish_layer) to
/// [`RustStream::publish_layer`](crate::runtime::RustStream::publish_layer).
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    /// Creates a metrics collector registered in the global default registry.
    ///
    /// # Errors
    ///
    /// Returns an error if a metric with one of the same names is already registered (for example,
    /// constructing two `Metrics` on the global registry).
    pub fn new() -> Result<Self, prometheus::Error> {
        Self::with_registry(prometheus::default_registry().clone())
    }

    /// Creates a metrics collector registered in `registry`.
    ///
    /// # Errors
    ///
    /// Returns an error if one of the metric names is already registered in `registry`.
    pub fn with_registry(registry: Registry) -> Result<Self, prometheus::Error> {
        let consumed = IntCounterVec::new(
            Opts::new(
                "ruststream_messages_consumed_total",
                "Messages handled, by name and outcome.",
            ),
            &["name", "status"],
        )?;
        let consume_duration = HistogramVec::new(
            HistogramOpts::new(
                "ruststream_consume_duration_seconds",
                "Handler execution time, by name.",
            )
            .buckets(DURATION_BUCKETS.to_vec()),
            &["name"],
        )?;
        let published = IntCounterVec::new(
            Opts::new(
                "ruststream_messages_published_total",
                "Messages published, by name and outcome.",
            ),
            &["name", "status"],
        )?;

        registry.register(Box::new(consumed.clone()))?;
        registry.register(Box::new(consume_duration.clone()))?;
        registry.register(Box::new(published.clone()))?;

        Ok(Self {
            inner: Arc::new(Inner {
                registry,
                consumed,
                consume_duration,
                published,
            }),
        })
    }

    /// The registry the metrics are registered in.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.inner.registry
    }

    /// A static consume-side [`Layer`] that times each handler and counts its outcome.
    #[must_use]
    pub fn consume_layer(&self) -> MetricsLayer {
        MetricsLayer {
            inner: Arc::clone(&self.inner),
        }
    }

    /// A publish-side [`PublishLayer`] that counts each published message.
    #[must_use]
    pub fn publish_layer(&self) -> MetricsPublish {
        MetricsPublish {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Renders the registry in the Prometheus text exposition format.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying encoder fails to write the gathered metric families.
    pub fn export(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        encoder.encode(&self.inner.registry.gather(), &mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }
}

const fn consume_status(result: HandlerResult) -> &'static str {
    match result {
        HandlerResult::Ack => "ack",
        // A delayed nack is still a nack for counting purposes.
        HandlerResult::Nack { .. } | HandlerResult::NackAfter { .. } => "nack",
    }
}

/// The [`Layer`] handed out by [`Metrics::consume_layer`]. Wraps a handler with timing and counters.
#[derive(Clone)]
pub struct MetricsLayer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for MetricsLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsLayer").finish_non_exhaustive()
    }
}

impl<H> Layer<H> for MetricsLayer {
    type Handler = MetricsHandler<H>;

    fn layer(&self, inner: H) -> Self::Handler {
        MetricsHandler {
            inner,
            metrics: Arc::clone(&self.inner),
        }
    }
}

// Lets the consume metric ride the application-wide stack and reach handlers mounted through a
// router (whose concrete types the router hides), the same way [`TracingLayer`] does.
impl BlanketLayer for MetricsLayer {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        self.layer(handler)
    }
}

/// The handler produced by [`MetricsLayer::layer`].
#[derive(Clone)]
pub struct MetricsHandler<H> {
    inner: H,
    metrics: Arc<Inner>,
}

impl<H> std::fmt::Debug for MetricsHandler<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsHandler").finish_non_exhaustive()
    }
}

impl<M, C, S, H> Handler<M, C, S> for MetricsHandler<H>
where
    M: Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    fn handle(
        &self,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        let name = ctx.name().to_owned();
        async move {
            let started = std::time::Instant::now();
            // Classify by the outcome inside the settlement; the continuation (if any) passes
            // through untouched to the dispatcher.
            let settle = self.inner.handle(msg, ctx).await;
            self.metrics
                .consume_duration
                .with_label_values(&[name.as_str()])
                .observe(started.elapsed().as_secs_f64());
            self.metrics
                .consumed
                .with_label_values(&[name.as_str(), consume_status(settle.outcome())])
                .inc();
            settle
        }
    }
}

/// The [`PublishLayer`] handed out by [`Metrics::publish_layer`].
#[derive(Clone)]
pub struct MetricsPublish {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for MetricsPublish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsPublish").finish_non_exhaustive()
    }
}

impl PublishLayer for MetricsPublish {
    fn on_publish<'a, N: PublishPipeline, P: crate::Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a
    {
        let name = out.name().to_owned();
        async move {
            let result = next.run(out).await;
            let status = if result.is_ok() { "ok" } else { "error" };
            self.inner
                .published
                .with_label_values(&[name.as_str(), status])
                .inc();
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use prometheus::Registry;

    use super::{Metrics, consume_status};
    use crate::runtime::{Context, HandlerOutcome, HandlerResult, Layer};

    #[test]
    fn debug_impls_registry_and_status_mapping() {
        let metrics = Metrics::with_registry(Registry::new()).unwrap();
        assert!(format!("{metrics:?}").contains("Metrics"));
        assert!(format!("{:?}", metrics.consume_layer()).contains("MetricsLayer"));
        assert!(format!("{:?}", metrics.publish_layer()).contains("MetricsPublish"));

        let handler = metrics
            .consume_layer()
            .layer(|_: &u32, _: &mut Context| async { HandlerOutcome::ack() });
        assert!(format!("{handler:?}").contains("MetricsHandler"));

        // registry() exposes the registry the three collectors were registered in: registering a
        // duplicate name there fails, which proves it is that same registry.
        let dup = prometheus::IntCounter::new("ruststream_messages_consumed_total", "dup").unwrap();
        assert!(metrics.registry().register(Box::new(dup)).is_err());

        // Every outcome maps to its counter label.
        assert_eq!(consume_status(HandlerResult::Ack), "ack");
        assert_eq!(consume_status(HandlerResult::drop()), "nack");
        assert_eq!(
            consume_status(HandlerResult::retry_after(Duration::from_secs(1))),
            "nack"
        );
    }
}
