//! Prometheus metrics for a service: the consume and publish layers and the registry they record into.
//!
//! Re-exported as [`ruststream::metrics`](https://docs.rs/ruststream/latest/ruststream/metrics/).

use std::sync::{Arc, OnceLock};
use std::time::Instant;
use std::{error, fmt};

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry,
    TextEncoder,
};

use ruststream_runtime::HandlerResult;
use ruststream_runtime::{
    BlanketLayer, Context, Handler, HandlerOutcome, Layer, Outgoing, PublishLayer, PublishNext,
    PublishPipeline,
};

use crate::by_name::{ByName, CAPACITY};
#[cfg(feature = "poll-diagnostics")]
use poll::PollCollector;
#[cfg(feature = "poll-diagnostics")]
use ruststream_runtime::PollDiagnostics;

/// Default histogram buckets (seconds) for handler duration.
const DURATION_BUCKETS: &[f64] = &[
    0.000_5, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

struct Inner {
    registry: Registry,
    consumed: IntCounterVec,
    consume_duration: HistogramVec,
    published: IntCounterVec,
    /// The series of every subscription a delivery has named.
    consumers: ByName<ConsumeSeries>,
    /// The series of every destination a publish has named.
    publishers: ByName<PublishSeries>,
}

/// The series one subscription records into. The duration is resolved with the entry, which the
/// first delivery builds; a status counter is resolved the first time a delivery settles with
/// it, so the registry exports only the statuses that happened.
struct ConsumeSeries {
    name: Box<str>,
    duration: Histogram,
    /// One per entry of [`CONSUME_STATUSES`].
    consumed: [OnceLock<IntCounter>; 2],
}

impl ConsumeSeries {
    fn new(metrics: &Inner, name: &str) -> Self {
        Self {
            name: Box::from(name),
            duration: metrics.consume_duration.with_label_values(&[name]),
            consumed: [OnceLock::new(), OnceLock::new()],
        }
    }

    /// The counter of the deliveries settled with `result`.
    fn consumed(&self, counters: &IntCounterVec, result: HandlerResult) -> &IntCounter {
        let status = consume_status(result);
        self.consumed[status]
            .get_or_init(|| counters.with_label_values(&[&*self.name, CONSUME_STATUSES[status]]))
    }
}

/// The series one destination's publishes count into, each resolved the first time a publish
/// ends with its status.
struct PublishSeries {
    name: Box<str>,
    ok: OnceLock<IntCounter>,
    error: OnceLock<IntCounter>,
}

impl PublishSeries {
    fn new(name: &str) -> Self {
        Self {
            name: Box::from(name),
            ok: OnceLock::new(),
            error: OnceLock::new(),
        }
    }

    /// The counter of the publishes that ended `ok` or not.
    fn published(&self, counters: &IntCounterVec, ok: bool) -> &IntCounter {
        let (cell, status) = if ok {
            (&self.ok, "ok")
        } else {
            (&self.error, "error")
        };
        cell.get_or_init(|| counters.with_label_values(&[&*self.name, status]))
    }
}

/// Collects consume and publish metrics into a shared Prometheus registry.
///
/// Cheap to clone (shares one registry and counter set). Hand [`consume_layer`](Self::consume_layer)
/// to [`RustStream::layer`](ruststream_runtime_tokio::RustStream::layer) and
/// [`publish_layer`](Self::publish_layer) to
/// [`RustStream::publish_layer`](ruststream_runtime_tokio::RustStream::publish_layer).
///
/// The consume layer records every handled message, and the publish layer every publish
/// attempt, the failed ones included. The registry is the global default unless
/// [`with_registry`](Self::with_registry) passes your own.
///
/// | Metric | Type | Labels |
/// |---|---|---|
/// | `ruststream_messages_consumed_total` | counter | `name`, `status` |
/// | `ruststream_consume_duration_seconds` | histogram | `name` |
/// | `ruststream_messages_published_total` | counter | `name`, `status` |
/// | `ruststream_handler_poll_duration_seconds` | histogram | `name` |
/// | `ruststream_handler_poll_average_seconds` | gauge | `name` |
///
/// The last two exist with the `poll-diagnostics` feature, once
/// [`Metrics::observe_poll_diagnostics`] hands the collector the app's
/// [`PollDiagnostics`]: the time a handler spent in one sampled poll, and its moving average.
///
/// `name` is the subscription or destination, and `status` the outcome: `ack` or `nack` on the
/// consume side, `ok` or `error` on the publish side.
///
/// # Examples
///
/// ```
/// use ruststream::metrics::Metrics;
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// fn app(metrics: &Metrics) -> impl App {
///     RustStream::new(AppInfo::new("svc", "0.1.0"))
///         .layer(metrics.consume_layer())
///         .publish_layer(metrics.publish_layer())
/// }
///
/// # async fn serve() -> Result<(), Box<dyn std::error::Error>> {
/// // One collector per process: its registry is what the `/metrics` route renders.
/// let metrics = Metrics::new()?;
/// app(&metrics).run().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<Inner>,
}

impl fmt::Debug for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
                consumers: ByName::new(CAPACITY),
                publishers: ByName::new(CAPACITY),
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

    /// Registers the poll-time diagnostics of `diagnostics` in the registry: the
    /// `ruststream_handler_poll_duration_seconds` histogram and the
    /// `ruststream_handler_poll_average_seconds` gauge, labelled per subscription and read from
    /// the reports at every [`export`](Self::export).
    ///
    /// # Errors
    ///
    /// Returns an error if the two metric names are already registered in the registry, for
    /// example by a second call.
    ///
    /// # Examples
    ///
    /// ```
    /// use prometheus::{Error, Registry};
    /// use ruststream::metrics::Metrics;
    /// use ruststream::runtime::{App, AppInfo, PollDiagnostics, RustStream};
    ///
    /// /// The registry is the service's own, shared with the collectors it already exports.
    /// fn app(registry: Registry) -> Result<impl App, Error> {
    ///     let metrics = Metrics::with_registry(registry)?;
    ///     let diagnostics = PollDiagnostics::new();
    ///     metrics.observe_poll_diagnostics(&diagnostics)?;
    ///     Ok(RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///         .layer(metrics.consume_layer())
    ///         .poll_diagnostics(diagnostics))
    /// }
    /// ```
    #[cfg(feature = "poll-diagnostics")]
    pub fn observe_poll_diagnostics(
        &self,
        diagnostics: &PollDiagnostics,
    ) -> Result<(), prometheus::Error> {
        self.inner
            .registry
            .register(Box::new(PollCollector::new(diagnostics.clone())?))
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

/// The `status` values of `ruststream_messages_consumed_total`, in the order
/// [`consume_status`] indexes them.
const CONSUME_STATUSES: [&str; 2] = ["ack", "nack"];

/// The index into [`CONSUME_STATUSES`] of a delivery settled with `result`.
const fn consume_status(result: HandlerResult) -> usize {
    match result {
        HandlerResult::Ack => 0,
        // A delayed nack is still a nack for counting purposes.
        HandlerResult::Nack { .. } | HandlerResult::NackAfter { .. } => 1,
    }
}

/// The [`Layer`] handed out by [`Metrics::consume_layer`]. Wraps a handler with timing and counters.
#[derive(Clone)]
pub struct MetricsLayer {
    inner: Arc<Inner>,
}

impl fmt::Debug for MetricsLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl<H> fmt::Debug for MetricsHandler<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        let metrics = &*self.metrics;
        // Looked up before the body borrows the context: the series of a subscription are
        // resolved once, the first time it is named.
        let series = metrics
            .consumers
            .get_or_build(ctx.name(), |name| ConsumeSeries::new(metrics, name));
        async move {
            let started = Instant::now();
            // Classify by the outcome inside the settlement; the continuation (if any) passes
            // through untouched to the dispatcher.
            let settle = self.inner.handle(msg, ctx).await;
            series.duration.observe(started.elapsed().as_secs_f64());
            series.consumed(&metrics.consumed, settle.outcome()).inc();
            settle
        }
    }
}

/// The [`PublishLayer`] handed out by [`Metrics::publish_layer`].
#[derive(Clone)]
pub struct MetricsPublish {
    inner: Arc<Inner>,
}

impl fmt::Debug for MetricsPublish {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsPublish").finish_non_exhaustive()
    }
}

impl PublishLayer for MetricsPublish {
    fn on_publish<'a, N: PublishPipeline, P: ruststream_core::Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync>>> + Send + 'a {
        // The message moves into the rest of the pipeline, so the destination's series are
        // looked up while its name is still readable and kept by reference past the send.
        let series = self
            .inner
            .publishers
            .get_or_build(out.name(), PublishSeries::new);
        async move {
            let result = next.run(out).await;
            series
                .published(&self.inner.published, result.is_ok())
                .inc();
            result
        }
    }
}

/// The poll-time diagnostics as a collector the registry reads at every gather.
#[cfg(feature = "poll-diagnostics")]
mod poll {
    use std::collections::HashMap;

    use prometheus::core::{Collector, Desc};
    use prometheus::proto::{
        Bucket, Gauge, Histogram, LabelPair, Metric, MetricFamily, MetricType,
    };

    use ruststream_runtime::BUCKET_BOUNDS_NS;
    use ruststream_runtime::PollDiagnostics;

    const DURATION: &str = "ruststream_handler_poll_duration_seconds";
    const DURATION_HELP: &str = "Time a handler spent in one sampled poll, by name.";
    const AVERAGE: &str = "ruststream_handler_poll_average_seconds";
    const AVERAGE_HELP: &str = "Moving average of the time a handler spent in one poll, by name.";

    pub(super) struct PollCollector {
        diagnostics: PollDiagnostics,
        descs: [Desc; 2],
    }

    impl PollCollector {
        pub(super) fn new(diagnostics: PollDiagnostics) -> Result<Self, prometheus::Error> {
            let desc = |name: &str, help: &str| {
                Desc::new(
                    name.to_owned(),
                    help.to_owned(),
                    vec!["name".to_owned()],
                    HashMap::new(),
                )
            };
            Ok(Self {
                diagnostics,
                descs: [desc(DURATION, DURATION_HELP)?, desc(AVERAGE, AVERAGE_HELP)?],
            })
        }
    }

    impl Collector for PollCollector {
        fn desc(&self) -> Vec<&Desc> {
            self.descs.iter().collect()
        }

        // Nanosecond counts and bounds stay far below 2^52, where an `f64` holds them exactly.
        #[allow(clippy::cast_precision_loss)]
        fn collect(&self) -> Vec<MetricFamily> {
            let mut durations = Vec::new();
            let mut averages = Vec::new();
            for stats in self.diagnostics.stats() {
                let (samples, sum_ns, counts) = stats.histogram();
                let mut cumulative = 0;
                let buckets = BUCKET_BOUNDS_NS
                    .iter()
                    .zip(counts)
                    .map(|(&bound, count)| {
                        cumulative += count;
                        let mut bucket = Bucket::default();
                        bucket.set_upper_bound(bound as f64 / 1e9);
                        bucket.set_cumulative_count(cumulative);
                        bucket
                    })
                    .collect();
                let mut histogram = Histogram::default();
                histogram.set_sample_count(samples);
                histogram.set_sample_sum(sum_ns as f64 / 1e9);
                histogram.set_bucket(buckets);
                let mut metric = Metric::default();
                metric.set_label(label(stats.name()));
                metric.set_histogram(histogram);
                durations.push(metric);

                let mut gauge = Gauge::default();
                gauge.set_value(stats.report().average().as_secs_f64());
                let mut metric = Metric::default();
                metric.set_label(label(stats.name()));
                metric.set_gauge(gauge);
                averages.push(metric);
            }
            vec![
                family(DURATION, DURATION_HELP, MetricType::HISTOGRAM, durations),
                family(AVERAGE, AVERAGE_HELP, MetricType::GAUGE, averages),
            ]
        }
    }

    fn label(subscription: &str) -> Vec<LabelPair> {
        let mut pair = LabelPair::default();
        pair.set_name("name".to_owned());
        pair.set_value(subscription.to_owned());
        vec![pair]
    }

    fn family(name: &str, help: &str, kind: MetricType, metrics: Vec<Metric>) -> MetricFamily {
        let mut family = MetricFamily::default();
        family.set_name(name.to_owned());
        family.set_help(help.to_owned());
        family.set_field_type(kind);
        family.set_metric(metrics);
        family
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use prometheus::Registry;

    use super::{CONSUME_STATUSES, Metrics, consume_status};
    use ruststream_runtime::HandlerResult;

    #[test]
    fn the_registry_and_the_status_mapping() {
        let metrics = Metrics::with_registry(Registry::new()).unwrap();

        // registry() exposes the registry the three collectors were registered in: registering a
        // duplicate name there fails, which proves it is that same registry.
        let dup = prometheus::IntCounter::new("ruststream_messages_consumed_total", "dup").unwrap();
        assert!(metrics.registry().register(Box::new(dup)).is_err());

        // Every outcome maps to its counter label.
        let status = |result| CONSUME_STATUSES[consume_status(result)];
        assert_eq!(status(HandlerResult::Ack), "ack");
        assert_eq!(status(HandlerResult::drop()), "nack");
        assert_eq!(
            status(HandlerResult::retry_after(Duration::from_secs(1))),
            "nack"
        );
    }
}
