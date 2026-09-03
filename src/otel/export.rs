//! OpenTelemetry SDK integration (`otel` feature): OTLP export for traces and metrics, messaging
//! semantic conventions, and per-handler dispatch metrics.
//!
//! The [`propagation`](crate::otel::propagation) submodule is the propagation half: it carries the
//! W3C trace context and emits plain `tracing` spans. This module is the export half:
//! [`OtelBuilder::init`] installs the OpenTelemetry tracer and meter providers as the process **globals**
//! and bridges `tracing` spans into them, so the spans the propagation module already opens are
//! exported without further wiring - and user-defined business metrics need no plumbing at all:
//! `opentelemetry::global::meter("my-svc").u64_counter("orders_accepted").build()` rides the same
//! pipeline.
//!
//! Two middleware carry the dispatch metrics, following the OpenTelemetry messaging semantic
//! conventions plus a `ruststream.*` namespace for what the conventions do not cover:
//!
//! - [`Otel::consume_layer`] - per delivery: `messaging.client.consumed.messages`,
//!   `messaging.process.duration`, `ruststream.messages.processed` (by settlement outcome),
//!   `ruststream.messages.in_flight`, `ruststream.message.queue_time` when the delivery
//!   carries a publish timestamp, `ruststream.messages.decode_failures` when the decode
//!   adapter rejected the payload, and `ruststream.messages.panics` when the handler unwound.
//! - [`Otel::publish_layer`] - per publish: `messaging.client.sent.messages`,
//!   `messaging.client.operation.duration`, `ruststream.message.payload.size`, and the
//!   [`PUBLISH_TIME_HEADER`] stamp the consume side turns into queue time.
//!
//! Batch handlers bypass the per-message layer (the documented middleware exception), so the
//! batch dispatch itself records `ruststream.batch.size` (a histogram of decoded batch sizes,
//! per destination) through the global meter; it goes live once [`OtelBuilder::init`] installs
//! the global providers.
//!
//! # Examples
//!
//! ```no_run
//! # #[cfg(all(feature = "memory", feature = "json"))]
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use ruststream::memory::MemoryBroker;
//! use ruststream::otel::Otel;
//! use ruststream::runtime::{AppInfo, RustStream};
//!
//! let otel = Otel::builder()
//!     .service_name("orders-svc")
//!     .otlp_endpoint("http://collector:4317")
//!     .messaging_system("memory")
//!     .init()?;
//!
//! let app = RustStream::new(AppInfo::new("orders-svc", "0.1.0"))
//!     .layer(otel.consume_layer())
//!     .publish_layer(otel.publish_layer())
//!     .with_broker(MemoryBroker::new(), |b| { /* handlers */ });
//! let running = app.start().await?;
//! otel.observe_health(running.health());
//! # let _ = running;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider as _, UpDownCounter};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use thiserror::Error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::HeaderMap;
use crate::runtime::{
    BlanketLayer, Context, Handler, HandlerOutcome, HandlerResult, HealthProbe, HealthState, Layer,
    Outgoing, PublishLayer, PublishNext, PublishPipeline,
};

/// Header carrying the publish wall-clock time (unix milliseconds, ASCII decimal).
///
/// Stamped by [`Otel::publish_layer`]; the consume side turns it into the
/// `ruststream.message.queue_time` histogram, a broker-agnostic consumer-lag proxy. Deliveries
/// without the header (foreign producers) simply record no queue time.
pub const PUBLISH_TIME_HEADER: &str = "x-ruststream-published-at";

/// The advisory bucket boundaries the messaging semantic conventions prescribe for duration
/// histograms, in seconds.
const SEMCONV_DURATION_BUCKETS: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

/// An error from [`OtelBuilder::init`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OtelInitError {
    /// Building an OTLP exporter failed (bad endpoint, TLS configuration).
    #[error("failed to build the OTLP exporter")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
    /// A global `tracing` subscriber was already installed, so the span bridge cannot be.
    ///
    /// Install the bridge into your own subscriber stack instead, or build with
    /// [`tracing_bridge(false)`](OtelBuilder::tracing_bridge).
    #[error("a tracing subscriber is already installed")]
    TracingInit(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Builder for [`Otel`]; obtained from [`Otel::builder`].
#[derive(Debug, Default)]
#[must_use]
pub struct OtelBuilder {
    service_name: Option<String>,
    endpoint: Option<String>,
    messaging_system: Option<String>,
    attributes: Vec<KeyValue>,
    tracing_bridge: bool,
    stamp_publish_time: bool,
}

impl OtelBuilder {
    fn new() -> Self {
        Self {
            tracing_bridge: true,
            stamp_publish_time: true,
            ..Self::default()
        }
    }

    /// The `service.name` resource attribute every exported span and metric carries.
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// The OTLP/gRPC endpoint. When not set, the exporter falls back to its standard
    /// environment configuration (`OTEL_EXPORTER_OTLP_ENDPOINT`, default `localhost:4317`).
    pub fn otlp_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// The `messaging.system` attribute stamped on every dispatch metric (for example `kafka`,
    /// `rabbitmq`, `nats`, `memory`). The core cannot derive it broker-agnostically, so it is
    /// omitted when not set.
    pub fn messaging_system(mut self, system: impl Into<String>) -> Self {
        self.messaging_system = Some(system.into());
        self
    }

    /// An extra resource attribute (team, environment, region).
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes
            .push(KeyValue::new(key.into(), value.into()));
        self
    }

    /// Whether [`init`](Self::init) installs a global `tracing` subscriber bridging spans into
    /// OpenTelemetry (default `true`). Disable to compose the bridge into your own subscriber
    /// stack (for example together with the [`logging`](crate::logging) feature's fmt layer).
    pub const fn tracing_bridge(mut self, enabled: bool) -> Self {
        self.tracing_bridge = enabled;
        self
    }

    /// Whether [`Otel::publish_layer`] stamps [`PUBLISH_TIME_HEADER`] onto outgoing messages
    /// (default `true`), which is what feeds the consume side's queue-time histogram.
    pub const fn stamp_publish_time(mut self, enabled: bool) -> Self {
        self.stamp_publish_time = enabled;
        self
    }

    /// Builds the OTLP exporters, installs the tracer and meter providers as the OpenTelemetry
    /// **globals**, and (unless disabled) installs the `tracing` span bridge.
    ///
    /// Installing the globals is what lets business metrics ride the same pipeline with no
    /// plumbing: `opentelemetry::global::meter(..)` anywhere in user code reaches the provider
    /// this call configured.
    ///
    /// # Errors
    ///
    /// Returns [`OtelInitError::Exporter`] when an OTLP exporter cannot be built, and
    /// [`OtelInitError::TracingInit`] when the span bridge is enabled but a global `tracing`
    /// subscriber is already installed.
    pub fn init(self) -> Result<Otel, OtelInitError> {
        let mut resource = Resource::builder();
        if let Some(name) = &self.service_name {
            resource = resource.with_service_name(name.clone());
        }
        let resource = resource.with_attributes(self.attributes.clone()).build();

        let mut span_exporter = opentelemetry_otlp::SpanExporter::builder().with_tonic();
        let mut metric_exporter = opentelemetry_otlp::MetricExporter::builder().with_tonic();
        if let Some(endpoint) = &self.endpoint {
            span_exporter = span_exporter.with_endpoint(endpoint.clone());
            metric_exporter = metric_exporter.with_endpoint(endpoint.clone());
        }

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter.build()?)
            .with_resource(resource.clone())
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(metric_exporter.build()?)
            .with_resource(resource)
            .build();

        if self.tracing_bridge {
            let bridge =
                tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("ruststream"));
            tracing_subscriber::registry()
                .with(bridge)
                .try_init()
                .map_err(|err| OtelInitError::TracingInit(err.into()))?;
        }

        // The global propagator defaults to a no-op: without this, user code propagating
        // through `opentelemetry::global` would silently drop the W3C context.
        global::set_text_map_propagator(TraceContextPropagator::new());
        global::set_tracer_provider(tracer_provider.clone());
        global::set_meter_provider(meter_provider.clone());

        Ok(self.attach(tracer_provider, meter_provider))
    }

    /// Attaches to caller-built providers without touching the process globals or installing a
    /// `tracing` subscriber: the bring-your-own-exporter path (a custom pipeline, an in-memory
    /// exporter in tests).
    #[must_use]
    pub fn attach(
        self,
        tracer_provider: SdkTracerProvider,
        meter_provider: SdkMeterProvider,
    ) -> Otel {
        let meter = meter_provider.meter("ruststream");
        Otel {
            instruments: Arc::new(Instruments::new(&meter, self.messaging_system.clone())),
            stamp_publish_time: self.stamp_publish_time,
            meter,
            tracer_provider,
            meter_provider,
        }
    }
}

/// The installed OpenTelemetry integration: hands out the dispatch middleware and owns the
/// providers for the final flush.
///
/// Built with [`Otel::builder`]. Dropping it does not flush; call [`shutdown`](Self::shutdown)
/// at the end of `main`.
#[derive(Clone)]
pub struct Otel {
    instruments: Arc<Instruments>,
    stamp_publish_time: bool,
    meter: Meter,
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl std::fmt::Debug for Otel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Otel").finish_non_exhaustive()
    }
}

impl Otel {
    /// Starts configuring the integration.
    pub fn builder() -> OtelBuilder {
        OtelBuilder::new()
    }

    /// A consume-side [`Layer`] recording the per-delivery metrics; add it with
    /// [`RustStream::layer`](crate::runtime::RustStream::layer). Pair it with
    /// [`OpenTelemetry::consume_layer`](crate::otel::propagation::OpenTelemetry::consume_layer) for
    /// the trace-context propagation half.
    #[must_use]
    pub fn consume_layer(&self) -> OtelConsumeLayer {
        OtelConsumeLayer {
            instruments: Arc::clone(&self.instruments),
        }
    }

    /// A publish-side middleware recording the per-publish metrics (and stamping the queue-time
    /// header unless disabled); add it with
    /// [`RustStream::publish_layer`](crate::runtime::RustStream::publish_layer).
    #[must_use]
    pub fn publish_layer(&self) -> OtelPublishLayer {
        OtelPublishLayer {
            instruments: Arc::clone(&self.instruments),
            stamp_publish_time: self.stamp_publish_time,
        }
    }

    /// Registers the `ruststream.app.state` gauge over a [`HealthProbe`]: each lifecycle state
    /// is reported as 0/1 under a `state` attribute, so dashboards see startup, running, and
    /// failure alongside the traffic metrics.
    pub fn observe_health(&self, probe: HealthProbe) {
        let _gauge = self
            .meter
            .u64_observable_gauge("ruststream.app.state")
            .with_description("The service lifecycle state, 0/1 per state attribute.")
            .with_callback(move |observer| {
                let current = probe.state();
                let flags = [
                    ("running", current.is_running()),
                    (
                        "shutting_down",
                        matches!(current, HealthState::ShuttingDown),
                    ),
                    ("stopped", matches!(current, HealthState::Stopped)),
                    ("failed", matches!(current, HealthState::Failed { .. })),
                ];
                for (name, active) in flags {
                    observer.observe(u64::from(active), &[KeyValue::new("state", name)]);
                }
            })
            .build();
    }

    /// Flushes and shuts the providers down. Call at the end of `main`, after the app's own
    /// graceful shutdown, so the last spans and metric points reach the collector.
    ///
    /// # Errors
    ///
    /// Returns the SDK error when a provider fails to flush or shut down.
    pub fn shutdown(self) -> Result<(), OTelSdkError> {
        // Both providers must get their flush even when the other fails, or a span-flush error
        // silently discards every buffered metric point; the first error wins.
        let traces = self.tracer_provider.shutdown();
        let metrics = self.meter_provider.shutdown();
        traces.and(metrics)
    }
}

/// The dispatch instruments, shared by both middleware.
struct Instruments {
    system: Option<KeyValue>,
    consumed: Counter<u64>,
    process_duration: Histogram<f64>,
    processed: Counter<u64>,
    in_flight: UpDownCounter<i64>,
    queue_time: Histogram<f64>,
    decode_failures: Counter<u64>,
    panics: Counter<u64>,
    sent: Counter<u64>,
    operation_duration: Histogram<f64>,
    payload_size: Histogram<u64>,
}

impl Instruments {
    fn new(meter: &Meter, system: Option<String>) -> Self {
        Self {
            system: system.map(|s| KeyValue::new("messaging.system", s)),
            consumed: meter
                .u64_counter("messaging.client.consumed.messages")
                .with_unit("{message}")
                .with_description("Deliveries received, per handler.")
                .build(),
            process_duration: meter
                .f64_histogram("messaging.process.duration")
                .with_unit("s")
                .with_boundaries(SEMCONV_DURATION_BUCKETS.to_vec())
                .with_description("Handler processing time, per handler.")
                .build(),
            processed: meter
                .u64_counter("ruststream.messages.processed")
                .with_unit("{message}")
                .with_description("Settled deliveries by outcome, per handler.")
                .build(),
            in_flight: meter
                .i64_up_down_counter("ruststream.messages.in_flight")
                .with_unit("{message}")
                .with_description("Deliveries currently inside handlers, per handler.")
                .build(),
            queue_time: meter
                .f64_histogram("ruststream.message.queue_time")
                .with_unit("s")
                .with_description(
                    "Time from publish to handler start, where the publish stamped its \
                     timestamp header.",
                )
                .build(),
            decode_failures: meter
                .u64_counter("ruststream.messages.decode_failures")
                .with_unit("{message}")
                .with_description("Deliveries whose payload failed to decode, per handler.")
                .build(),
            panics: meter
                .u64_counter("ruststream.messages.panics")
                .with_unit("{message}")
                .with_description("Handler invocations that panicked, per handler.")
                .build(),
            sent: meter
                .u64_counter("messaging.client.sent.messages")
                .with_unit("{message}")
                .with_description("Messages published, split by error.type on failures.")
                .build(),
            operation_duration: meter
                .f64_histogram("messaging.client.operation.duration")
                .with_unit("s")
                .with_boundaries(SEMCONV_DURATION_BUCKETS.to_vec())
                .with_description("Duration of the publish operation.")
                .build(),
            payload_size: meter
                .u64_histogram("ruststream.message.payload.size")
                .with_unit("By")
                .with_description("Published payload sizes (direction attribute).")
                .build(),
        }
    }

    /// The shared attribute set: destination plus, when configured, the messaging system.
    fn attrs(&self, destination: &str, extra: Option<KeyValue>) -> Vec<KeyValue> {
        let mut attrs = Vec::with_capacity(3);
        attrs.push(KeyValue::new(
            "messaging.destination.name",
            destination.to_owned(),
        ));
        if let Some(system) = &self.system {
            attrs.push(system.clone());
        }
        if let Some(extra) = extra {
            attrs.push(extra);
        }
        attrs
    }
}

/// Maps a settlement outcome onto the `outcome` attribute of `ruststream.messages.processed`.
const fn outcome_attr(result: HandlerResult) -> &'static str {
    match result {
        HandlerResult::Ack => "ack",
        HandlerResult::Nack { requeue: true } => "nack_requeue",
        HandlerResult::Nack { requeue: false } => "nack_drop",
        HandlerResult::NackAfter { .. } => "retry_after",
    }
}

/// Parses the publish timestamp header into the queue time relative to now, if present and sane.
fn queue_time_seconds(headers: &HeaderMap) -> Option<f64> {
    let stamped: u128 = headers.get_str(PUBLISH_TIME_HEADER)?.parse().ok()?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();
    let elapsed_millis = now.checked_sub(stamped)?;
    #[allow(clippy::cast_precision_loss)] // Millisecond spans fit f64 comfortably.
    Some(elapsed_millis as f64 / 1000.0)
}

/// The [`Layer`] handed out by [`Otel::consume_layer`].
#[derive(Clone)]
pub struct OtelConsumeLayer {
    instruments: Arc<Instruments>,
}

impl std::fmt::Debug for OtelConsumeLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelConsumeLayer").finish_non_exhaustive()
    }
}

impl<H> Layer<H> for OtelConsumeLayer {
    type Handler = OtelConsumeHandler<H>;

    fn layer(&self, inner: H) -> Self::Handler {
        OtelConsumeHandler {
            inner,
            instruments: Arc::clone(&self.instruments),
        }
    }
}

impl BlanketLayer for OtelConsumeLayer {
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

/// The handler wrapper produced by [`OtelConsumeLayer`].
#[derive(Clone)]
pub struct OtelConsumeHandler<H> {
    inner: H,
    instruments: Arc<Instruments>,
}

impl<H> std::fmt::Debug for OtelConsumeHandler<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelConsumeHandler").finish_non_exhaustive()
    }
}

impl<M, C, S, H> Handler<M, C, S> for OtelConsumeHandler<H>
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
        let queue_time = queue_time_seconds(ctx.headers());
        async move {
            let instruments = &self.instruments;
            let attrs = instruments.attrs(&name, None);
            instruments.consumed.add(1, &attrs);
            if let Some(seconds) = queue_time {
                instruments.queue_time.record(seconds, &attrs);
            }
            instruments.in_flight.add(1, &attrs);
            let started = Instant::now();
            let settle = {
                // The decrement rides Drop so an unwinding handler (caught one level up by the
                // dispatch failure policy) or a cancelled dispatch cannot leak the increment.
                let _in_flight = InFlightGuard {
                    instruments,
                    attrs: &attrs,
                };
                self.inner.handle(msg, ctx).await
            };
            // The decode adapter (wrapped by this layer) marks the context when the payload did
            // not decode.
            if ctx.decode_failed() {
                instruments.decode_failures.add(1, &attrs);
            }
            instruments
                .process_duration
                .record(started.elapsed().as_secs_f64(), &attrs);
            instruments.processed.add(
                1,
                &instruments.attrs(
                    &name,
                    Some(KeyValue::new("outcome", outcome_attr(settle.outcome()))),
                ),
            );
            settle
        }
    }
}

/// Balances the `ruststream.messages.in_flight` up-down counter via `Drop`.
struct InFlightGuard<'a> {
    instruments: &'a Instruments,
    attrs: &'a [KeyValue],
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.instruments.in_flight.add(-1, self.attrs);
        // A panicking handler unwinds through this Drop (the handler future's live locals are
        // dropped as part of the unwind) before the dispatch loop's catch_unwind resolves it, so
        // this counts exactly one panic per unwound delivery.
        if thread::panicking() {
            self.instruments.panics.add(1, self.attrs);
        }
    }
}

/// The publish middleware handed out by [`Otel::publish_layer`].
#[derive(Clone)]
pub struct OtelPublishLayer {
    instruments: Arc<Instruments>,
    stamp_publish_time: bool,
}

impl std::fmt::Debug for OtelPublishLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelPublishLayer").finish_non_exhaustive()
    }
}

impl PublishLayer for OtelPublishLayer {
    fn on_publish<'a, N: PublishPipeline, P: crate::Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a
    {
        if self.stamp_publish_time
            && let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH)
        {
            out.headers_mut()
                .insert(PUBLISH_TIME_HEADER, now.as_millis().to_string());
        }
        let name = out.name().to_owned();
        #[allow(clippy::cast_possible_truncation)] // Payloads beyond u64 bytes do not exist.
        let payload_bytes = out.payload().len() as u64;
        async move {
            let instruments = &self.instruments;
            let attrs = instruments.attrs(&name, None);
            instruments.payload_size.record(
                payload_bytes,
                &instruments.attrs(&name, Some(KeyValue::new("direction", "publish"))),
            );
            let started = Instant::now();
            let result = next.run(out).await;
            instruments
                .operation_duration
                .record(started.elapsed().as_secs_f64(), &attrs);
            let sent_attrs = match &result {
                Ok(()) => attrs,
                // The error's own message is unbounded, remote-derived text: putting it in an
                // attribute mints a fresh time series per distinct failure until the SDK's
                // cardinality cap collapses the instrument. Semconv's `_OTHER` marks the failure
                // dimension; the message itself belongs to logs and the publish span.
                Err(_) => instruments.attrs(&name, Some(KeyValue::new("error.type", "_OTHER"))),
            };
            instruments.sent.add(1, &sent_attrs);
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_attr_names_every_settlement() {
        assert_eq!(outcome_attr(HandlerResult::Ack), "ack");
        assert_eq!(
            outcome_attr(HandlerResult::Nack { requeue: true }),
            "nack_requeue"
        );
        assert_eq!(
            outcome_attr(HandlerResult::Nack { requeue: false }),
            "nack_drop"
        );
        assert_eq!(
            outcome_attr(HandlerResult::retry_after(std::time::Duration::from_secs(
                1
            ))),
            "retry_after"
        );
    }

    #[cfg(feature = "json")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_layer_splits_failures_by_error_type() {
        use std::future::ready;

        use opentelemetry_sdk::metrics::InMemoryMetricExporter;

        use crate::codec::JsonCodec;
        use crate::runtime::{PublishContext, PublishIdentity, PublishStack, TypedPublisher};

        /// A publisher with no broker behind it: every publish fails.
        struct Failing;
        impl crate::Publisher for Failing {
            type Error = std::io::Error;
            fn publish(
                &self,
                _msg: crate::OutgoingMessage<'_>,
            ) -> impl Future<Output = Result<(), Self::Error>> {
                ready(Err(std::io::Error::other("no broker")))
            }
        }

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let otel = Otel::builder().attach(SdkTracerProvider::builder().build(), provider.clone());

        let pipeline = PublishStack::new(otel.publish_layer(), PublishIdentity);
        let publisher = TypedPublisher::with_codec(Failing, JsonCodec);
        let headers = HeaderMap::new();
        let cx = PublishContext::new("orders", &headers, &());
        let result = publisher.publish("orders", &7_u32, &pipeline, &cx).await;
        assert!(
            result.is_err(),
            "the failing publisher must surface its error"
        );

        provider.force_flush().expect("flush failed");
        let recorded: Vec<String> = exporter
            .get_finished_metrics()
            .expect("exporter drained")
            .iter()
            .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .map(|metric| metric.name().to_owned())
            .collect();
        assert!(
            recorded
                .iter()
                .any(|name| name == "messaging.client.sent.messages"),
            "the failed publish must still count as sent (split by error.type): {recorded:?}",
        );
    }

    #[test]
    fn queue_time_ignores_absent_garbage_and_future_stamps() {
        let empty = HeaderMap::new();
        assert_eq!(queue_time_seconds(&empty), None);

        let mut garbage = HeaderMap::new();
        garbage.insert(PUBLISH_TIME_HEADER, "not-a-number");
        assert_eq!(queue_time_seconds(&garbage), None);

        // A stamp from the future would need a negative duration; it is dropped instead.
        let mut future = HeaderMap::new();
        future.insert(PUBLISH_TIME_HEADER, u128::MAX.to_string());
        assert_eq!(queue_time_seconds(&future), None);

        let mut sane = HeaderMap::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        sane.insert(PUBLISH_TIME_HEADER, (now - 1_500).to_string());
        let seconds = queue_time_seconds(&sane).expect("a past stamp must produce a queue time");
        assert!(
            seconds >= 1.0,
            "expected at least a second of lag, got {seconds}"
        );
    }
}
