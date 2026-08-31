//! Integration tests for the `otel` feature's dispatch metrics, collected through an in-memory
//! exporter (no collector, no network): the consume layer's per-delivery instruments and the
//! publish layer's per-publish instruments, both labeled per handler.
#![cfg(all(feature = "otel", feature = "testing", feature = "macros"))]

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use opentelemetry::global;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, HistogramDataPoint, MetricData, ResourceMetrics, ScopeMetrics, SumDataPoint,
};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use ruststream::memory::MemoryBroker;
use ruststream::otel::{Otel, PUBLISH_TIME_HEADER};
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream};
use ruststream::testing::{TestApp, expect_published};
use ruststream::{ConnectedBroker, subscriber};
use tokio::sync::{Mutex, Notify};

use common::{Order, connected};

#[subscriber("otel.orders")]
async fn consume(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("otel.drops")]
async fn reject(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::drop()
}

/// Panics on every delivery; the drop policy keeps the service alive across the panic.
#[subscriber("otel.panics", on_failure(panic = drop))]
async fn implode(order: &Order) -> HandlerOutcome {
    // The test never publishes u32::MAX, so this always panics; the trailing expression keeps
    // the body typed as HandlerOutcome.
    assert_eq!(order.id, u32::MAX, "handler exploded");
    HandlerOutcome::ack()
}

#[subscriber("otel.requests", publish("otel.confirmations"))]
async fn confirm(order: &Order) -> Order {
    Order { id: order.id }
}

/// Serializes the tests that touch the process-global OpenTelemetry providers (`init()` and the
/// batch test's `set_meter_provider`): interleaving them could rebind the global mid-test, and
/// the batch-size instrument binds to whatever provider is global at its first use.
static GLOBAL_PROVIDERS: Mutex<()> = Mutex::const_new(());

/// An `Otel` wired to an in-memory exporter; returns the exporter to read points back.
fn otel_with_memory_exporter() -> (Otel, SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let otel = Otel::builder()
        .messaging_system("memory")
        .attach(SdkTracerProvider::builder().build(), meter_provider.clone());
    (otel, meter_provider, exporter)
}

/// The total of every u64 sum data point recorded under `name`.
fn u64_sum(exporter: &InMemoryMetricExporter, name: &str) -> u64 {
    exporter
        .get_finished_metrics()
        .expect("exporter drained")
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                sum.data_points().map(SumDataPoint::value).sum::<u64>()
            }
            _ => 0,
        })
        .sum()
}

/// The total of every i64 sum data point recorded under `name` (up-down counters).
fn i64_sum(exporter: &InMemoryMetricExporter, name: &str) -> i64 {
    exporter
        .get_finished_metrics()
        .expect("exporter drained")
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .map(|metric| match metric.data() {
            AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                sum.data_points().map(SumDataPoint::value).sum::<i64>()
            }
            _ => 0,
        })
        .sum()
}

/// Every value the u64 sum under `name` recorded for the attribute `key`, one per data point
/// that carries it.
fn sum_attr_values(exporter: &InMemoryMetricExporter, name: &str, key: &str) -> Vec<String> {
    exporter
        .get_finished_metrics()
        .expect("exporter drained")
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .flat_map(|point| {
                    point
                        .attributes()
                        .filter(|kv| kv.key.as_str() == key)
                        .map(|kv| kv.value.to_string())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

/// Every point of the u64 histogram `name`: its count, its value total, and the
/// `messaging.destination.name` attribute when the point carries one.
fn u64_histogram_points(
    exporter: &InMemoryMetricExporter,
    name: &str,
) -> Vec<(u64, u64, Option<String>)> {
    exporter
        .get_finished_metrics()
        .expect("exporter drained")
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(|point| {
                    let destination = point
                        .attributes()
                        .find(|kv| kv.key.as_str() == "messaging.destination.name")
                        .map(|kv| kv.value.to_string());
                    (point.count(), point.sum(), destination)
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

/// How many points were recorded under the histogram `name`.
fn histogram_count(exporter: &InMemoryMetricExporter, name: &str) -> u64 {
    exporter
        .get_finished_metrics()
        .expect("exporter drained")
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .map(|metric| match metric.data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(HistogramDataPoint::count)
                .sum::<u64>(),
            _ => 0,
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_layer_records_per_delivery_metrics() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(consume);
            b.include(reject);
        });

    let tb = TestApp::start(app).await.expect("harness start failed");
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 7 })
        .to("otel.orders")
        .publish()
        .await
        .expect("publish failed");
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 8 })
        .to("otel.drops")
        .publish()
        .await
        .expect("publish failed");
    tb.broker::<MemoryBroker>()
        .subscriber("otel.orders")
        .assert_called_once();
    tb.broker::<MemoryBroker>()
        .subscriber("otel.drops")
        .assert_called_once();

    provider.force_flush().expect("flush failed");
    assert_eq!(u64_sum(&exporter, "messaging.client.consumed.messages"), 2);
    assert_eq!(u64_sum(&exporter, "ruststream.messages.processed"), 2);
    assert_eq!(histogram_count(&exporter, "messaging.process.duration"), 2);
    assert_eq!(
        u64_sum(&exporter, "ruststream.messages.decode_failures"),
        0,
        "cleanly decoded deliveries must not count as decode failures",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_undecodable_payload_bumps_the_decode_failure_counter() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(consume);
        });

    let tb = TestApp::start(app).await.expect("harness start failed");
    tb.broker::<MemoryBroker>()
        .raw(b"not json")
        .to("otel.orders")
        .publish()
        .await
        .expect("publish failed");
    tb.broker::<MemoryBroker>()
        .subscriber("otel.orders")
        .assert_called_once()
        .assert_last_failed_to_decode();

    provider.force_flush().expect("flush failed");
    assert_eq!(
        u64_sum(&exporter, "ruststream.messages.decode_failures"),
        1,
        "the rejected payload must be counted as a decode failure",
    );
}

/// One point recorded under the u64 gauge `name` with `value` for the given state attribute.
fn gauge_reports(exporter: &InMemoryMetricExporter, name: &str) -> bool {
    exporter
        .get_finished_metrics()
        .expect("exporter drained")
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .any(|metric| metric.name() == name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_installs_globals_and_shutdown_returns() {
    let _globals = GLOBAL_PROVIDERS.lock().await;
    // Building the exporters performs no I/O, so a dead endpoint only shows up when flushing;
    // the test asserts init and shutdown return rather than hang. The bridge-less form runs
    // first inside the same test, keeping the single try_init deterministic.
    let quiet = Otel::builder()
        .tracing_bridge(false)
        .stamp_publish_time(false)
        .otlp_endpoint("http://127.0.0.1:1")
        .init()
        .expect("bridge-less init failed");
    let _ = quiet.shutdown();

    let bridged = Otel::builder()
        .service_name("otel-test")
        .otlp_endpoint("http://127.0.0.1:1")
        .messaging_system("memory")
        .attribute("deployment.environment", "test")
        .init()
        .expect("init failed");
    assert!(format!("{bridged:?}").contains("Otel"));
    let probe_layer = bridged.consume_layer();
    assert!(format!("{probe_layer:?}").contains("OtelConsumeLayer"));
    assert!(format!("{:?}", bridged.publish_layer()).contains("OtelPublishLayer"));
    let _ = bridged.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_handler_does_not_leak_the_in_flight_gauge() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(implode);
        });

    let tb = TestApp::start(app).await.expect("harness start failed");
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .to("otel.panics")
        .publish()
        .await
        .expect("publish failed");
    tb.broker::<MemoryBroker>()
        .subscriber("otel.panics")
        .assert_called_once();

    provider.force_flush().expect("flush failed");
    assert_eq!(
        i64_sum(&exporter, "ruststream.messages.in_flight"),
        0,
        "a panic caught by the failure policy must still balance the in-flight gauge",
    );
    assert_eq!(
        u64_sum(&exporter, "ruststream.messages.panics"),
        1,
        "the unwound handler must bump the panic counter exactly once",
    );
}

/// Signals when the failing handler holds its delivery, so the test can kill the bus first.
static FAIL_ENTERED: Notify = Notify::const_new();
static FAIL_PROCEED: Notify = Notify::const_new();
static FAIL_ONCE: AtomicBool = AtomicBool::new(false);

/// Replies once (the publish fails against the killed bus); redeliveries settle quietly.
#[subscriber("otel.failing", publish("otel.nowhere"))]
async fn confirm_once(order: &Order) -> Result<Order, HandlerOutcome> {
    if FAIL_ONCE.swap(true, Ordering::SeqCst) {
        return Err(HandlerOutcome::ack());
    }
    FAIL_ENTERED.notify_one();
    FAIL_PROCEED.notified().await;
    Ok(Order { id: order.id })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_publish_keeps_error_type_low_cardinality() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    // An aliased connected clone: shutting it down kills the shared bus mid-flight, which is
    // the only way a memory publish fails.
    let bus_killer = connected(&broker).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(otel.publish_layer())
        .with_broker(broker, |b| {
            b.include(confirm_once);
        });

    let running = app.start().await.expect("startup failed");
    publisher
        .message(&Order { id: 5 })
        .to("otel.failing")
        .publish()
        .await
        .expect("publish failed");

    // The handler holds the delivery while the bus dies under it; its reply publish then fails.
    tokio::time::timeout(Duration::from_secs(5), FAIL_ENTERED.notified())
        .await
        .expect("the handler never received the request");
    bus_killer.shutdown().await.expect("bus shutdown failed");
    FAIL_PROCEED.notify_one();

    common::wait_for(
        || {
            provider.force_flush().expect("flush failed");
            !sum_attr_values(&exporter, "messaging.client.sent.messages", "error.type").is_empty()
        },
        Duration::from_secs(5),
    )
    .await;
    running.shutdown().await.expect("graceful shutdown failed");

    let errors = sum_attr_values(&exporter, "messaging.client.sent.messages", "error.type");
    assert!(
        errors.iter().all(|value| value == "_OTHER"),
        "error.type must be a bounded class, not the raw error text (a fresh time series per \
         distinct failure): got {errors:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_flushes_metrics_even_when_the_tracer_fails() {
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let tracer_provider = SdkTracerProvider::builder().build();
    // A second shutdown on the shared inner state errors, which is exactly the failure the
    // meter's flush must survive.
    tracer_provider
        .shutdown()
        .expect("the first tracer shutdown succeeds");
    let otel = Otel::builder().attach(tracer_provider, meter_provider.clone());

    meter_provider
        .meter("probe")
        .u64_counter("otel.shutdown.probe")
        .build()
        .add(1, &[]);

    assert!(
        otel.shutdown().is_err(),
        "the poisoned tracer provider must surface its shutdown error",
    );
    assert_eq!(
        u64_sum(&exporter, "otel.shutdown.probe"),
        1,
        "a failing tracer shutdown must not skip the meter flush",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_layer_records_per_publish_metrics_and_queue_time() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let observer = connected(&broker).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .publish_layer(otel.publish_layer())
        .with_broker(broker, |b| {
            b.include(confirm);
        });

    let running = app.start().await.expect("startup failed");
    publisher
        .message(&Order { id: 3 })
        .to("otel.requests")
        .publish()
        .await
        .expect("publish failed");

    let confirmed =
        expect_published(&observer, "otel.confirmations", 1, Duration::from_secs(5)).await;
    assert_eq!(confirmed.len(), 1, "the reply must be published");
    assert!(
        confirmed[0]
            .headers()
            .get_str(PUBLISH_TIME_HEADER)
            .is_some(),
        "the publish layer must stamp the publish-time header",
    );

    otel.observe_health(running.health());
    running.shutdown().await.expect("graceful shutdown failed");
    provider.force_flush().expect("flush failed");
    assert!(
        gauge_reports(&exporter, "ruststream.app.state"),
        "the health gauge must be collected",
    );
    assert_eq!(u64_sum(&exporter, "messaging.client.sent.messages"), 1);
    assert_eq!(
        histogram_count(&exporter, "messaging.client.operation.duration"),
        1
    );
}

/// Elements the batch handler has consumed so far, to wait on without sleeping.
static BATCHED_ELEMENTS: AtomicUsize = AtomicUsize::new(0);

#[subscriber(batch("otel.batches"))]
async fn absorb(orders: &[Order]) -> HandlerOutcome {
    BATCHED_ELEMENTS.fetch_add(orders.len(), Ordering::SeqCst);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_dispatch_records_the_batch_size_histogram() {
    let _globals = GLOBAL_PROVIDERS.lock().await;
    let (_otel, provider, exporter) = otel_with_memory_exporter();
    // Batch handlers bypass the per-message layer, so the batch-size histogram rides the global
    // meter: the in-memory provider must be installed as the process global before the first
    // batch is dispatched, which is when the lazily-built instrument binds its provider.
    global::set_meter_provider(provider.clone());

    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(absorb);
    });

    let running = app.start().await.expect("startup failed");
    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to("otel.batches")
            .publish()
            .await
            .expect("publish failed");
    }
    common::wait_for(
        || BATCHED_ELEMENTS.load(Ordering::SeqCst) >= 3,
        Duration::from_secs(5),
    )
    .await;
    running.shutdown().await.expect("graceful shutdown failed");

    provider.force_flush().expect("flush failed");
    let points = u64_histogram_points(&exporter, "ruststream.batch.size");
    let (batches, elements) = points
        .iter()
        .fold((0, 0), |(count, total), (c, t, _)| (count + c, total + t));
    assert!(
        batches >= 1,
        "at least one batch must be recorded: {points:?}",
    );
    // The three publishes may buffer into one batch or split, but the sizes always add up.
    assert_eq!(
        elements, 3,
        "the recorded sizes must add up to every decoded element: {points:?}",
    );
    assert!(
        points
            .iter()
            .all(|(_, _, dest)| dest.as_deref() == Some("otel.batches")),
        "every point must carry the destination attribute: {points:?}",
    );
}
