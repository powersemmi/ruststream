//! Integration tests for the `otel` feature's dispatch metrics, collected through an in-memory
//! exporter (no collector, no network): the consume layer's per-delivery instruments and the
//! publish layer's per-publish instruments, both labeled per handler.
#![cfg(all(feature = "otel", feature = "testing", feature = "macros"))]

mod common;

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use allocation_counter::measure;
use opentelemetry::global;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, HistogramDataPoint, MetricData, ResourceMetrics, ScopeMetrics, SumDataPoint,
};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, Instrument, SdkMeterProvider, Stream};
use opentelemetry_sdk::trace::SdkTracerProvider;
use ruststream::memory::{MemoryBroker, MemoryPublish, Retention};
use ruststream::otel::{Otel, PUBLISH_TIME_HEADER};
use ruststream::runtime::{
    App, AppInfo, ForReply, HandlerOutcome, Names, Outgoing as OutgoingView, PublishContext,
    PublishExt, PublishTransform, Reply, RustStream, SubscriberSettings,
};
use ruststream::testing::{TestApp, expect_published};
use ruststream::{Broker, ConnectedBroker, HeaderMap, Str, nonzero, subscriber};
use tokio::runtime::Builder;
use tokio::sync::Mutex;

use common::{Order, Wire, connected};

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

#[subscriber("otel.requests", reply("otel.confirmations"))]
async fn confirm(order: &Order) -> Order {
    Order { id: order.id }
}

#[subscriber("otel.confirmations")]
async fn settle(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
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

/// The bucket boundaries of every point of the f64 histogram `name`, with the point's count.
fn f64_histogram_bounds(exporter: &InMemoryMetricExporter, name: &str) -> Vec<(u64, Vec<f64>)> {
    exporter
        .get_finished_metrics()
        .expect("exporter drained")
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(|point| (point.count(), point.bounds().collect()))
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
        .with_broker(MemoryBroker::new(), |b| b.include(consume).include(reject));

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
        .with_broker(MemoryBroker::new(), |b| b.include(consume));

    let tb = TestApp::start(app).await.expect("harness start failed");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"not json"))
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
    let _ = bridged.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_handler_does_not_leak_the_in_flight_gauge() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .with_broker(MemoryBroker::new(), |b| b.include(implode));

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

/// Replies once, on a broker of its own that the test shuts down before the request arrives; the
/// redelivery the failed reply causes settles quietly.
#[subscriber("otel.failing", reply("otel.nowhere"))]
async fn confirm_once(
    order: &Order,
    ctx: &mut Context<'_, (), Arc<AtomicBool>>,
) -> Result<Order, HandlerOutcome> {
    if ctx.state().swap(true, Ordering::SeqCst) {
        return Err(HandlerOutcome::ack());
    }
    Ok(Order { id: order.id })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_publish_keeps_error_type_low_cardinality() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let egress = MemoryBroker::new();
    // An aliased connected clone of the reply's broker: shutting it down kills the shared bus,
    // which is the only way a memory publish fails.
    let bus_killer = egress.clone();
    let egress = egress.bindable();
    let to_egress = egress.bind(MemoryPublish);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(AtomicBool::new(false))))
        .publish_layer(otel.publish_layer())
        .with_broker_labeled("egress", egress, |b| b.end())
        .with_broker_labeled("ingress", MemoryBroker::new(), |b| {
            b.include(confirm_once).out(Reply, to_egress)
        });
    let tb = TestApp::start(app).await.expect("harness start failed");
    connected(&bus_killer)
        .await
        .shutdown()
        .await
        .expect("bus shutdown failed");

    tb.broker_named("ingress")
        .message(&Order { id: 5 })
        .to("otel.failing")
        .publish()
        .await
        .expect("publish failed");
    tb.broker_named("ingress")
        .subscriber("otel.failing")
        .assert_called(2);

    provider.force_flush().expect("flush failed");
    let errors = sum_attr_values(&exporter, "messaging.client.sent.messages", "error.type");
    assert!(
        !errors.is_empty(),
        "the failed reply must be counted with its error class",
    );
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

// The subject IS the running app: the health gauge is fed from `RunningApp::health`, which only
// the started form hands out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_layer_records_per_publish_metrics_and_queue_time() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    // The reply is read back off the publish log, so the broker keeps what it published.
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(64)));
    let publisher = broker.publisher();
    let observer = connected(&broker).await;
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .publish_layer(otel.publish_layer())
        .with_broker(broker, |b| b.include(confirm));

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

// Queue time is measured in seconds, and a stamped delivery on one process lands in
// milliseconds: under the SDK's default boundaries (0, 5, 10, 25, ..) every sample falls into
// the first bucket and a p99 reads as seconds of lag that never happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_time_is_bucketed_like_the_other_durations() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .publish_layer(otel.publish_layer())
        .with_broker(MemoryBroker::new(), |b| b.include(confirm).include(settle));

    let tb = TestApp::start(app).await.expect("harness start failed");
    tb.message(&Order { id: 5 })
        .to("otel.requests")
        .publish()
        .await
        .expect("publish failed");
    tb.broker::<MemoryBroker>()
        .subscriber("otel.confirmations")
        .assert_called(1);
    tb.shutdown().await.expect("graceful shutdown failed");

    provider.force_flush().expect("flush failed");
    let queue_time = f64_histogram_bounds(&exporter, "ruststream.message.queue_time");
    let process = f64_histogram_bounds(&exporter, "messaging.process.duration");
    assert!(
        queue_time.iter().any(|(count, _)| *count > 0),
        "the stamped reply must record a queue time: {queue_time:?}",
    );
    let (_, expected) = process.first().expect("a process duration was recorded");
    for (_, bounds) in &queue_time {
        assert_eq!(
            bounds, expected,
            "queue time must share the semantic-convention duration buckets",
        );
    }
}

#[subscriber("otel.batches")]
async fn absorb(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
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

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(absorb.batch(nonzero!(8)))
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    for id in 0..3u32 {
        tb.message(&Order { id })
            .to("otel.batches")
            .publish()
            .await
            .expect("publish failed");
    }
    tb.broker::<MemoryBroker>()
        .subscriber("otel.batches")
        .assert_called(3);
    tb.shutdown().await.expect("graceful shutdown failed");

    provider.force_flush().expect("flush failed");
    let points = u64_histogram_points(&exporter, "ruststream.batch.size");
    let (batches, elements) = points
        .iter()
        .fold((0, 0), |(count, total), (c, t, _)| (count + c, total + t));
    assert!(
        batches >= 1,
        "at least one batch must be recorded: {points:?}",
    );
    // However the stream was cut into batches, the recorded sizes add up to every element.
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

/// Drives `rounds` request/reply rounds through the harness: each one a delivery to `confirm`
/// and the reply it publishes.
async fn request_rounds(tb: &TestApp<()>, rounds: u32) {
    for id in 0..rounds {
        tb.message(&Order { id })
            .to("otel.requests")
            .publish()
            .await
            .expect("publish failed");
    }
}

/// The allocations this thread makes over 32 request/reply rounds of `app`, after a warm-up that
/// pays every first time: the SDK's tracker per attribute set, the harness's buffers.
///
/// Every task runs on this thread's own runtime, so the count holds the whole dispatch and the
/// tests the harness runs beside this one do not enter it.
fn steady_allocations<A: App<State = ()>>(app: A) -> u64 {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds");
    let tb = runtime
        .block_on(TestApp::start(app))
        .expect("harness start failed");
    runtime.block_on(request_rounds(&tb, 8));
    let counted = measure(|| runtime.block_on(request_rounds(&tb, 32)));
    runtime
        .block_on(tb.shutdown())
        .expect("graceful shutdown failed");
    counted.count_total
}

/// The dispatch metrics cost a delivery and its reply no allocation: the attribute sets are built
/// once per destination and reused, so a service with the layers allocates exactly what one
/// without them does.
#[test]
fn the_dispatch_metrics_allocate_nothing_per_message() {
    // A tracing subscriber `init()` installs between the two counts would give one of them the
    // events the other did not format.
    let _globals = GLOBAL_PROVIDERS.blocking_lock();
    let exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .build();
    // The publish-time stamp is a header the message carries, data the broker sends, so it
    // stays out of a count of what recording costs.
    let otel = Otel::builder()
        .messaging_system("memory")
        .stamp_publish_time(false)
        .attach(SdkTracerProvider::builder().build(), meter_provider);

    let bare = steady_allocations(
        RustStream::new(AppInfo::new("svc", "0.1.0"))
            .with_broker(MemoryBroker::new(), |b| b.include(confirm)),
    );
    let recorded = steady_allocations(
        RustStream::new(AppInfo::new("svc", "0.1.0"))
            .layer(otel.consume_layer())
            .publish_layer(otel.publish_layer())
            .with_broker(MemoryBroker::new(), |b| b.include(confirm)),
    );
    assert_eq!(
        recorded, bare,
        "32 recorded rounds allocate {recorded} times, the same rounds unrecorded {bare}",
    );
}

/// Answers on the destination the request names in its `x-tenant` header, so a test can publish
/// to as many destinations as it likes.
struct ToTenant;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ToTenant {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut OutgoingView<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(Ok(tenant)) = cx.headers().get_shared("x-tenant").map(Str::try_from) {
            out.set_name(tenant);
        }
    }
}

#[subscriber("otel.tenants", reply("otel.answers"))]
async fn answer(order: &Order) -> Order {
    Order { id: order.id }
}

/// Publishes one request answered on `tenant`.
async fn tenant_request(tb: &TestApp<()>, tenant: &str) {
    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", tenant.to_owned());
    tb.message(&Order { id: 1 })
        .to("otel.tenants")
        .with_headers(headers)
        .publish()
        .await
        .expect("publish failed");
}

/// Drives `rounds` deliveries to `consume`.
async fn order_rounds(tb: &TestApp<()>, rounds: u32) {
    for id in 0..rounds {
        tb.message(&Order { id })
            .to("otel.orders")
            .publish()
            .await
            .expect("publish failed");
    }
}

/// The allocations this thread makes over 32 answers to one destination and over 32 deliveries
/// to one subscription, both named only after 2,500 other destinations were answered.
fn allocations_past_many_destinations<A: App<State = ()>>(app: A) -> (u64, u64) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds");
    let tb = runtime
        .block_on(TestApp::start(app))
        .expect("harness start failed");
    runtime.block_on(async {
        for tenant in 0..2_500 {
            tenant_request(&tb, &format!("tenant.{tenant}")).await;
        }
        for _ in 0..8 {
            tenant_request(&tb, "tenant.late").await;
        }
        order_rounds(&tb, 8).await;
    });
    let answers = measure(|| {
        runtime.block_on(async {
            for _ in 0..32 {
                tenant_request(&tb, "tenant.late").await;
            }
        });
    });
    let deliveries = measure(|| runtime.block_on(order_rounds(&tb, 32)));
    runtime
        .block_on(tb.shutdown())
        .expect("graceful shutdown failed");
    (answers.count_total, deliveries.count_total)
}

/// Past the destinations the layers keep attribute sets for, a message builds only what it
/// records: its destination, the shared set, and the one set its outcome adds.
#[test]
fn a_destination_past_the_kept_ones_builds_only_what_it_records() {
    let _globals = GLOBAL_PROVIDERS.blocking_lock();
    // The SDK's own limit sits above the layers', so its overflow series stays out of the count.
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(InMemoryMetricExporter::default())
        .with_view(|_: &Instrument| Stream::builder().with_cardinality_limit(8_192).build().ok())
        .build();
    let otel = Otel::builder()
        .messaging_system("memory")
        .stamp_publish_time(false)
        .attach(SdkTracerProvider::builder().build(), meter_provider);

    let (bare_answers, bare_deliveries) = allocations_past_many_destinations(
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(answer)
                .out_reply(MemoryPublish)
                .transform(ToTenant)
                .include(consume)
        }),
    );
    let (answers, deliveries) = allocations_past_many_destinations(
        RustStream::new(AppInfo::new("svc", "0.1.0"))
            .layer(otel.consume_layer())
            .publish_layer(otel.publish_layer())
            .with_broker(MemoryBroker::new(), |b| {
                b.include(answer)
                    .out_reply(MemoryPublish)
                    .transform(ToTenant)
                    .include(consume)
            }),
    );
    assert_eq!(
        answers - bare_answers,
        3 * 32,
        "an answer to an uncached destination allocates its name, the shared set and the payload \
         set",
    );
    assert_eq!(
        deliveries - bare_deliveries,
        3 * 32,
        "a delivery to an uncached subscription allocates its name, the shared set and the \
         outcome set",
    );
}
