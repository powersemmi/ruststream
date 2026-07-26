//! Integration tests for the `otel` feature's dispatch metrics, collected through an in-memory
//! exporter (no collector, no network): the consume layer's per-delivery instruments and the
//! publish layer's per-publish instruments, both labeled per handler.
#![cfg(all(feature = "otel", feature = "testing", feature = "macros"))]

use std::time::Duration;

use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use ruststream::memory::MemoryBroker;
use ruststream::otel::Otel;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, TypedPublisher};
use ruststream::testing::{TestApp, expect_published};
use ruststream::{OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

#[subscriber("otel.orders")]
async fn consume(_order: &Order) -> HandlerResult {
    HandlerResult::Ack
}

#[subscriber("otel.requests", publish("otel.confirmations"))]
async fn confirm(order: &Order) -> Order {
    Order { id: order.id }
}

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
        .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .map(opentelemetry_sdk::metrics::data::SumDataPoint::value)
                .sum::<u64>(),
            _ => 0,
        })
        .sum()
}

/// How many points were recorded under the histogram `name`.
fn histogram_count(exporter: &InMemoryMetricExporter, name: &str) -> u64 {
    exporter
        .get_finished_metrics()
        .expect("exporter drained")
        .iter()
        .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .map(|metric| match metric.data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(opentelemetry_sdk::metrics::data::HistogramDataPoint::count)
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
        .with_broker(MemoryBroker::new(), |b| b.include(consume));

    let tb = TestApp::start(app).await.expect("harness start failed");
    tb.broker::<MemoryBroker>()
        .publish("otel.orders", &Order { id: 7 })
        .await
        .expect("publish failed");
    tb.broker::<MemoryBroker>()
        .subscriber("otel.orders")
        .assert_called_once();

    provider.force_flush().expect("flush failed");
    assert_eq!(u64_sum(&exporter, "messaging.client.consumed.messages"), 1);
    assert_eq!(u64_sum(&exporter, "ruststream.messages.processed"), 1);
    assert_eq!(histogram_count(&exporter, "messaging.process.duration"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_layer_records_per_publish_metrics_and_queue_time() {
    let (otel, provider, exporter) = otel_with_memory_exporter();
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let observer = broker.clone();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(otel.consume_layer())
        .publish_layer(otel.publish_layer())
        .with_broker(broker, |b| {
            let replies = TypedPublisher::new(b.broker().publisher());
            b.include_publishing(confirm, replies);
        });

    let running = app.start().await.expect("startup failed");
    publisher
        .publish(OutgoingMessage::new(
            "otel.requests",
            serde_json::to_vec(&Order { id: 3 }).unwrap().as_slice(),
        ))
        .await
        .expect("publish failed");

    let confirmed =
        expect_published(&observer, "otel.confirmations", 1, Duration::from_secs(5)).await;
    assert_eq!(confirmed.len(), 1, "the reply must be published");
    assert!(
        confirmed[0]
            .headers()
            .get_str(ruststream::otel::PUBLISH_TIME_HEADER)
            .is_some(),
        "the publish layer must stamp the publish-time header",
    );

    running.shutdown().await.expect("graceful shutdown failed");
    provider.force_flush().expect("flush failed");
    assert_eq!(u64_sum(&exporter, "messaging.client.sent.messages"), 1);
    assert_eq!(
        histogram_count(&exporter, "messaging.client.operation.duration"),
        1
    );
}
