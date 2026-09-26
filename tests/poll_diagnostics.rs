//! The `poll-diagnostics` feature: every subscription samples the time its handler spends in
//! `poll`, keeps an average and a histogram of it, and warns when the average says the handler
//! computes on the app's runtime and belongs on `threads(n)`.
//!
//! A slow handler is made slow by a spin bounded by a counter, never by sleeping: a sleep is an
//! `await`, which is the one thing the measurement leaves out.
//!
//! The warning tests capture the log with a subscriber installed for the test's own thread, so
//! they run on the current-thread runtime, where the harness drives every handler on that thread.
#![cfg(all(
    feature = "poll-diagnostics",
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::hint::black_box;
use std::io;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use common::Order;
use ruststream::memory::MemoryBroker;
use ruststream::nonzero;
use ruststream::prelude::*;
use ruststream::runtime::PollDiagnostics;
use ruststream::testing::TestApp;
use tracing_subscriber::fmt::MakeWriter;

/// Two million dependent multiply-adds: at least two million cycles on any machine, so well over
/// the thresholds the slow cases set, and deterministic in what it computes.
const SLOW_ROUNDS: u64 = 2_000_000;

/// The threshold the slow cases warn over. [`SLOW_ROUNDS`] cannot finish in it.
const LOW_THRESHOLD: Duration = Duration::from_micros(50);

/// The threshold the fast cases stay under: a handler that returns at once is nowhere near it.
const HIGH_THRESHOLD: Duration = Duration::from_millis(50);

/// How many samples the average needs before it may warn.
const WARM_UP: u32 = 16;

fn spin(rounds: u64) -> u64 {
    let mut acc = 0u64;
    for i in 0..rounds {
        acc = black_box(acc.wrapping_mul(31).wrapping_add(i));
    }
    acc
}

/// Computes in its one poll.
#[subscriber("slow")]
async fn slow(order: &Order) -> HandlerOutcome {
    black_box(spin(SLOW_ROUNDS + u64::from(order.id)));
    HandlerOutcome::ack()
}

/// Returns at once.
#[subscriber("fast")]
async fn fast(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Every poll counted, so the counts below are exact.
fn every_poll() -> PollDiagnostics {
    PollDiagnostics::new().sample_every(NonZeroU32::MIN)
}

async fn publish(tb: &TestApp<()>, to: &str, count: u32) {
    for id in 0..count {
        tb.message(&Order { id })
            .to(to)
            .publish()
            .await
            .expect("publish");
    }
}

/// The log lines written while the guard lives, as text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(self.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    fn text(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_handler_is_sampled_over_the_threshold() {
    let diagnostics = every_poll().threshold(LOW_THRESHOLD);
    let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
        .poll_diagnostics(diagnostics.clone())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(slow);
        });
    let tb = TestApp::start(app).await.expect("startup");
    publish(&tb, "slow", 3).await;
    tb.broker::<MemoryBroker>()
        .subscriber("slow")
        .assert_called(3);

    let report = diagnostics
        .report("slow")
        .expect("the subscription is sampled");
    assert_eq!(report.subscription(), "slow");
    assert_eq!(report.samples(), 3);
    assert!(
        report.average() > LOW_THRESHOLD,
        "average {:?} of a spinning handler",
        report.average()
    );
    assert!(report.p99() > LOW_THRESHOLD, "p99 {:?}", report.p99());
}

/// The ids from here on are handled at once; the ones below compute.
const FAST_FROM: u32 = 1_000;

/// Computes on a low id, returns at once on a high one.
#[subscriber("varying")]
async fn varying(order: &Order) -> HandlerOutcome {
    if order.id < FAST_FROM {
        black_box(spin(SLOW_ROUNDS + u64::from(order.id)));
    }
    HandlerOutcome::ack()
}

/// Publishes `count` orders to `varying`, from `first` on.
async fn publish_from(tb: &TestApp<()>, first: u32, count: u32) {
    for id in first..first + count {
        tb.message(&Order { id })
            .to("varying")
            .publish()
            .await
            .expect("publish");
    }
}

/// The warning comes once the average is warmed up, once per crossing, and again when the average
/// crosses after it fell back.
#[tokio::test]
async fn the_average_warns_once_per_crossing_naming_the_fix() {
    let logs = Captured::default();
    let _guard = logs.install();
    let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
        .poll_diagnostics(every_poll().threshold(LOW_THRESHOLD))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(varying);
        });
    let tb = TestApp::start(app).await.expect("startup");
    let warnings = || logs.text().matches("threads(n)").count();

    publish_from(&tb, 0, WARM_UP - 1).await;
    assert_eq!(warnings(), 0, "warned before the average warmed up");
    publish_from(&tb, WARM_UP - 1, 4).await;
    assert_eq!(warnings(), 1, "{}", logs.text());
    let text = logs.text();
    let line = text
        .lines()
        .find(|line| line.contains("threads(n)"))
        .unwrap_or_else(|| panic!("no warning naming threads(n) in:\n{text}"));
    assert!(line.contains("WARN"), "{line}");
    assert!(line.contains("subscription=varying"), "{line}");
    assert!(line.contains("average="), "{line}");
    assert!(line.contains("p99="), "{line}");

    // The average falls under the threshold, then crosses it again.
    publish_from(&tb, FAST_FROM, 100).await;
    assert_eq!(warnings(), 1, "{}", logs.text());
    publish_from(&tb, 0, WARM_UP).await;
    assert_eq!(warnings(), 2, "{}", logs.text());
}

#[tokio::test]
async fn a_fast_handler_is_not_warned_about() {
    let logs = Captured::default();
    let _guard = logs.install();
    let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
        .poll_diagnostics(every_poll().threshold(HIGH_THRESHOLD))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(fast);
        });
    let tb = TestApp::start(app).await.expect("startup");
    publish(&tb, "fast", 2 * WARM_UP).await;
    assert!(!logs.text().contains("threads(n)"), "{}", logs.text());
}

/// The slow body, declared on dedicated threads.
#[subscriber("slow.threads", threads(2))]
async fn slow_on_threads(order: &Order) -> HandlerOutcome {
    black_box(spin(SLOW_ROUNDS + u64::from(order.id)));
    HandlerOutcome::ack()
}

/// A subscription already on `threads(n)` is measured, and the warning that advises it is not
/// written for it. The harness runs its threads as workers of the test's runtime, which is what
/// lets the log be captured here; the placement is the declared one either way.
#[tokio::test]
async fn a_slow_handler_on_threads_is_measured_without_a_warning() {
    let logs = Captured::default();
    let _guard = logs.install();
    let diagnostics = every_poll().threshold(LOW_THRESHOLD);
    let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
        .poll_diagnostics(diagnostics.clone())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(slow_on_threads);
        });
    let tb = TestApp::start(app).await.expect("startup");
    publish(&tb, "slow.threads", 2 * WARM_UP).await;

    let report = diagnostics
        .report("slow.threads")
        .expect("the subscription is sampled");
    assert!(report.average() > LOW_THRESHOLD, "{:?}", report.average());
    assert!(!logs.text().contains("threads(n)"), "{}", logs.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_nth_poll_is_sampled() {
    let diagnostics = PollDiagnostics::new().sample_every(nonzero!(4u32));
    let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
        .poll_diagnostics(diagnostics.clone())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(fast);
        });
    let tb = TestApp::start(app).await.expect("startup");
    publish(&tb, "fast", 7).await;
    assert_eq!(diagnostics.report("fast").map(|r| r.samples()), Some(1));
    publish(&tb, "fast", 1).await;
    assert_eq!(diagnostics.report("fast").map(|r| r.samples()), Some(2));
}

/// Waits on a timer, so its delivery takes two polls.
#[subscriber("waits")]
async fn waits(_order: &Order) -> HandlerOutcome {
    tokio::time::sleep(Duration::from_secs(1)).await;
    HandlerOutcome::ack()
}

#[tokio::test]
async fn each_poll_is_a_sample() {
    tokio::time::pause();
    let diagnostics = every_poll();
    let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
        .poll_diagnostics(diagnostics.clone())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(waits);
        });
    let tb = TestApp::start(app).await.expect("startup");
    tb.message(&Order { id: 1 })
        .to("waits")
        .publish()
        .await
        .expect("publish");
    tb.advance(Duration::from_secs(1)).await.expect("advance");
    tb.broker::<MemoryBroker>()
        .subscriber("waits")
        .assert_called(1)
        .settled(HandlerOutcome::ack());

    let report = diagnostics
        .report("waits")
        .expect("the subscription is sampled");
    // Two polls, the one that armed the timer and the one that finished: the sample is taken
    // per poll, not per delivery.
    assert_eq!(report.samples(), 2);
}

/// Settles a whole batch at once.
#[subscriber("batches")]
async fn batches(orders: &[Order]) -> HandlerOutcome {
    black_box(orders.len());
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_is_sampled() {
    let diagnostics = every_poll();
    let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
        .poll_diagnostics(diagnostics.clone())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(batches.batch(nonzero!(8)));
        });
    let tb = TestApp::start(app).await.expect("startup");
    publish(&tb, "batches", 3).await;
    let handled: Vec<Vec<Order>> = tb.broker::<MemoryBroker>().subscriber("batches").batches();
    let report = diagnostics
        .report("batches")
        .expect("the subscription is sampled");
    assert_eq!(report.samples(), handled.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_subscription_is_reported_on_its_own() {
    let diagnostics = every_poll();
    let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
        .poll_diagnostics(diagnostics.clone())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(fast);
            b.include(waits);
        });
    let tb = TestApp::start(app).await.expect("startup");
    publish(&tb, "fast", 2).await;
    let mut names: Vec<String> = diagnostics
        .reports()
        .iter()
        .map(|r| r.subscription().to_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["fast", "waits"]);
    assert_eq!(diagnostics.report("waits").map(|r| r.samples()), Some(0));
    assert_eq!(diagnostics.report("nowhere"), None);
}

/// The reports through the Prometheus registry of the `metrics` feature.
#[cfg(feature = "metrics")]
mod prometheus_export {
    use prometheus::Registry;
    use ruststream::memory::MemoryBroker;
    use ruststream::metrics::Metrics;
    use ruststream::prelude::*;
    use ruststream::testing::TestApp;

    use super::{every_poll, fast, publish};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_histogram_and_the_average_are_exported_per_subscription() {
        let diagnostics = every_poll();
        let metrics = Metrics::with_registry(Registry::new()).expect("metrics");
        metrics
            .observe_poll_diagnostics(&diagnostics)
            .expect("the collector registers");
        let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
            .poll_diagnostics(diagnostics.clone())
            .with_broker(MemoryBroker::new(), |b| {
                b.include(fast);
            });
        let tb = TestApp::start(app).await.expect("startup");
        publish(&tb, "fast", 3).await;

        let text = metrics.export().expect("export");
        assert!(
            text.contains(r#"ruststream_handler_poll_duration_seconds_count{name="fast"} 3"#),
            "{text}"
        );
        assert!(
            text.contains(
                r#"ruststream_handler_poll_duration_seconds_bucket{name="fast",le="+Inf"} 3"#
            ),
            "{text}"
        );
        assert!(
            text.contains(r#"ruststream_handler_poll_average_seconds{name="fast"}"#),
            "{text}"
        );
        // A second registration of the same collector is refused, as any duplicate metric is.
        assert!(metrics.observe_poll_diagnostics(&diagnostics).is_err());
    }
}

/// The reports through the meter of the `otel` feature.
#[cfg(feature = "otel")]
mod otel_export {
    use opentelemetry_sdk::metrics::data::{
        AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use ruststream::memory::MemoryBroker;
    use ruststream::otel::Otel;
    use ruststream::prelude::*;
    use ruststream::testing::TestApp;

    use super::{every_poll, fast, publish};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_average_p99_and_samples_are_observed_per_subscription() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let otel = Otel::builder().attach(SdkTracerProvider::builder().build(), provider.clone());
        let diagnostics = every_poll();
        otel.observe_poll_diagnostics(&diagnostics);
        let app = RustStream::new(AppInfo::new("diag", "0.1.0"))
            .poll_diagnostics(diagnostics.clone())
            .with_broker(MemoryBroker::new(), |b| {
                b.include(fast);
            });
        let tb = TestApp::start(app).await.expect("startup");
        publish(&tb, "fast", 3).await;
        provider.force_flush().expect("flush");

        let metrics = exporter.get_finished_metrics().expect("drained");
        let observed = |name: &str| -> Vec<(String, f64)> {
            metrics
                .iter()
                .flat_map(ResourceMetrics::scope_metrics)
                .flat_map(ScopeMetrics::metrics)
                .filter(|metric| metric.name() == name)
                .flat_map(|metric| match metric.data() {
                    AggregatedMetrics::F64(MetricData::Gauge(gauge)) => gauge
                        .data_points()
                        .map(|point| (subscription_of(point.attributes()), point.value()))
                        .collect::<Vec<_>>(),
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                        .data_points()
                        .map(|point| {
                            let count = u32::try_from(point.value()).expect("a small count");
                            (subscription_of(point.attributes()), f64::from(count))
                        })
                        .collect::<Vec<_>>(),
                    _ => Vec::new(),
                })
                .collect()
        };
        assert_eq!(
            observed("ruststream.handler.poll.samples"),
            [("fast".to_owned(), 3.0)]
        );
        let average = observed("ruststream.handler.poll.average");
        assert_eq!(average.len(), 1, "{average:?}");
        assert!(average[0].1 > 0.0, "{average:?}");
        let p99 = observed("ruststream.handler.poll.p99");
        assert_eq!(p99.len(), 1, "{p99:?}");
        assert!(p99[0].1 > 0.0, "{p99:?}");
    }

    fn subscription_of<'a>(
        attributes: impl Iterator<Item = &'a opentelemetry::KeyValue>,
    ) -> String {
        attributes
            .filter(|kv| kv.key.as_str() == "messaging.destination.name")
            .map(|kv| kv.value.to_string())
            .collect()
    }
}
