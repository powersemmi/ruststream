//! Integration test for the Prometheus metrics layer (consume + publish paths).
#![cfg(all(feature = "metrics", feature = "memory", feature = "json"))]

use std::sync::Arc;
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::metrics::Metrics;
use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerResult, Router, RustStream};
use ruststream::{Name, OutgoingMessage, Publisher};
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_metrics_are_recorded() {
    let metrics = Metrics::with_registry(prometheus::Registry::new()).unwrap();
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(metrics.consume_layer())
        .with_broker(broker, |b| {
            b.subscribe(
                Name::new("pings"),
                |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
                HandlerMetadata::raw("pings"),
            );
        });

    let shutdown = Arc::new(Notify::new());
    let signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { signal.notified().await }));

    let recorded = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("pings", b"{}"))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if metrics
                .export()
                .unwrap()
                .contains("ruststream_messages_consumed_total")
            {
                break;
            }
        }
    })
    .await;
    assert!(recorded.is_ok(), "consume metric was never recorded");

    let text = metrics.export().unwrap();
    assert!(text.contains(r#"ruststream_messages_consumed_total{name="pings",status="ack"}"#));
    assert!(text.contains("ruststream_consume_duration_seconds"));

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_metrics_are_recorded_through_a_router() {
    let metrics = Metrics::with_registry(prometheus::Registry::new()).unwrap();
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // The consume layer rides a Router (via `Router::layer`) and must still reach the
    // router-mounted handler, whose concrete type the router hides. That works only because
    // `MetricsLayer` implements `BlanketLayer`.
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include_router(Router::new().layer(metrics.consume_layer()).subscribe(
            Name::new("pings"),
            |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
            HandlerMetadata::raw("pings"),
        ));
    });

    let shutdown = Arc::new(Notify::new());
    let signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { signal.notified().await }));

    let recorded = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("pings", b"{}"))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if metrics
                .export()
                .unwrap()
                .contains("ruststream_messages_consumed_total")
            {
                break;
            }
        }
    })
    .await;
    assert!(
        recorded.is_ok(),
        "consume metric was never recorded for a router handler"
    );

    let text = metrics.export().unwrap();
    assert!(text.contains(r#"ruststream_messages_consumed_total{name="pings",status="ack"}"#));

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[cfg(feature = "macros")]
mod publish {
    use super::{Arc, Duration, Notify};
    use ruststream::memory::MemoryBroker;
    use ruststream::metrics::Metrics;
    use ruststream::runtime::{AppInfo, RustStream, TypedPublisher};
    use ruststream::{OutgoingMessage, Publisher, subscriber};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct Req {
        n: u32,
    }

    #[derive(Serialize, Deserialize)]
    struct Resp {
        n: u32,
    }

    #[subscriber("requests", publish("responses"))]
    async fn reply(req: &Req) -> Resp {
        Resp { n: req.n }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_metrics_are_recorded() {
        let metrics = Metrics::with_registry(prometheus::Registry::new()).unwrap();
        let ingress = MemoryBroker::new();
        let egress = MemoryBroker::new();
        let ingress_pub = ingress.publisher();
        let egress_pub = TypedPublisher::new(egress.publisher());

        let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
            .publish_layer(metrics.publish_layer())
            .with_broker(ingress, |b| {
                b.include_publishing(reply, egress_pub);
            });

        let shutdown = Arc::new(Notify::new());
        let signal = Arc::clone(&shutdown);
        let run = tokio::spawn(app.run_until(async move { signal.notified().await }));

        let payload = serde_json::to_vec(&Req { n: 7 }).unwrap();
        let recorded = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let _ = ingress_pub
                    .publish(OutgoingMessage::new("requests", &payload))
                    .await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                if metrics
                    .export()
                    .unwrap()
                    .contains("ruststream_messages_published_total")
                {
                    break;
                }
            }
        })
        .await;
        assert!(recorded.is_ok(), "publish metric was never recorded");

        let text = metrics.export().unwrap();
        assert!(
            text.contains(r#"ruststream_messages_published_total{name="responses",status="ok"}"#)
        );

        shutdown.notify_one();
        run.await.unwrap().unwrap();
    }
}
