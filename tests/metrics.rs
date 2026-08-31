//! Integration test for the Prometheus metrics layer (consume + publish paths).
//!
//! Apps come up through `start()`, which resolves only after subscriptions are open, so each test
//! publishes exactly once; the metric itself is recorded when the reaction settles, moments after
//! the handler returns, so the exported text is polled without sleeping.
#![cfg(all(feature = "metrics", feature = "memory", feature = "json"))]

mod common;

use common::wait_for;

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::metrics::Metrics;
use ruststream::prelude::*;

/// Acks whatever arrives: the subject is the metric the dispatch records around the body, not the
/// body itself.
struct Ping;

impl<'p> Handle<Payload<'p>> for Ping {
    fn handle(
        &self,
        _ping: &Payload<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_metrics_are_recorded() {
    let metrics = Metrics::with_registry(prometheus::Registry::new()).unwrap();
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(metrics.consume_layer())
        .with_broker(broker, |b| {
            b.include(subscriber("pings", Ping).build());
        });

    let running = app.start().await.expect("startup failed");
    publisher
        .raw(b"{}")
        .to("pings")
        .publish()
        .await
        .expect("publish failed");
    wait_for(
        || {
            metrics
                .export()
                .unwrap()
                .contains("ruststream_messages_consumed_total")
        },
        Duration::from_secs(5),
    )
    .await;

    let text = metrics.export().unwrap();
    assert!(text.contains(r#"ruststream_messages_consumed_total{name="pings",status="ack"}"#));
    assert!(text.contains("ruststream_consume_duration_seconds"));

    running.shutdown().await.unwrap();
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
        b.include_router(
            Router::new()
                .layer(metrics.consume_layer())
                .include(subscriber("pings", Ping).build()),
        );
    });

    let running = app.start().await.expect("startup failed");
    publisher
        .raw(b"{}")
        .to("pings")
        .publish()
        .await
        .expect("publish failed");
    wait_for(
        || {
            metrics
                .export()
                .unwrap()
                .contains("ruststream_messages_consumed_total")
        },
        Duration::from_secs(5),
    )
    .await;

    let text = metrics.export().unwrap();
    assert!(text.contains(r#"ruststream_messages_consumed_total{name="pings",status="ack"}"#));

    running.shutdown().await.unwrap();
}

#[cfg(feature = "macros")]
mod publish {
    use super::common::{Req, Resp};
    use super::{Duration, wait_for};
    use ruststream::memory::{MemoryBroker, MemoryPublish};
    use ruststream::metrics::Metrics;
    use ruststream::prelude::*;

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

        let egress = egress.bindable();
        let egress_pub = egress.bind(TypedPublisher::new(MemoryPublish));
        let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
            .publish_layer(metrics.publish_layer())
            .with_broker(egress, |_b| {})
            .with_broker(ingress, |b| {
                b.include(reply).publisher(egress_pub);
            });

        let running = app.start().await.expect("startup failed");
        let payload = serde_json::to_vec(&Req { n: 7 }).unwrap();
        ingress_pub
            .raw(&payload)
            .to("requests")
            .publish()
            .await
            .expect("publish failed");
        wait_for(
            || {
                metrics
                    .export()
                    .unwrap()
                    .contains("ruststream_messages_published_total")
            },
            Duration::from_secs(5),
        )
        .await;

        let text = metrics.export().unwrap();
        assert!(
            text.contains(r#"ruststream_messages_published_total{name="responses",status="ok"}"#)
        );

        running.shutdown().await.unwrap();
    }
}
