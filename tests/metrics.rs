//! Integration test for the Prometheus metrics layer (consume + publish paths).
//!
//! The subject is the exporter's output, so the assertions stay on the exported text; the handler
//! side rides the harness, whose injection settles the whole dispatch - the metric recording
//! included - before it returns.
#![cfg(all(
    feature = "metrics",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use crate::common;

use common::Wire;

use std::convert::Infallible;
use std::future::ready;

use ruststream::memory::prelude::*;
use ruststream::metrics::Metrics;
use ruststream::testing::TestApp;

/// The payload view the body takes: whatever bytes arrive, undecoded, so the test's subject
/// stays the metric rather than a message model.
// The body needs the delivery, not its bytes; the field is what makes the type a payload view.
#[allow(dead_code)]
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

/// Acks whatever arrives: the subject is the metric the dispatch records around the body, not the
/// body itself.
struct Ping;

impl<'p> Handle<Frame<'p>> for Ping {
    fn handle(
        &self,
        _ping: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_metrics_are_recorded() {
    let metrics = Metrics::with_registry(prometheus::Registry::new()).unwrap();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(metrics.consume_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("pings", Ping).build())
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Wire::of(b"{}"))
        .to("pings")
        .publish()
        .await
        .expect("publish failed");
    tb.broker::<MemoryBroker>()
        .subscriber("pings")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    let text = metrics.export().unwrap();
    assert!(text.contains(r#"ruststream_messages_consumed_total{name="pings",status="ack"}"#));
    assert!(text.contains("ruststream_consume_duration_seconds"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_metrics_are_recorded_through_a_router() {
    let metrics = Metrics::with_registry(prometheus::Registry::new()).unwrap();

    // The consume layer rides a Router (via `Router::layer`) and must still reach the
    // router-mounted handler, whose concrete type the router hides. That works only because
    // `MetricsLayer` implements `BlanketLayer`.
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(
            Router::new()
                .layer(metrics.consume_layer())
                .include(subscriber("pings", Ping).build()),
        )
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Wire::of(b"{}"))
        .to("pings")
        .publish()
        .await
        .expect("publish failed");
    tb.broker::<MemoryBroker>()
        .subscriber("pings")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    let text = metrics.export().unwrap();
    assert!(text.contains(r#"ruststream_messages_consumed_total{name="pings",status="ack"}"#));
}

#[cfg(feature = "macros")]
mod publish {
    use allocation_counter::measure;

    use super::common::{Req, Resp};
    use ruststream::memory::prelude::*;
    use ruststream::metrics::Metrics;
    use ruststream::testing::TestApp;
    use tokio::runtime::Builder;

    #[subscriber("requests", reply("responses"))]
    async fn reply(req: &Req) -> Resp {
        Resp { n: req.n }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_metrics_are_recorded() {
        let metrics = Metrics::with_registry(prometheus::Registry::new()).unwrap();

        let egress = MemoryBroker::new().bindable();
        let egress_pub = egress.bind(Publish);
        let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
            .publish_layer(metrics.publish_layer())
            .with_broker_labeled("egress", egress, |b| b.end())
            .with_broker_labeled("ingress", MemoryBroker::new(), |b| {
                b.include(reply).out(Reply, egress_pub)
            });
        let tb = TestApp::start(app).await.expect("startup failed");

        tb.broker_named("ingress")
            .message(&Req { n: 7 })
            .to("requests")
            .publish()
            .await
            .expect("publish failed");
        tb.broker_named("egress")
            .published::<Resp>("responses")
            .assert_called_once()
            .with(&Resp { n: 7 });

        let text = metrics.export().unwrap();
        assert!(
            text.contains(r#"ruststream_messages_published_total{name="responses",status="ok"}"#)
        );
    }

    /// Drives `rounds` request/reply rounds through the harness: each one a delivery to `reply`
    /// and the answer it publishes.
    async fn request_rounds(tb: &TestApp<()>, rounds: u32) {
        for n in 0..rounds {
            tb.message(&Req { n })
                .to("requests")
                .publish()
                .await
                .expect("publish failed");
        }
    }

    /// The allocations this thread makes over 32 request/reply rounds of `app`, after a warm-up
    /// that pays every first time: the registry's series per label set, the harness's buffers.
    ///
    /// Every task runs on this thread's own runtime, so the count holds the whole dispatch and
    /// the tests the harness runs beside this one do not enter it.
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

    /// The metrics cost a delivery and its reply no allocation: the series of a name are resolved
    /// once and reused, so a service with the layers allocates exactly what one without them does.
    #[test]
    fn the_metrics_allocate_nothing_per_message() {
        let metrics = Metrics::with_registry(prometheus::Registry::new()).unwrap();

        let bare = steady_allocations(
            RustStream::new(AppInfo::new("svc", "0.1.0"))
                .with_broker(MemoryBroker::new(), |b| b.include(reply)),
        );
        let recorded = steady_allocations(
            RustStream::new(AppInfo::new("svc", "0.1.0"))
                .layer(metrics.consume_layer())
                .publish_layer(metrics.publish_layer())
                .with_broker(MemoryBroker::new(), |b| b.include(reply)),
        );
        assert_eq!(
            recorded, bare,
            "32 recorded rounds allocate {recorded} times, the same rounds unrecorded {bare}",
        );
    }
}
