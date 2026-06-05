//! Integration tests for `RustStream` lifecycle and dispatch, using `MemoryBroker`.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, Context, Handler, HandlerMetadata, HandlerResult, Layer, Outgoing, PublishMiddleware,
    PublishNext, Router, RustStream, State,
};
use ruststream::{Name, OutgoingMessage, Publisher};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Order {
    id: u32,
    total: f64,
}

fn order_bytes(id: u32, total: f64) -> Vec<u8> {
    serde_json::to_vec(&Order { id, total }).unwrap()
}

/// A test layer that counts every invocation, to prove the global stack wraps handlers.
#[derive(Clone)]
struct CountLayer(Arc<AtomicU32>);

struct CountHandler<H> {
    inner: H,
    count: Arc<AtomicU32>,
}

impl<H> Layer<H> for CountLayer {
    type Handler = CountHandler<H>;

    fn layer(&self, inner: H) -> CountHandler<H> {
        CountHandler {
            inner,
            count: Arc::clone(&self.0),
        }
    }
}

impl<M, H> Handler<M> for CountHandler<H>
where
    M: Sync,
    H: Handler<M>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> HandlerResult {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.handle(msg, ctx).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_dispatches_typed_messages() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let received = Arc::new(AtomicU32::new(0));
    let received_clone = Arc::clone(&received);

    let handler =
        ruststream::runtime::typed(JsonCodec, move |order: &Order, _ctx: &mut Context| {
            let received = Arc::clone(&received_clone);
            let total = order.total;
            let id = order.id;
            async move {
                assert!(total > 0.0);
                received.fetch_add(id, Ordering::SeqCst);
                HandlerResult::Ack
            }
        });

    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // Subscribe up front so messages published after run() starts are buffered, not lost.
        let subscriber = b.broker().subscribe("orders");
        b.handle(
            subscriber,
            handler,
            HandlerMetadata::typed::<Order>("orders"),
        );
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .publish(OutgoingMessage::new("orders", &order_bytes(7, 9.99)))
        .await
        .unwrap();
    publisher
        .publish(OutgoingMessage::new("orders", &order_bytes(3, 1.0)))
        .await
        .unwrap();

    wait_for(
        || received.load(Ordering::SeqCst) == 10,
        Duration::from_secs(1),
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_subscribes_via_descriptor_after_connect() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen = Arc::new(AtomicU32::new(0));
    let seen_clone = Arc::clone(&seen);

    let app = RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(broker, |b| {
        b.subscribe(
            Name::new("events"),
            move |_msg: &_, _ctx: &mut Context| {
                let seen = Arc::clone(&seen_clone);
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    HandlerResult::Ack
                }
            },
            HandlerMetadata::raw("events"),
        );
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // The descriptor subscribes inside run(); retry publishing until the subscription is live.
    wait_for_published(&publisher, &seen, Duration::from_secs(1)).await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn included_router_handlers_dispatch() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen = Arc::new(AtomicU32::new(0));
    let seen_clone = Arc::clone(&seen);

    // Router defined independently of any live broker, then mounted.
    let mut router = Router::<MemoryBroker>::new();
    router.subscribe(
        Name::new("events"),
        move |_msg: &_, _ctx: &mut Context| {
            let seen = Arc::clone(&seen_clone);
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                HandlerResult::Ack
            }
        },
        HandlerMetadata::raw("events"),
    );

    let app = RustStream::new(AppInfo::new("events", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    wait_for_published(&publisher, &seen, Duration::from_secs(1)).await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_layer_wraps_handlers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let layer_hits = Arc::new(AtomicU32::new(0));
    let handler_hits = Arc::new(AtomicU32::new(0));
    let handler_hits_clone = Arc::clone(&handler_hits);

    // The app's global stack does not reach router handlers, so the router carries its own layer.
    let mut router = Router::<MemoryBroker>::new().layer(CountLayer(Arc::clone(&layer_hits)));
    router.subscribe(
        Name::new("events"),
        move |_msg: &_, _ctx: &mut Context| {
            let handler_hits = Arc::clone(&handler_hits_clone);
            async move {
                handler_hits.fetch_add(1, Ordering::SeqCst);
                HandlerResult::Ack
            }
        },
        HandlerMetadata::raw("events"),
    );

    let app = RustStream::new(AppInfo::new("events", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    wait_for_published(&publisher, &handler_hits, Duration::from_secs(1)).await;

    assert!(
        layer_hits.load(Ordering::SeqCst) >= 1,
        "router layer did not wrap the handler"
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_layer_wraps_handlers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let layer_hits = Arc::new(AtomicU32::new(0));
    let handler_hits = Arc::new(AtomicU32::new(0));
    let handler_hits_clone = Arc::clone(&handler_hits);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(CountLayer(Arc::clone(&layer_hits)))
        .with_broker(broker, |b| {
            let subscriber = b.broker().subscribe("orders");
            b.handle(
                subscriber,
                move |_msg: &_, _ctx: &mut Context| {
                    let handler_hits = Arc::clone(&handler_hits_clone);
                    async move {
                        handler_hits.fetch_add(1, Ordering::SeqCst);
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("orders"),
            );
        });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .publish(OutgoingMessage::new("orders", b"x"))
        .await
        .unwrap();

    wait_for(
        || handler_hits.load(Ordering::SeqCst) == 1 && layer_hits.load(Ordering::SeqCst) == 1,
        Duration::from_secs(1),
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_broker_publish_via_named_publisher() {
    let ingress = MemoryBroker::new();
    let egress = MemoryBroker::new();
    let ingress_pub = ingress.publisher();

    let received = Arc::new(AtomicU32::new(0));
    let received_clone = Arc::clone(&received);

    let app = RustStream::new(AppInfo::new("bridge", "0.1.0"))
        .publisher("egress", egress.publisher())
        .with_broker(ingress, |b| {
            let out = b.publisher("egress").expect("egress registered");
            b.subscribe(
                Name::new("orders"),
                move |_msg: &_, _ctx: &mut Context| {
                    let out = Arc::clone(&out);
                    async move {
                        let _ = out.publish_bytes("responses", b"reply").await;
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("orders"),
            );
        })
        .with_broker(egress, |b| {
            let subscriber = b.broker().subscribe("responses");
            b.handle(
                subscriber,
                move |_msg: &_, _ctx: &mut Context| {
                    let received = Arc::clone(&received_clone);
                    async move {
                        received.fetch_add(1, Ordering::SeqCst);
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("responses"),
            );
        });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    // ingress "orders" subscribes inside run() (deferred); retry until the bridge fires.
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = ingress_pub
                .publish(OutgoingMessage::new("orders", b"x"))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if received.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "cross-broker publish did not arrive on egress"
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

struct Config {
    greeting: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_reads_context_topic_and_state() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen = Arc::new(Mutex::new(None::<(String, String)>));
    let seen_clone = Arc::clone(&seen);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .insert_state(Config {
            greeting: "hello".to_owned(),
        })
        .with_broker(broker, |b| {
            let subscriber = b.broker().subscribe("orders");
            b.handle(
                subscriber,
                move |_msg: &_, ctx: &mut Context| {
                    let name = ctx.name().to_owned();
                    let greeting = ctx.get::<Config>().map(|c| c.greeting.clone());
                    // Middleware/handlers may enrich the working headers.
                    ctx.headers_mut().insert("x-seen", b"1".to_vec());
                    let seen = Arc::clone(&seen_clone);
                    async move {
                        *seen.lock().expect("poisoned") =
                            Some((name, greeting.unwrap_or_default()));
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("orders"),
            );
        });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .publish(OutgoingMessage::new("orders", b"x"))
        .await
        .unwrap();

    wait_for(
        || seen.lock().expect("poisoned").is_some(),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        *seen.lock().expect("poisoned"),
        Some(("orders".to_owned(), "hello".to_owned())),
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifespan_hooks_run_in_order() {
    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let (o1, o2, o3, o4) = (
        Arc::clone(&order),
        Arc::clone(&order),
        Arc::clone(&order),
        Arc::clone(&order),
    );

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .on_startup(move |mut state: State| {
            let o1 = Arc::clone(&o1);
            async move {
                state.insert(Config {
                    greeting: "lazy".to_owned(),
                });
                o1.lock().expect("poisoned").push("startup");
                Ok::<State, std::convert::Infallible>(state)
            }
        })
        .after_startup(move |_state| {
            let o2 = Arc::clone(&o2);
            async move {
                o2.lock().expect("poisoned").push("after_startup");
                Ok::<(), std::convert::Infallible>(())
            }
        })
        .on_shutdown(move |_state| {
            let o3 = Arc::clone(&o3);
            async move {
                o3.lock().expect("poisoned").push("on_shutdown");
                Ok::<(), std::convert::Infallible>(())
            }
        })
        .after_shutdown(move |state| {
            let o4 = Arc::clone(&o4);
            let greeting = state.get::<Config>().map(|c| c.greeting.clone());
            async move {
                assert_eq!(greeting.as_deref(), Some("lazy"));
                o4.lock().expect("poisoned").push("after_shutdown");
                Ok::<(), std::convert::Infallible>(())
            }
        })
        .with_broker(MemoryBroker::new(), |_b| {});

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    wait_for(
        || order.lock().expect("poisoned").contains(&"after_startup"),
        Duration::from_secs(1),
    )
    .await;
    shutdown.notify_one();
    run.await.unwrap().unwrap();

    assert_eq!(
        *order.lock().expect("poisoned"),
        vec!["startup", "after_startup", "on_shutdown", "after_shutdown"],
    );
}

#[test]
fn app_records_handler_metadata() {
    let broker = MemoryBroker::new();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        let subscriber = b.broker().subscribe("orders");
        b.handle(
            subscriber,
            |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
            HandlerMetadata::typed::<Order>("orders").with_description("processes orders"),
        );
        let alerts = b.broker().subscribe("alerts");
        b.handle(
            alerts,
            |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
            HandlerMetadata::raw("alerts"),
        );
    });

    assert_eq!(app.handlers().len(), 2);
    assert_eq!(app.handlers()[0].name, "orders");
    assert_eq!(
        app.handlers()[0].description.as_deref(),
        Some("processes orders"),
    );
    assert_eq!(app.handlers()[1].input_type, "bytes");
    assert_eq!(app.info().title, "svc");
}

async fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) {
    let result = tokio::time::timeout(timeout, async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "condition not met within {timeout:?}");
}

async fn wait_for_published(publisher: &impl Publisher, seen: &AtomicU32, timeout: Duration) {
    let result = tokio::time::timeout(timeout, async {
        loop {
            let _ = publisher
                .publish(OutgoingMessage::new("events", b"ping"))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if seen.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "no delivery within {timeout:?}");
}

/// A publish middleware that tags every outgoing message with a header.
struct Tagger;

impl PublishMiddleware for Tagger {
    fn on_publish<'a>(
        &'a self,
        out: &'a mut Outgoing,
        next: PublishNext<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>,
    > {
        Box::pin(async move {
            out.headers_mut().insert("x-envelope", b"1".to_vec());
            next.run(out).await
        })
    }
}

/// A handler that publishes manually via `ctx.publisher`. A struct (not a closure) so the future
/// may borrow `ctx` across the await.
struct Bridge;

impl<M: Send + Sync> Handler<M> for Bridge {
    async fn handle(&self, _msg: &M, ctx: &mut Context<'_>) -> HandlerResult {
        if let Some(out) = ctx.publisher("egress") {
            let _ = out
                .publish(Outgoing::new("responses", b"reply".to_vec()))
                .await;
        }
        HandlerResult::Ack
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_publisher_runs_through_pipeline() {
    let ingress = MemoryBroker::new();
    let egress = MemoryBroker::new();
    let ingress_pub = ingress.publisher();

    let tagged = Arc::new(AtomicU32::new(0));
    let tagged_clone = Arc::clone(&tagged);

    let app = RustStream::new(AppInfo::new("bridge", "0.1.0"))
        .publisher("egress", egress.publisher())
        .publish_layer(Tagger)
        .with_broker(ingress, |b| {
            b.subscribe(Name::new("orders"), Bridge, HandlerMetadata::raw("orders"));
        })
        .with_broker(egress, |b| {
            let subscriber = b.broker().subscribe("responses");
            b.handle(
                subscriber,
                move |msg: &_, _ctx: &mut Context| {
                    let tagged = Arc::clone(&tagged_clone);
                    // The Tagger middleware must have run on the manual publish.
                    let has_header = ruststream::IncomingMessage::headers(msg)
                        .get("x-envelope")
                        .is_some();
                    async move {
                        if has_header {
                            tagged.fetch_add(1, Ordering::SeqCst);
                        }
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("responses"),
            );
        });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let _ = ingress_pub
                .publish(OutgoingMessage::new("orders", b"x"))
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if tagged.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "manual publish did not reach egress with the pipeline header",
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
