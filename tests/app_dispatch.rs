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
    AppInfo, BlanketLayer, Context, Handler, HandlerMetadata, HandlerResult, Layer, Router,
    RustStream, Settle,
};
use ruststream::{Name, OutgoingMessage, Publisher};
use serde::{Deserialize, Serialize};
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

impl<M, C, S, H> Handler<M, C, S> for CountHandler<H>
where
    M: Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> Settle {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.handle(msg, ctx).await
    }
}

// Lets CountLayer be an app-global layer that reaches router handlers via include_router.
impl BlanketLayer for CountLayer {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        CountHandler {
            inner: handler,
            count: Arc::clone(&self.0),
        }
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
        Duration::from_secs(5),
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_drains_post_settle_continuations() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // The continuation signals `parked` once it is running, then blocks on `release` until the
    // test lets it proceed, and finally marks `drained`. The drain on shutdown must wait for it.
    let parked = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let drained = Arc::new(AtomicU32::new(0));

    let on_parked = Arc::clone(&parked);
    let gate = Arc::clone(&release);
    let flag = Arc::clone(&drained);
    let handler = move |_msg: &_, _ctx: &mut Context| {
        let on_parked = Arc::clone(&on_parked);
        let gate = Arc::clone(&gate);
        let flag = Arc::clone(&flag);
        async move {
            HandlerResult::ack().and_after(async move {
                // Signal once the continuation is in flight, then block: the drain must await it.
                on_parked.notify_one();
                gate.notified().await;
                flag.store(1, Ordering::SeqCst);
            })
        }
    };

    let app = RustStream::new(AppInfo::new("drain", "0.1.0")).with_broker(broker, |b| {
        let subscriber = b.broker().subscribe("work");
        b.handle(subscriber, handler, HandlerMetadata::raw("work"));
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .publish(OutgoingMessage::new("work", b"go"))
        .await
        .unwrap();

    // Wait until the continuation is spawned and blocked (the message is already acked).
    parked.notified().await;

    // Begin shutdown while the continuation is still in flight, then release it: the drain holds
    // run() open until the continuation completes.
    shutdown.notify_one();
    release.notify_one();
    run.await.unwrap().unwrap();

    // run() returned only after the in-flight continuation finished.
    assert_eq!(drained.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_timeout_abandons_stuck_continuations() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    // A continuation that never completes: it parks forever. With a shutdown timeout, the drain
    // bounds its wait and returns, leaving the continuation abandoned (at-most-once).
    let parked = Arc::new(Notify::new());
    let finished = Arc::new(AtomicU32::new(0));

    let on_parked = Arc::clone(&parked);
    let flag = Arc::clone(&finished);
    let handler = move |_msg: &_, _ctx: &mut Context| {
        let on_parked = Arc::clone(&on_parked);
        let flag = Arc::clone(&flag);
        async move {
            HandlerResult::ack().and_after(async move {
                on_parked.notify_one();
                std::future::pending::<()>().await;
                flag.store(1, Ordering::SeqCst);
            })
        }
    };

    let app = RustStream::new(AppInfo::new("drain", "0.1.0"))
        .shutdown_timeout(Duration::from_millis(50))
        .with_broker(broker, |b| {
            let subscriber = b.broker().subscribe("work");
            b.handle(subscriber, handler, HandlerMetadata::raw("work"));
        });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .publish(OutgoingMessage::new("work", b"go"))
        .await
        .unwrap();
    parked.notified().await;

    shutdown.notify_one();
    // The drain times out and run() returns without the continuation ever completing.
    run.await.unwrap().unwrap();
    assert_eq!(finished.load(Ordering::SeqCst), 0);
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
    wait_for_published(&publisher, &seen, Duration::from_secs(5)).await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn included_router_handlers_dispatch() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen = Arc::new(AtomicU32::new(0));
    let seen_clone = Arc::clone(&seen);

    // Router defined independently of any live broker, then mounted. Consuming builder.
    let router = Router::<MemoryBroker>::new().subscribe(
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

    wait_for_published(&publisher, &seen, Duration::from_secs(5)).await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_layer_reaches_router_handlers() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let layer_hits = Arc::new(AtomicU32::new(0));
    let handler_hits = Arc::new(AtomicU32::new(0));
    let handler_hits_clone = Arc::clone(&handler_hits);

    // The app-global stack must reach handlers mounted through include_router.
    let router = Router::<MemoryBroker>::new().subscribe(
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
        .layer(CountLayer(Arc::clone(&layer_hits)))
        .with_broker(broker, |b| b.include_router(router));

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    wait_for_published(&publisher, &handler_hits, Duration::from_secs(5)).await;

    assert!(
        layer_hits.load(Ordering::SeqCst) >= 1,
        "global layer did not reach the router handler"
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
        Duration::from_secs(5),
    )
    .await;

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_broker_publish_via_captured_publisher() {
    let ingress = MemoryBroker::new();
    let egress = MemoryBroker::new();
    let ingress_pub = ingress.publisher();
    // Capture the egress broker's own publisher into a handler on the ingress broker - typed, no
    // registry.
    let egress_pub = egress.publisher();

    let received = Arc::new(AtomicU32::new(0));
    let received_clone = Arc::clone(&received);

    let app = RustStream::new(AppInfo::new("bridge", "0.1.0"))
        .with_broker(ingress, |b| {
            let out = egress_pub.clone();
            b.subscribe(
                Name::new("orders"),
                move |_msg: &_, _ctx: &mut Context| {
                    let out = out.clone();
                    async move {
                        let _ = out
                            .publish(OutgoingMessage::new("responses", b"reply".as_slice()))
                            .await;
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
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let _ = ingress_pub
                .publish(OutgoingMessage::new("orders", b"x"))
                .await;
            tokio::task::yield_now().await;
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
        .on_startup(|()| async {
            Ok::<_, std::convert::Infallible>(Config {
                greeting: "hello".to_owned(),
            })
        })
        .with_broker(broker, |b| {
            let subscriber = b.broker().subscribe("orders");
            b.handle(
                subscriber,
                move |_msg: &_, ctx: &mut Context<'_, (), Config>| {
                    let name = ctx.name().to_owned();
                    let greeting = ctx.state().greeting.clone();
                    // Middleware/handlers may enrich the working headers.
                    ctx.headers_mut().insert("x-seen", b"1".to_vec());
                    let seen = Arc::clone(&seen_clone);
                    async move {
                        *seen.lock().expect("poisoned") = Some((name, greeting));
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
        Duration::from_secs(5),
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
        .on_startup(move |()| {
            let o1 = Arc::clone(&o1);
            async move {
                o1.lock().expect("poisoned").push("startup");
                Ok::<Config, std::convert::Infallible>(Config {
                    greeting: "lazy".to_owned(),
                })
            }
        })
        .after_startup(move |_state: Arc<Config>| {
            let o2 = Arc::clone(&o2);
            async move {
                o2.lock().expect("poisoned").push("after_startup");
                Ok::<(), std::convert::Infallible>(())
            }
        })
        .on_shutdown(move |_state: Arc<Config>| {
            let o3 = Arc::clone(&o3);
            async move {
                o3.lock().expect("poisoned").push("on_shutdown");
                Ok::<(), std::convert::Infallible>(())
            }
        })
        .after_shutdown(move |state: Arc<Config>| {
            let o4 = Arc::clone(&o4);
            let greeting = state.greeting.clone();
            async move {
                assert_eq!(greeting.as_str(), "lazy");
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
        Duration::from_secs(5),
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
            // Yield to the scheduler; in multi-thread mode the handler runs in a different
            // thread and updates the atomic independently - no sleep needed for correctness.
            tokio::task::yield_now().await;
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
            // Yield once so the handler task has a chance to run before checking.
            tokio::task::yield_now().await;
            if seen.load(Ordering::SeqCst) >= 1 {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "no delivery within {timeout:?}");
}
