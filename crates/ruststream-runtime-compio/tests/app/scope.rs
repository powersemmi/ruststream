//! What a scope of the compio app mounts: a handler attached to a subscription that already exists,
//! a router, and the app-global layer that wraps both.

use std::convert::Infallible;
use std::future::{pending, ready};
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ruststream_core::{BytesMut, OutgoingMessage, Publisher};
use ruststream_memory::local::{LocalMemoryBroker, LocalMemoryPublish};
use ruststream_runtime::{
    AppInfo, BlanketLayer, Context, Handler, HandlerMetadata, HandlerOutcome, Router, subscriber,
};
use ruststream_runtime_compio::{Cores, RustStream, RustStreamError};

use super::support::{On, acks_of, direct, tapped, wire};

fn one_core() -> Cores {
    Cores::new(NonZeroUsize::MIN).unpinned().without_signals()
}

/// Counts the deliveries the handlers it wraps are given.
#[derive(Clone)]
struct Counting(Arc<AtomicUsize>);

struct Counted<Inner> {
    inner: Inner,
    count: Arc<AtomicUsize>,
}

impl<Message, Cx, State, Inner> Handler<Message, Cx, State> for Counted<Inner>
where
    Message: Sync,
    Cx: Send,
    State: Send + Sync,
    Inner: Handler<Message, Cx, State>,
{
    async fn handle(&self, msg: &Message, ctx: &mut Context<'_, Cx, State>) -> HandlerOutcome {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.handle(msg, ctx).await
    }
}

impl BlanketLayer for Counting {
    fn apply<Message, Cx, State, Inner>(
        &self,
        handler: Inner,
    ) -> impl Handler<Message, Cx, State> + 'static
    where
        Message: Send + Sync + 'static,
        Cx: Send + 'static,
        State: Send + Sync + 'static,
        Inner: Handler<Message, Cx, State> + 'static,
    {
        Counted {
            inner: handler,
            count: Arc::clone(&self.0),
        }
    }
}

#[test]
fn a_handler_attached_to_an_open_subscription_is_wrapped_by_the_global_layer() {
    let (open, line) = direct();
    let poster = line.poster();
    let mut line = line;
    let count = Arc::new(AtomicUsize::new(0));

    let got = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&got);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(Counting(Arc::clone(&count)))
        .after_startup(async move |_state: Arc<()>| {
            poster.post(3);
            Ok::<_, Infallible>(())
        })
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.handle(
                open,
                |_mail: &_, _ctx: &mut Context<'_>| ready(HandlerOutcome::ack()),
                HandlerMetadata::raw("direct"),
            )
        });

    app.run_until(one_core(), async move {
        *slot.lock().expect("got") = line.acks(1).await.first().copied();
    })
    .expect("the run ends cleanly");

    assert_eq!(*got.lock().expect("got"), Some(3));

    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn a_router_mounted_on_the_scope_serves_its_handlers_under_the_global_layer() {
    let log = super::support::Log::default();
    let (source, line) = wire("orders", &log);
    let poster = line.poster();
    let mut line = line;
    let count = Arc::new(AtomicUsize::new(0));
    let router = Router::<LocalMemoryBroker<8>>::new()
        .include(subscriber(source, On(|_| ready(Ok(())))).build());

    let got = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&got);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .layer(Counting(Arc::clone(&count)))
        .after_startup(async move |_state: Arc<()>| {
            poster.post(5);
            Ok::<_, Infallible>(())
        })
        .with_broker(LocalMemoryBroker::<8>::new(), |b| b.include_router(router));

    app.run_until(one_core(), async move {
        *slot.lock().expect("got") = line.acks(1).await.first().copied();
    })
    .expect("the run ends cleanly");

    assert_eq!(*got.lock().expect("got"), Some(5));

    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn a_scope_startup_publish_reaches_the_subscription_the_scope_opened() {
    let (source, mut heard) = tapped("seed");
    let got = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&got);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        LocalMemoryBroker::<8>::new(),
        |b| {
            b.after_startup(LocalMemoryPublish, async move |publisher| {
                publisher
                    .publish(
                        OutgoingMessage::produced(
                            "seed",
                            BytesMut::from(&9_usize.to_be_bytes()[..]),
                        ),
                        None,
                    )
                    .await
            })
            .include(subscriber(source, On(|_| ready(Ok(())))).build())
        },
    );

    app.run_until(one_core(), async move {
        *slot.lock().expect("got") = acks_of(&mut heard, 1).await.first().copied();
    })
    .expect("the run ends cleanly");

    assert_eq!(*got.lock().expect("got"), Some(9));
}

#[test]
fn a_scope_startup_publish_that_fails_aborts_the_start() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        LocalMemoryBroker::<8>::new(),
        |b| {
            b.after_startup(LocalMemoryPublish, async move |_publisher| {
                Err::<(), _>(io::Error::other("the seed was refused"))
            })
        },
    );

    let failure = app
        .run_until(one_core(), pending())
        .expect_err("the hook failed");

    assert!(
        matches!(&failure, RustStreamError::Startup(source) if source.to_string().contains("the seed was refused")),
        "{failure:?}"
    );
}
