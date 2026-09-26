//! The retry declaration over a bare subscription name: nothing at the mount site says whether
//! the broker maps a cap and a dead-letter destination onto the subscription the name opens, so
//! the broker's `Subscribe` answers for it at startup.
//!
//! The brokers here are the in-memory bus under a `Subscribe` that answers one way or another:
//! one moves a spent delivery itself and maps nothing, one maps the declaration, and one has its
//! copies published by this process and addressed by the mount site.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use common::Order;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::{Coordinator, InProcess, TestApp, TestableBroker};
use ruststream::{
    Broker, BrokerMoves, Connected, ConnectedBroker, DeclareRetryError, DefaultPublish,
    NamedCopies, OutgoingMessage, PairError, PublishPolicy, RawMessage, RetryDeclaration,
    Subscribe, nonzero, register_testable_broker, subscriber,
};

/// What a broker records when a registration mounted by a bare name declares its retries: the
/// subscription, and both halves of the declaration.
type Declared = Arc<Mutex<Vec<(String, RetryDeclaration)>>>;

/// A broker that moves a spent delivery itself and maps no declaration made over a bare
/// subscription name: its `Subscribe` keeps the default.
#[derive(Debug, Clone, Copy)]
struct Moved;

/// The same broker with a mechanism of its own, as Pub/Sub, SQS and Pulsar have one: it takes the
/// declaration for the subscription the name opens, and refuses half of one.
#[derive(Debug, Clone, Copy)]
struct Mapped;

/// A broker whose retry copies this process publishes but cannot address from a name: the mount
/// site says where they go, and the runtime applies the declaration.
#[derive(Debug, Clone, Copy)]
struct Unaddressed;

/// A broker over the in-memory bus whose `Subscribe` answers differently about retries: the
/// deliveries are the bus's, and `Answer` is the only difference between one of these and the
/// next.
struct Bus<Answer> {
    inner: MemoryBroker,
    declared: Declared,
    _answer: PhantomData<fn() -> Answer>,
}

impl<Answer> Bus<Answer> {
    fn new(declared: &Declared) -> Self {
        Self {
            inner: MemoryBroker::new(),
            declared: Arc::clone(declared),
            _answer: PhantomData,
        }
    }
}

/// The connected form of [`Bus`]: the live bus, and what the broker was told about retries.
struct ConnectedBus<Answer> {
    inner: Connected<MemoryBroker>,
    declared: Declared,
    _answer: PhantomData<fn() -> Answer>,
}

impl<Answer: Send + Sync + 'static> Broker for Bus<Answer> {
    type Error = <MemoryBroker as Broker>::Error;
    type Connected = ConnectedBus<Answer>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(ConnectedBus {
            inner: self.inner.connect().await?,
            declared: self.declared,
            _answer: PhantomData,
        })
    }
}

impl<Answer: Send + Sync + 'static> ConnectedBroker for ConnectedBus<Answer> {
    type Error = <Connected<MemoryBroker> as ConnectedBroker>::Error;
    type Closed = ();

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        self.inner.shutdown().await?;
        Ok(())
    }
}

/// The bus has no server, so the transition the harness connects through is its ordinary
/// `connect`.
impl<Answer: Send + Sync + 'static> InProcess for Bus<Answer> {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.connect()
    }
}

/// The harness drives the wrapped bus.
impl<Answer: Send + Sync + 'static> TestableBroker for ConnectedBus<Answer> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.inner.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.inner.inject(message);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.inner.published(name)
    }
}

register_testable_broker!(Bus<Moved>);
register_testable_broker!(Bus<Mapped>);
register_testable_broker!(Bus<Unaddressed>);

/// The bus's own publisher, paired through the broker wrapped around it.
#[derive(Debug, Default, Clone, Copy)]
struct BusPublish;

impl<Answer: Send + Sync + 'static> PublishPolicy<ConnectedBus<Answer>> for BusPublish {
    type Live = <MemoryPublish as PublishPolicy<Connected<MemoryBroker>>>::Live;

    fn pair(
        self,
        connected: &ConnectedBus<Answer>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> + Send {
        MemoryPublish.pair(&connected.inner)
    }
}

impl<Answer: Send + Sync + 'static> DefaultPublish for ConnectedBus<Answer> {
    type Policy = BusPublish;
}

impl Subscribe for ConnectedBus<Moved> {
    type Subscriber = <Connected<MemoryBroker> as Subscribe>::Subscriber;
    type Copies = BrokerMoves;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        self.inner.subscribe(name)
    }
}

impl Subscribe for ConnectedBus<Unaddressed> {
    type Subscriber = <Connected<MemoryBroker> as Subscribe>::Subscriber;
    type Copies = NamedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        self.inner.subscribe(name)
    }
}

impl Subscribe for ConnectedBus<Mapped> {
    type Subscriber = <Connected<MemoryBroker> as Subscribe>::Subscriber;
    type Copies = BrokerMoves;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        self.inner.subscribe(name)
    }

    /// What a broker with a native dead-letter policy does with a bare name: take the cap and the
    /// destination for the subscription this name opens, and refuse half a declaration the way a
    /// descriptor of its own would.
    fn declare_retry(
        &self,
        name: &str,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        if declaration.max_attempts().is_none() || declaration.dead_letter().is_none() {
            return Err(DeclareRetryError::Broker(Box::new(io::Error::other(
                format!("subscription `{name}` needs the cap and the destination together"),
            ))));
        }
        self.declared
            .lock()
            .expect("declaration mutex poisoned")
            .push((name.to_owned(), declaration.clone()));
        Ok(())
    }
}

/// Settles every delivery: these suites assert on startup, not on what the handler does.
#[subscriber("orders-workers")]
async fn moved(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// A cap and a destination declared over a bare name on a broker that moves a spent delivery
/// itself, and maps no declaration of its own, refuse to start: nothing here would apply them,
/// and the error says where they belong instead.
#[tokio::test]
async fn a_declaration_a_bare_name_carries_nowhere_refuses_to_start() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Moved>::new(&declared),
        |b| {
            b.include(moved)
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead");
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a declaration the broker maps nowhere must not start");
    let message = failed.to_string();
    assert!(message.contains("orders-workers"), "{message}");
    assert!(message.contains("ConnectedBus"), "{message}");
    assert!(
        message.contains(
            "moves a spent delivery itself and maps no retry declaration made over a bare \
             subscription name"
        ),
        "{message}"
    );
    assert!(
        message.contains("declare the cap and the destination on this broker's own descriptor"),
        "{message}"
    );
    assert!(
        declared
            .lock()
            .expect("declaration mutex poisoned")
            .is_empty(),
        "the refusal comes from the default, which records nothing",
    );
}

/// The same broker with a mechanism of its own starts, and both halves of the declaration reach
/// it against the subscription the name opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_that_maps_a_bare_name_takes_both_halves() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Mapped>::new(&declared),
        |b| {
            b.include(moved)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders.dead");
        },
    );
    let _tb = TestApp::start(app).await.expect("startup failed");

    let recorded = declared.lock().expect("declaration mutex poisoned").clone();
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].0, "orders-workers");
    assert_eq!(recorded[0].1.max_attempts().map(NonZeroU32::get), Some(5));
    assert_eq!(recorded[0].1.dead_letter(), Some("orders.dead"));
}

/// A broker that refuses what it cannot map refuses at startup, and its own reason reaches the
/// operator beside the subscription.
#[tokio::test]
async fn a_broker_that_refuses_half_a_declaration_refuses_to_start() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Mapped>::new(&declared),
        |b| {
            b.include(moved).max_attempts(nonzero!(5u32));
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a declaration the broker rejects must not start");
    let message = failed.to_string();
    assert!(message.contains("orders-workers"), "{message}");
    assert!(
        message.contains("needs the cap and the destination together"),
        "{message}"
    );
}

/// A broker whose copies are published from here but addressed by the mount site takes the
/// declaration the same way: the default accepts it, and nothing is asked of the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_on_an_unaddressed_broker_declares_with_a_named_destination() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Unaddressed>::new(&declared),
        |b| {
            b.include(moved)
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead")
                .out_retry(BusPublish)
                .to("orders.retry");
        },
    );
    let _tb = TestApp::start(app).await.expect("startup failed");

    assert!(
        declared
            .lock()
            .expect("declaration mutex poisoned")
            .is_empty(),
        "the runtime applies this one, so the broker is told nothing",
    );
}

/// A registration that declared nothing opens on the same broker: what the default refuses is a
/// declaration nobody would apply, not the subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_that_declares_nothing_opens_where_the_broker_moves_deliveries() {
    let declared = Declared::default();
    let app = RustStream::new(AppInfo::new("declared", "0.1.0")).with_broker(
        Bus::<Moved>::new(&declared),
        |b| {
            b.include(moved);
        },
    );
    let _tb = TestApp::start(app).await.expect("startup failed");
}
