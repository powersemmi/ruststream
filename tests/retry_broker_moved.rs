//! A delayed retry on a subscription whose broker moves a spent delivery itself and cannot hold a
//! delivery back.
//!
//! The runtime has no delay to hand over and no copy to publish, so it settles the delivery with a
//! requeue. Where the declaration sends the delivery elsewhere, the broker moves it on that
//! requeue and no delay applies. Where the delivery comes back to the subscription, the delay is
//! lost, and the runtime warns.
//!
//! The tests capture the log through one process-wide subscriber that writes into the capture the
//! test's own thread installed, so they run on the current-thread runtime, where the harness
//! drives every subscription on that thread.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::cell::RefCell;
use std::convert::Infallible;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Once, PoisonError};
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt};
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublisher,
    MemorySubscriber,
};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream, State};
use ruststream::testing::TestApp;
use ruststream::{
    AckError, BrokerMoves, FromRef, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    RetryDeclaration, Subscribe, Subscriber, SubscriptionSource, nonzero, subscriber,
};
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// A queue of a broker that counts its deliveries, moves the one that spends the cap to the
/// dead-letter destination itself, and cannot hold a delivery back for a delay.
#[derive(Debug, Clone)]
struct MovedQueue {
    name: &'static str,
    declared: RetryDeclaration,
}

impl MovedQueue {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            declared: RetryDeclaration::new(),
        }
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for MovedQueue {
    type Subscriber = MovedSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.name
    }

    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.declared = declaration.clone();
        self
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<MovedSubscriber, MemoryError> {
        // A dead-letter policy of this kind takes the cap and the destination together.
        let rule = self
            .declared
            .max_attempts()
            .zip(self.declared.dead_letter())
            .map(|(cap, dead)| (u64::from(cap.get()), dead.to_owned()));
        Ok(MovedSubscriber {
            inner: connected.subscribe(self.name).await?,
            publisher: connected.publisher(),
            rule,
        })
    }
}

/// The queue's subscriber, handing every delivery the rule the broker moves it by.
struct MovedSubscriber {
    inner: MemorySubscriber,
    publisher: MemoryPublisher,
    rule: Option<(u64, String)>,
}

impl Subscriber for MovedSubscriber {
    type Message = MovedMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<MovedMessage, Infallible>> + Send + '_ {
        let Self {
            inner,
            publisher,
            rule,
        } = self;
        inner.stream().map(move |item| {
            item.map(|inner| MovedMessage {
                inner,
                publisher: publisher.clone(),
                rule: rule.clone(),
            })
        })
    }
}

/// A delivery that reports the broker's own count and keeps the default, unsupported delayed
/// redelivery.
struct MovedMessage {
    inner: MemoryMessage,
    publisher: MemoryPublisher,
    rule: Option<(u64, String)>,
}

impl IncomingMessage for MovedMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    fn redelivery_count(&self) -> Option<u64> {
        self.inner.redelivery_count()
    }

    async fn ack(self) -> Result<(), AckError> {
        self.inner.ack().await
    }

    /// A requeue of the delivery that spends the cap moves it to the dead-letter destination
    /// instead of bringing it back.
    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue
            && let Some((cap, dead)) = &self.rule
            && self
                .inner
                .redelivery_count()
                .is_some_and(|count| count >= *cap)
        {
            self.publisher
                .publish(OutgoingMessage::new(dead, self.inner.payload()), None)
                .await
                .map_err(|err| AckError::Broker(Box::new(err)))?;
            return self.inner.ack().await;
        }
        self.inner.nack(requeue).await
    }
}

/// Never ready: every delivery asks to come back after a delay.
#[subscriber(MovedQueue::new("orders"))]
async fn not_ready(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// What a handler counts its deliveries with, so that only the first one asks for a delay.
#[derive(Clone, FromRef)]
struct Deliveries {
    seen: Arc<AtomicU32>,
}

/// Asks for a delay on its first delivery and settles the one that comes back.
#[subscriber(MovedQueue::new("invoices"))]
async fn ready_on_return(order: &Order, State(seen): State<Arc<AtomicU32>>) -> HandlerOutcome {
    let _ = order.id;
    if seen.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// The warning's message, as the log renders it.
const DELAY_DROPPED: &str = "the delay is dropped";

/// The log lines written on one thread while its [`Installed`] guard lives, as text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

thread_local! {
    /// The capture the current thread's log lines go to, if a test installed one.
    static CURRENT: RefCell<Option<Captured>> = const { RefCell::new(None) };
}

/// Clears the current thread's capture when dropped.
struct Installed;

impl Drop for Installed {
    fn drop(&mut self) {
        CURRENT.with(|current| current.borrow_mut().take());
    }
}

impl Captured {
    /// Routes the current thread's log lines into this capture until the guard drops.
    ///
    /// The subscriber is process-wide, installed once, rather than a per-thread default: tracing
    /// caches a callsite's interest globally, and a thread with no subscriber of its own could
    /// register the warning's callsite as never enabled and silence it for every test.
    fn install(&self) -> Installed {
        static GLOBAL: Once = Once::new();
        GLOBAL.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(ThreadCapture)
                .with_ansi(false)
                .with_max_level(Level::WARN)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("no other global subscriber in this test binary");
        });
        CURRENT.with(|current| *current.borrow_mut() = Some(self.clone()));
        Installed
    }

    fn text(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }

    /// The warning lines, one per delivery requeued without its delay.
    fn dropped_delays(&self) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(DELAY_DROPPED))
            .map(str::to_owned)
            .collect()
    }
}

/// The global subscriber's writer: the current thread's capture, or nowhere.
struct ThreadCapture;

/// One log line's destination, resolved on the thread that wrote it.
struct LineWriter(Option<Captured>);

impl io::Write for LineWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(Captured(bytes)) = &self.0 {
            bytes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for ThreadCapture {
    type Writer = LineWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LineWriter(CURRENT.with(|current| current.borrow().clone()))
    }
}

/// A delivery that spends the cap is the broker's to move, and the requeue is what moves it: no
/// delay was dropped, so nothing warns that one was.
#[tokio::test]
async fn a_spent_delivery_is_moved_without_a_dropped_delay_warning() {
    let log = Captured::default();
    let _guard = log.install();
    let app =
        RustStream::new(AppInfo::new("moved", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(not_ready)
                .max_attempts(nonzero!(1u32))
                .dead_letter("orders.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));
    tb.broker::<MemoryBroker>()
        .published::<Order>("orders.dead")
        .assert_called_once()
        .with(&Order { id: 1 });
    assert_eq!(log.dropped_delays(), Vec::<String>::new(), "{}", log.text());
}

/// A delivery with attempts left comes back at once, without its delay, and the runtime says so,
/// naming the subscription.
#[tokio::test]
async fn a_delivery_with_attempts_left_warns_that_its_delay_is_dropped() {
    let log = Captured::default();
    let _guard = log.install();
    let deliveries = Deliveries {
        seen: Arc::new(AtomicU32::new(0)),
    };
    let app = RustStream::new(AppInfo::new("moved", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(deliveries))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(ready_on_return)
                .max_attempts(nonzero!(2u32))
                .dead_letter("invoices.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 2 })
        .to("invoices")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("invoices")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .published::<Order>("invoices.dead")
        .assert_not_called();
    let warnings = log.dropped_delays();
    assert_eq!(warnings.len(), 1, "{}", log.text());
    assert!(
        warnings[0].contains("subscription=invoices"),
        "the subscription is missing from: {}",
        warnings[0],
    );
}
