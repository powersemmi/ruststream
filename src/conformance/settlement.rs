//! What a settlement means, checked against the transport a broker really runs on.
//!
//! [`run_suite`](super::harness::run_suite) holds the settlement answers of the in-process
//! transport. This suite holds the same meanings wherever the broker connects, the live server
//! included, and adds what only a real subscription can show:
//!
//! * `ack` consumes: the acknowledged delivery never comes back, neither after the broker's
//!   redelivery timeout nor on a new connection to the same subscription.
//! * `nack(false)` drops the delivery the same way.
//! * `Ok(())` from `nack(true)` means the delivery comes back.
//! * Settling out of order is safe: acknowledging a later delivery does not consume earlier ones
//!   left unsettled.
//! * An unsettled delivery is not consumed: dropped without a settlement, from a runtime that
//!   stops right after, it comes back.
//!
//! [`AckError::Unsupported`] stays an honest answer. A settlement answered that way is checked on
//! its own subscription only, since the transport never learned of it. A transport that cannot
//! requeue has no redelivery to observe, so the two checks that wait for one end where the
//! requeue reports itself unsupported.
//!
//! [`suite`] returns what each settlement answered, and [`matches_in_process`] runs it twice,
//! against the server and against the broker's in-process transport, and fails when the two
//! answer differently: an in-process transport that claims a settlement the server refuses
//! passes a handler's retry in a test and loses the message in production.
//!
//! [`lifecycle`](super::harness::lifecycle) holds the delay of
//! [`nack_after`](IncomingMessage::nack_after) as a floor for every broker whose deliveries offer
//! it: the delivery comes back no sooner than the delay, which is long enough to need a real
//! timer on a broker with whole-second granularity.
//!
//! # Examples
//!
//! ```no_run
//! # #[cfg(feature = "memory")]
//! # async fn run() {
//! use std::time::Duration;
//!
//! use ruststream::conformance::settlement;
//! use ruststream::memory::{MemoryBroker, MemorySource};
//!
//! // Clones of one broker: the second connection reaches what the first one left.
//! let broker = MemoryBroker::new();
//! settlement::matches_in_process(
//!     || broker.clone(),
//!     |name| MemorySource::new(name),
//!     |connected| connected.publisher(),
//!     Duration::ZERO,
//! )
//! .await;
//! # }
//! ```

use std::{fmt, pin::pin, time::Duration};

use futures::{Stream, StreamExt};
use tokio::time::{Instant, timeout, timeout_at};

use super::harness::{InProcessBroker, on_foreign_runtime};
use super::helpers::unique_subject;
use crate::{
    AckError, Broker, Connected, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher,
    Subscriber, SubscriptionSource, testing::InProcess,
};

/// The message type a subscriber yields.
type SubscriberMessage<S> = <S as Subscriber>::Message;

/// How long past the broker's redelivery timeout a check waits before it calls a delivery gone.
const QUIET_MARGIN: Duration = Duration::from_secs(1);
/// How long past the broker's redelivery timeout a check waits for a delivery it expects. Generous,
/// because a new connection may join a consumer group or wait for an assignment first, and only a
/// failing run pays it.
const ARRIVAL: Duration = Duration::from_secs(10);
/// The payload a check publishes to learn that everything published before it was delivered.
const SENTINEL: &[u8] = b"conformance-sentinel";

/// What a delivery answered when it was settled one way.
///
/// # Examples
///
/// ```
/// use ruststream::conformance::settlement::Answer;
///
/// assert_ne!(Answer::Settled, Answer::Unsupported);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Answer {
    /// The settlement returned `Ok(())`, and the suite held it to its meaning.
    Settled,
    /// The settlement returned [`AckError::Unsupported`]: the transport cannot perform it.
    Unsupported,
}

/// What a broker's deliveries answered to each settlement in one [`suite`] run.
///
/// Two runs of one broker, against the server and in process, must return the same answers;
/// [`matches_in_process`] compares them.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use std::time::Duration;
///
/// use ruststream::conformance::settlement::{self, Answer};
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// let broker = MemoryBroker::new();
/// let answers = settlement::suite(
///     || broker.clone(),
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
///     Duration::ZERO,
/// )
/// .await;
/// assert_eq!(answers.requeue, Answer::Settled);
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Answers {
    /// What `ack` answered.
    pub ack: Answer,
    /// What `nack(false)` answered.
    pub reject: Answer,
    /// What `nack(true)` answered.
    pub requeue: Answer,
    /// What [`supports_nack_after`](IncomingMessage::supports_nack_after) reported.
    pub delay: bool,
}

/// Holds `ack`, `nack` and an unsettled drop to their meaning on whatever transport the broker
/// connects to, and returns what each settlement answered.
///
/// Run it from the broker crate against a real server; wrap the broker in
/// [`InProcessBroker`] to run it in process, or call [`matches_in_process`] to run both and
/// compare the answers.
///
/// The inputs:
/// * `make_broker` is called once per connection, and a check connects twice to the same
///   subscription, so every broker it returns must reach the same server: the same address for a
///   live run, clones of one broker in process.
/// * `make_source` builds the descriptor a check subscribes with. The same descriptor opens the
///   subscription again on the second connection, so it names a subscription that outlives a
///   connection where the broker has one (a durable consumer, a queue, a consumer group). It must
///   let three deliveries be in flight at once.
/// * `make_publisher` produces the publisher a check publishes with.
/// * `redelivery_timeout` is how long the broker takes to hand back a delivery nobody settled:
///   the ack wait, visibility timeout or ack deadline the descriptor configures. A broker that
///   hands such a delivery back only when the connection closes passes [`Duration::ZERO`].
///
/// Every check publishes under a subject of its own (see [`unique_subject`]), so a run is
/// repeatable against one server. Each begins with `conformance.settlement.`, which is what to
/// provision where the server needs a subject set up first (a stream, a topic pattern).
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use std::time::Duration;
///
/// use ruststream::conformance::harness::InProcessBroker;
/// use ruststream::conformance::settlement;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// let broker = MemoryBroker::new();
/// settlement::suite(
///     || InProcessBroker::new(broker.clone()),
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
///     Duration::ZERO,
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a message naming the check when a settlement fails with anything other than
/// [`AckError::Unsupported`], an acknowledged or dropped delivery comes back, a requeue reported
/// done never comes back, a delivery left unsettled while a later one was acknowledged is lost,
/// or a delivery dropped unsettled is lost.
// A check is awaited on the test's own task and never spawned, so the caller's factories, which
// every connection of the run borrows, need not be `Send` or `Sync`.
#[allow(clippy::future_not_send)]
pub async fn suite<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
    redelivery_timeout: Duration,
) -> Answers
where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Clone + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    SubscriberMessage<Src::Subscriber>: 'static,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    Probes {
        make_broker: &make_broker,
        make_source: &make_source,
        make_publisher: &make_publisher,
        redelivery_timeout,
        run: "",
    }
    .answers()
    .await
}

/// Runs [`suite`] against the server and against the broker's in-process transport, and fails
/// when the two answer any settlement differently.
///
/// The in-process run wraps each broker `make_broker` returns in [`InProcessBroker`], so the
/// descriptor and the publisher are the production ones and only the transport underneath
/// differs. Everything [`suite`] says about its inputs holds for both runs.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use std::time::Duration;
///
/// use ruststream::conformance::settlement;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// let broker = MemoryBroker::new();
/// settlement::matches_in_process(
///     || broker.clone(),
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
///     Duration::ZERO,
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics when either run fails [`suite`], and when the in-process answers differ from the live
/// ones.
// A check is awaited on the test's own task and never spawned, so the caller's factories, which
// every connection of the run borrows, need not be `Send` or `Sync`.
#[allow(clippy::future_not_send)]
pub async fn matches_in_process<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
    redelivery_timeout: Duration,
) where
    B: InProcess,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Clone + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    SubscriberMessage<Src::Subscriber>: 'static,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let live = Probes {
        make_broker: &make_broker,
        make_source: &make_source,
        make_publisher: &make_publisher,
        redelivery_timeout,
        run: "live ",
    }
    .answers()
    .await;
    let in_process = Probes {
        make_broker: &|| InProcessBroker::new(make_broker()),
        make_source: &make_source,
        make_publisher: &make_publisher,
        redelivery_timeout,
        run: "in process ",
    }
    .answers()
    .await;
    assert_eq!(
        in_process, live,
        "settlement: the in-process transport answers settlements differently from the server \
         (left: in process, right: live); a test on it passes where production fails",
    );
}

/// Which settlement a consumption check performs.
#[derive(Debug, Clone, Copy)]
enum Consume {
    Ack,
    Reject,
}

impl Consume {
    const fn label(self) -> &'static str {
        match self {
            Self::Ack => "settlement ack consumes",
            Self::Reject => "settlement nack(false) drops",
        }
    }
}

/// The caller's factories and the broker's redelivery timeout, borrowed for one [`suite`] run.
struct Probes<'a, MkBroker, MkSrc, MkPub> {
    make_broker: &'a MkBroker,
    make_source: &'a MkSrc,
    make_publisher: &'a MkPub,
    redelivery_timeout: Duration,
    /// Which run of [`matches_in_process`] this is, as a panic prefix; empty for [`suite`].
    run: &'static str,
}

// Awaited on the test's own task like the suite that holds it; see `suite`.
#[allow(clippy::future_not_send)]
impl<B, MkBroker, Src, MkSrc, Pub, MkPub> Probes<'_, MkBroker, MkSrc, MkPub>
where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Clone + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    SubscriberMessage<Src::Subscriber>: 'static,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    /// Every check in order, and what each settlement answered.
    async fn answers(&self) -> Answers {
        let (ack, delay) = self.consumed_by(Consume::Ack).await;
        let (reject, _) = self.consumed_by(Consume::Reject).await;
        let requeue = self.requeue_returns().await;
        // A transport that cannot requeue has no redelivery to observe: what it drops is gone,
        // which is its honest behaviour, and the checks below have nothing to wait for.
        if requeue == Answer::Settled {
            self.out_of_order_is_safe().await;
            self.unsettled_drop_returns().await;
        }
        Answers {
            ack,
            reject,
            requeue,
            delay,
        }
    }

    /// The name a panic gives `check`, with the run it belongs to.
    fn label(&self, check: &str) -> String {
        format!("{}{check}", self.run)
    }

    /// A new connection, and the subscription `source` opens on it.
    async fn open(&self, source: &Src, label: &str) -> (Connected<B>, Src::Subscriber) {
        let connected = (self.make_broker)()
            .connect()
            .await
            .unwrap_or_else(|err| panic!("{label}: the broker must connect, got: {err:?}"));
        let subscriber = source
            .clone()
            .subscribe(&connected)
            .await
            .unwrap_or_else(|err| panic!("{label}: the subscription must open, got: {err:?}"));
        (connected, subscriber)
    }

    /// Closes the subscription, then the connection it was opened on.
    async fn close(connected: Connected<B>, subscriber: Src::Subscriber, label: &str) {
        drop(subscriber);
        if let Err(err) = connected.shutdown().await {
            panic!("{label}: the broker must shut down cleanly, got: {err:?}");
        }
    }

    /// A delivery settled with `how` never comes back: not on its subscription within the
    /// redelivery timeout, and not on a new connection to it. Returns the answer and whether the
    /// delivery offered a delayed nack.
    async fn consumed_by(&self, how: Consume) -> (Answer, bool) {
        let label = &self.label(how.label());
        let subject = unique_subject("conformance.settlement.consume");
        let source = (self.make_source)(&subject);

        let (connected, mut subscriber) = self.open(&source, label).await;
        publish(
            &(self.make_publisher)(&connected),
            &subject,
            b"settled",
            label,
        )
        .await;
        let (answer, delay) = {
            let mut stream = pin!(subscriber.stream());
            let msg = arrival(&mut stream, ARRIVAL, label).await;
            let delay = msg.supports_nack_after();
            let answer = match how {
                Consume::Ack => settle(msg.ack().await, label, "ack"),
                Consume::Reject => settle(msg.nack(false).await, label, "nack(false)"),
            };
            quiet(
                &mut stream,
                self.redelivery_timeout + QUIET_MARGIN,
                label,
                "the settled delivery came back on its subscription",
            )
            .await;
            (answer, delay)
        };
        Self::close(connected, subscriber, label).await;
        // A transport that settled nothing never learned the delivery was consumed, so what a new
        // connection reads is its start position's business, not a broken promise.
        if answer == Answer::Unsupported {
            return (answer, delay);
        }

        // A new connection to the same subscription: a settlement the broker kept only in the
        // closed connection shows up here as a redelivery ahead of the sentinel.
        let (connected, mut subscriber) = self.open(&source, label).await;
        publish(
            &(self.make_publisher)(&connected),
            &subject,
            SENTINEL,
            label,
        )
        .await;
        {
            let mut stream = pin!(subscriber.stream());
            until_sentinel(&mut stream, self.redelivery_timeout + ARRIVAL, label).await;
        }
        Self::close(connected, subscriber, label).await;
        (answer, delay)
    }

    /// `Ok(())` from `nack(true)` is a promise the delivery comes back.
    async fn requeue_returns(&self) -> Answer {
        let label = &self.label("settlement nack(true) returns");
        let subject = unique_subject("conformance.settlement.requeue");
        let source = (self.make_source)(&subject);
        let (connected, mut subscriber) = self.open(&source, label).await;
        publish(
            &(self.make_publisher)(&connected),
            &subject,
            b"requeued",
            label,
        )
        .await;
        let answer = {
            let mut stream = pin!(subscriber.stream());
            let msg = arrival(&mut stream, ARRIVAL, label).await;
            let answer = settle(msg.nack(true).await, label, "nack(true)");
            if answer == Answer::Settled {
                let again = arrival(&mut stream, self.redelivery_timeout + ARRIVAL, label).await;
                assert_eq!(
                    again.payload(),
                    b"requeued",
                    "{label}: nack(true) returned Ok, so the same delivery must come back",
                );
                settle(again.ack().await, label, "ack");
            }
            answer
        };
        Self::close(connected, subscriber, label).await;
        answer
    }

    /// Three deliveries in flight, the last one acknowledged and the first two dropped unsettled:
    /// the first two come back, on the subscription or on a new connection to it.
    async fn out_of_order_is_safe(&self) {
        let label = &self.label("settlement out of order");
        let subject = unique_subject("conformance.settlement.order");
        let source = (self.make_source)(&subject);
        let (connected, mut subscriber) = self.open(&source, label).await;
        let publisher = (self.make_publisher)(&connected);
        for payload in [b"first".as_slice(), b"second", b"third"] {
            publish(&publisher, &subject, payload, label).await;
        }
        drop(publisher);
        let (mut missing, acked) = {
            let mut stream = pin!(subscriber.stream());
            let mut held = Vec::with_capacity(2);
            for _ in 0..2 {
                held.push(arrival(&mut stream, ARRIVAL, label).await);
            }
            let last = arrival(&mut stream, ARRIVAL, label).await;
            let acked = last.payload().to_vec();
            settle(last.ack().await, label, "ack");
            let missing: Vec<Vec<u8>> = held.iter().map(|msg| msg.payload().to_vec()).collect();
            // Released unsettled, after the later delivery was acknowledged: a broker that commits
            // a position commits past them here.
            drop(held);
            (missing, acked)
        };
        self.expect_back(
            connected,
            subscriber,
            &source,
            &mut missing,
            Some(&acked),
            label,
        )
        .await;
    }

    /// A delivery dropped without a settlement, on a runtime that stops right after, comes back.
    async fn unsettled_drop_returns(&self) {
        let label = &self.label("settlement unsettled drop");
        let subject = unique_subject("conformance.settlement.unsettled");
        let source = (self.make_source)(&subject);
        let (connected, mut subscriber) = self.open(&source, label).await;
        publish(
            &(self.make_publisher)(&connected),
            &subject,
            b"unsettled",
            label,
        )
        .await;
        {
            let mut stream = pin!(subscriber.stream());
            let msg = arrival(&mut stream, ARRIVAL, label).await;
            // Released where a handler on a dedicated thread releases it: whatever the drop leaves
            // to a task on this runtime is cancelled with the runtime.
            on_foreign_runtime(async move || drop(msg)).await;
        }
        let mut missing = vec![b"unsettled".to_vec()];
        self.expect_back(connected, subscriber, &source, &mut missing, None, label)
            .await;
    }

    /// Waits for every payload in `missing` to come back: first on the open subscription within
    /// the redelivery timeout, then on the same subscription opened again on the same connection,
    /// then on a new connection to it. `tolerated` is an acknowledged payload that may be
    /// delivered again: a log commits a contiguous prefix, so the one acknowledged past an
    /// unsettled delivery comes back with it, a duplicate rather than a loss.
    async fn expect_back(
        &self,
        connected: Connected<B>,
        mut subscriber: Src::Subscriber,
        source: &Src,
        missing: &mut Vec<Vec<u8>>,
        tolerated: Option<&[u8]>,
        label: &str,
    ) {
        {
            let mut stream = pin!(subscriber.stream());
            collect_back(
                &mut stream,
                missing,
                tolerated,
                self.redelivery_timeout + QUIET_MARGIN,
                label,
            )
            .await;
        }
        drop(subscriber);
        if !missing.is_empty() {
            // A consumer that goes away hands back what it held: a queue requeues it, a group
            // reassigns it from the committed position.
            let mut subscriber = source
                .clone()
                .subscribe(&connected)
                .await
                .unwrap_or_else(|err| panic!("{label}: the subscription must open, got: {err:?}"));
            {
                let mut stream = pin!(subscriber.stream());
                collect_back(
                    &mut stream,
                    missing,
                    tolerated,
                    self.redelivery_timeout + ARRIVAL,
                    label,
                )
                .await;
            }
            drop(subscriber);
        }
        if let Err(err) = connected.shutdown().await {
            panic!("{label}: the broker must shut down cleanly, got: {err:?}");
        }
        if missing.is_empty() {
            return;
        }
        let (connected, mut subscriber) = self.open(source, label).await;
        {
            let mut stream = pin!(subscriber.stream());
            collect_back(
                &mut stream,
                missing,
                tolerated,
                self.redelivery_timeout + ARRIVAL,
                label,
            )
            .await;
        }
        Self::close(connected, subscriber, label).await;
        assert!(
            missing.is_empty(),
            "{label}: deliveries released without a settlement never came back, neither on their \
             subscription, nor on it opened again, nor on a new connection to it; the broker \
             consumed them without an ack. Lost: {:?}",
            missing
                .iter()
                .map(|payload| String::from_utf8_lossy(payload).into_owned())
                .collect::<Vec<_>>(),
        );
    }
}

/// Publishes `payload` under `subject`, panicking with `label` when the publish fails.
async fn publish<Pub: Publisher>(publisher: &Pub, subject: &str, payload: &[u8], label: &str) {
    if let Err(err) = publisher
        .publish(OutgoingMessage::new(subject, payload), None)
        .await
    {
        panic!("{label}: the publish must succeed, got: {err:?}");
    }
}

/// Reads a settlement's answer: `Unsupported` is an honest one, any other error fails the check.
fn settle(result: Result<(), AckError>, label: &str, what: &str) -> Answer {
    match result {
        Ok(()) => Answer::Settled,
        Err(AckError::Unsupported) => Answer::Unsupported,
        Err(other) => panic!("{label}: {what} must succeed or be unsupported, got: {other:?}"),
    }
}

/// Unwraps one stream item, naming the check when the stream ended or failed.
fn delivered<M, E: fmt::Debug>(item: Option<Result<M, E>>, label: &str) -> M {
    item.unwrap_or_else(|| panic!("{label}: the subscription stream ended"))
        .unwrap_or_else(|err| panic!("{label}: the subscription stream failed: {err:?}"))
}

/// The next delivery, within `within`.
async fn arrival<S, M, E>(stream: &mut S, within: Duration, label: &str) -> M
where
    S: Stream<Item = Result<M, E>> + Unpin,
    E: fmt::Debug,
{
    let item = timeout(within, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label}: no delivery arrived within {within:?}"));
    delivered(item, label)
}

/// Asserts nothing is delivered within `within`, the bounded negative wait of a check.
async fn quiet<S, M, E>(stream: &mut S, within: Duration, label: &str, what: &str)
where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    if let Ok(item) = timeout(within, stream.next()).await {
        let msg = delivered(item, label);
        panic!(
            "{label}: {what} within {within:?}: {:?}",
            String::from_utf8_lossy(msg.payload()),
        );
    }
}

/// Reads until the sentinel arrives and acknowledges it; any other delivery first fails the check.
async fn until_sentinel<S, M, E>(stream: &mut S, within: Duration, label: &str)
where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let msg = arrival(stream, within, label).await;
    assert!(
        msg.payload() == SENTINEL,
        "{label}: the settled delivery came back on a new connection to its subscription: {:?}",
        String::from_utf8_lossy(msg.payload()),
    );
    settle(msg.ack().await, label, "ack");
}

/// Collects redeliveries of `missing` until none is left or `within` runs out, acknowledging each.
async fn collect_back<S, M, E>(
    stream: &mut S,
    missing: &mut Vec<Vec<u8>>,
    tolerated: Option<&[u8]>,
    within: Duration,
    label: &str,
) where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let deadline = Instant::now() + within;
    while !missing.is_empty() {
        let Ok(item) = timeout_at(deadline, stream.next()).await else {
            return;
        };
        let msg = delivered(item, label);
        let payload = msg.payload();
        if let Some(found) = missing.iter().position(|expected| expected == payload) {
            missing.swap_remove(found);
        } else {
            assert!(
                tolerated == Some(payload),
                "{label}: an unexpected delivery arrived while waiting for the unsettled ones: \
                 {:?}",
                String::from_utf8_lossy(payload),
            );
        }
        settle(msg.ack().await, label, "ack");
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests;
