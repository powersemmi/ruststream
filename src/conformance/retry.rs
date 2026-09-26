//! Checks of a broker's retry path: where a retry copy goes, how the broker counts the deliveries
//! of one message, and the delivery limit and dead-letter destination a broker applies itself.
//!
//! A registration retries a delivery in one of three ways, and the subscription descriptor's
//! [`Copies`](crate::SubscriptionSource::Copies) says which. Each way has its check here:
//!
//! * [`AddressedCopies`](crate::AddressedCopies): the runtime publishes a `retry_after` copy to the
//!   address the descriptor reports. [`redelivery_address`] holds the broker to that address, and
//!   along the way to the count every delivery reports through
//!   [`redelivery_count`](crate::IncomingMessage::redelivery_count). Run it with the broker's own
//!   descriptor, and a second time with the bare [`Name`](crate::Name) source where the connected
//!   form declares `Subscribe::Copies = AddressedCopies`: that is how `#[subscriber("orders")]`
//!   retries.
//! * [`BrokerMoves`]: the broker applies the registration's `max_attempts(..)` and
//!   `dead_letter(..)` itself. [`broker_moves`] declares both, requeues a message until the cap
//!   runs out and expects it in the dead-letter destination, and holds the broker to refusing a
//!   declaration it cannot map.
//! * [`NamedCopies`](crate::NamedCopies): the mount site names the destination, so the broker has
//!   no address to answer for and nothing here to run.
//!
//! # Examples
//!
//! ```no_run
//! # #[cfg(feature = "memory")]
//! # async fn run() {
//! use ruststream::Name;
//! use ruststream::conformance::retry;
//! use ruststream::memory::{MemoryBroker, MemorySource};
//!
//! // The broker's own descriptor, then the bare name the attribute form subscribes with.
//! retry::redelivery_address(
//!     MemoryBroker::new,
//!     |name| MemorySource::new(name),
//!     |connected| connected.publisher(),
//! )
//! .await;
//! retry::redelivery_address(
//!     MemoryBroker::new,
//!     |name| Name::new(name.to_owned()),
//!     |connected| connected.publisher(),
//! )
//! .await;
//! # }
//! ```

use std::{fmt, num::NonZeroU32, time::Duration};

use futures::{Stream, StreamExt, stream};
use tokio::time::{Instant, timeout, timeout_at};

use super::harness::on_foreign_runtime;
use super::helpers::unique_subject;
use crate::{
    AckError, Broker, BrokerMoves, Connected, ConnectedBroker, HeaderMap, IncomingMessage,
    OutgoingMessage, Publisher, RedeliveryAddressed, RetryDeclaration, Subscriber,
    SubscriptionSource, runtime::RETRY_COUNT_HEADER,
};

/// How long a delivery the check expects may take, live included.
const ARRIVAL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the check waits for a broker-moved redelivery or dead-letter move: a server that
/// moves a message runs its own timers, and they are coarser than a publish.
const MOVE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long nothing has to arrive for "nothing more arrives" to hold.
const QUIET: Duration = Duration::from_millis(500);
/// How many deliveries past the expected ones a quiet window reads before it gives up: a broker
/// that redelivers without end fails instead of hanging the check.
const EXTRA_DELIVERIES: usize = 8;
/// The requeues [`redelivery_address`] counts the deliveries of the copy across.
const REQUEUES: u64 = 2;

/// A header of the check's own on the copy, beside the retry count, both of which the copy must
/// carry through the broker unchanged.
const COPY_HEADER: &str = "x-conformance-copy";
const COPY_HEADER_VALUE: &[u8] = b"retry";
/// What the runtime writes on the first copy of a delivery.
const FIRST_COPY_COUNT: &str = "1";

const CONTROL: &[u8] = b"control";
const COPY: &[u8] = b"copy";
const POISON: &[u8] = b"poison";

/// What a descriptor that addresses its own retry copies promises: a copy published to the
/// address it reports reaches the subscription that reported it, once per group, with its
/// headers.
///
/// The runtime publishes a `retry_after` copy exactly like this, so an address that reaches
/// nothing loses every delayed message. Only a descriptor declaring
/// [`AddressedCopies`](crate::AddressedCopies) has one; a
/// [`NamedCopies`](crate::NamedCopies) descriptor takes its destination from the mount site and
/// has nothing to check here.
///
/// The check opens two subscriptions from the one descriptor, the way two replicas of a service
/// read one subscription (a transport that refuses the second, such as a socket the first one
/// bound, is checked with the one), and publishes two messages through one publisher. The copy
/// goes first, to the reported address, from a current-thread runtime on a thread of its own that
/// stops right after: that is where a handler on dedicated threads publishes it from, and the
/// retry publisher's first use may be that one. It carries a [`RETRY_COUNT_HEADER`] of `1` and a
/// header of the check's own. A control message follows from the suite's runtime, to the subject
/// the descriptor was built from (the same promise [`lifecycle`](super::harness::lifecycle)
/// holds). The copy must then:
///
/// * reach the subscriptions, and reach each at most once;
/// * reach as many of the two as the control message did: exactly one where the two share their
///   deliveries (a queue, a consumer group), each of them where the transport hands every
///   subscription everything;
/// * arrive with both headers byte for byte.
///
/// The copy is then requeued twice with `nack(true)`. Where the delivery reports a
/// [`redelivery_count`](IncomingMessage::redelivery_count), it is `1` on the copy's first delivery
/// and one more on each redelivery; `None` is the honest answer of a transport that counts
/// nothing, and a requeue the transport reports as [`AckError::Unsupported`] ends the cycle.
///
/// Run it with the broker's own descriptor, and again with the bare [`Name`](crate::Name) source
/// where the connected form's `Subscribe::Copies` is [`AddressedCopies`](crate::AddressedCopies):
/// that answer makes the name the address of every `#[subscriber("orders")]` registration, and
/// only a publish to it proves the claim. The `Name` call compiles only where the broker made
/// that claim.
///
/// `make_source(subject)` builds the descriptor that reads what a publish to `subject` delivers,
/// as for [`lifecycle`](super::harness::lifecycle). The copy moves to the other runtime's thread
/// with the publisher, so the publisher is `'static`.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::Name;
/// use ruststream::conformance::harness;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// harness::redelivery_address(
///     MemoryBroker::new,
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
/// )
/// .await;
/// // The in-memory broker answers `AddressedCopies` for a bare name too.
/// harness::redelivery_address(
///     MemoryBroker::new,
///     |name| Name::new(name.to_owned()),
///     |connected| connected.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if the copy does not reach the subscription, reaches a group
/// more than once, loses a header, or the delivery count does not grow by one per requeue.
pub async fn redelivery_address<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: RedeliveryAddressed<Connected<B>> + Clone + Send + Sync,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher + 'static,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.redelivery");

    let connected = make_broker()
        .connect()
        .await
        .expect("broker must connect after synchronous construction");

    let source = make_source(&subject);
    let address = source
        .redelivery_address(&connected)
        .await
        .expect("reporting a redelivery address must not fail against a live connection");
    let mut first = source
        .clone()
        .subscribe(&connected)
        .await
        .expect("subscription source must open against the connected form");
    // A transport may refuse a second reader of one subscription (a bound socket is read by the
    // subscription that bound it); it has no group to hold to "exactly one", and the one reader
    // is checked alone.
    let mut second = source.subscribe(&connected).await.ok();
    let publisher = make_publisher(&connected);

    let mut headers = HeaderMap::new();
    headers.insert(COPY_HEADER, COPY_HEADER_VALUE);
    headers.insert(RETRY_COUNT_HEADER, FIRST_COPY_COUNT);
    // Published from a runtime that stops before the copy is read, the way a handler on a
    // dedicated thread publishes a retry copy, and as the publisher's first use: the runtime pairs
    // a retry publisher per registration, and a handler thread may be the first to send on it.
    let destination = address.as_str().to_owned();
    let publisher = on_foreign_runtime(async move || {
        publisher
            .publish(
                OutgoingMessage::new(&destination, COPY).with_headers(headers),
                None,
            )
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "redelivery_address: publishing the retry copy to the reported address \
                     {destination:?} failed: {err}"
                )
            });
        publisher
    })
    .await;
    // Then the control message from the suite's runtime, through the same publisher: what the
    // first publish attached must not have died with the runtime that made it.
    publisher
        .publish(OutgoingMessage::new(&subject, CONTROL), None)
        .await
        .expect(
            "redelivery_address: publishing the control message to the descriptor's subject after \
             the retry copy failed",
        );

    let second = second.as_mut().map_or_else(
        || stream::pending().right_stream(),
        |second| second.stream().map(|item| (1_usize, item)).left_stream(),
    );
    let mut readers = std::pin::pin!(stream::select(
        first.stream().map(|item| (0_usize, item)),
        second,
    ));
    let mut seen = Seen::default();
    let deadline = Instant::now() + ARRIVAL_TIMEOUT;
    while seen.control_total() == 0 || seen.copy_total() == 0 {
        let Ok(next) = timeout_at(deadline, readers.next()).await else {
            panic!(
                "redelivery_address: {} within {ARRIVAL_TIMEOUT:?}; a copy published to the \
                 reported address {address} must reach the subscription that reported it",
                seen.missing(),
            );
        };
        let (reader, msg) = unwrap_delivery(next, "redelivery_address");
        seen.take(reader, msg, address.as_str()).await;
    }
    for _ in 0..EXTRA_DELIVERIES {
        match timeout(QUIET, readers.next()).await {
            Err(_) => break,
            Ok(next) => {
                let (reader, msg) = unwrap_delivery(next, "redelivery_address");
                seen.take(reader, msg, address.as_str()).await;
            }
        }
    }
    seen.assert_once_per_group(address.as_str());

    let (_, copy) = seen
        .held
        .take()
        .expect("the loop above stops only once a copy arrived");
    count_across_requeues(copy, &mut readers).await;

    let _closed = connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}

/// What the two subscriptions of [`redelivery_address`] received.
struct Seen<M> {
    control: [u32; 2],
    copies: [u32; 2],
    /// The first copy, kept unsettled for the requeue cycle.
    held: Option<(usize, M)>,
}

impl<M> Default for Seen<M> {
    fn default() -> Self {
        Self {
            control: [0; 2],
            copies: [0; 2],
            held: None,
        }
    }
}

impl<M: IncomingMessage> Seen<M> {
    fn control_total(&self) -> u32 {
        self.control.iter().sum()
    }

    fn copy_total(&self) -> u32 {
        self.copies.iter().sum()
    }

    fn missing(&self) -> &'static str {
        match (self.control_total(), self.copy_total()) {
            (0, 0) => "neither the control message nor the copy arrived",
            (0, _) => "the control message published to the descriptor's subject never arrived",
            _ => "the copy never arrived",
        }
    }

    async fn take(&mut self, reader: usize, msg: M, address: &str) {
        if msg.payload() == CONTROL {
            self.control[reader] += 1;
            settle_ack(msg, "redelivery_address control").await;
            return;
        }
        assert_eq!(
            msg.payload(),
            COPY,
            "redelivery_address: a subscription received a message the check never published",
        );
        self.copies[reader] += 1;
        assert_eq!(
            msg.headers().get(COPY_HEADER),
            Some(COPY_HEADER_VALUE),
            "redelivery_address: the copy published to {address} lost the header {COPY_HEADER}; \
             a retry copy carries the headers the handler saw",
        );
        assert_eq!(
            msg.headers().get_str(RETRY_COUNT_HEADER),
            Some(FIRST_COPY_COUNT),
            "redelivery_address: the copy published to {address} lost {RETRY_COUNT_HEADER}; the \
             runtime counts a registration's attempts with it",
        );
        if self.held.is_none() {
            self.held = Some((reader, msg));
        } else {
            settle_ack(msg, "redelivery_address copy").await;
        }
    }

    fn assert_once_per_group(&self, address: &str) {
        for reader in 0..2 {
            assert!(
                self.control[reader] <= 1 && self.copies[reader] <= 1,
                "redelivery_address: one subscription received the control message {} and the \
                 copy {} times; each is published once",
                self.control[reader],
                self.copies[reader],
            );
        }
        assert_eq!(
            self.copy_total(),
            self.control_total(),
            "redelivery_address: the copy published to {address} reached {} of two subscriptions \
             of one descriptor where a publish to its subject reached {}; exactly one consumer of \
             a group gets a retry copy, as it gets any delivery",
            self.copy_total(),
            self.control_total(),
        );
    }
}

/// Requeues `msg` [`REQUEUES`] times, reading each redelivery back from `readers`, and holds a
/// reported delivery count to one more per delivery.
async fn count_across_requeues<S, M, E>(mut msg: M, readers: &mut S)
where
    S: Stream<Item = (usize, Result<M, E>)> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    assert_count(&msg, 1, "the copy's first delivery");
    for delivery in 2..=REQUEUES + 1 {
        match msg.nack(true).await {
            Ok(()) => {}
            // A transport with no requeue has no redelivery to count.
            Err(AckError::Unsupported) => return,
            Err(other) => panic!(
                "redelivery_address: nack(requeue = true) must succeed or be unsupported, got: \
                 {other:?}"
            ),
        }
        let again = next_copy(readers).await;
        assert_count(&again, delivery, "a redelivery after nack(true)");
        msg = again;
    }
    settle_ack(msg, "redelivery_address requeued copy").await;
}

/// The next delivery of the copy after a requeue.
///
/// A transport that commits a position rather than single messages may deliver the settled control
/// message again while it rewinds to the requeued copy before it: that is its at-least-once
/// answer, so such a delivery is settled and passed over. A deadline bounds the whole wait.
async fn next_copy<S, M, E>(readers: &mut S) -> M
where
    S: Stream<Item = (usize, Result<M, E>)> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let deadline = Instant::now() + ARRIVAL_TIMEOUT;
    for _ in 0..EXTRA_DELIVERIES {
        let next = timeout_at(deadline, readers.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "redelivery_address: a copy requeued with nack(true) did not come back within \
                     {ARRIVAL_TIMEOUT:?}; Ok from a requeue promises a redelivery",
                )
            });
        let (_, again) = unwrap_delivery(next, "redelivery_address requeue");
        if again.payload() == CONTROL {
            settle_ack(again, "redelivery_address control redelivered").await;
            continue;
        }
        assert_eq!(
            again.payload(),
            COPY,
            "redelivery_address: nack(true) must redeliver the requeued copy",
        );
        return again;
    }
    panic!(
        "redelivery_address: {EXTRA_DELIVERIES} deliveries of the control message arrived and \
         none of the requeued copy"
    );
}

/// Where a delivery reports how often it was delivered, it is `expected`.
fn assert_count<M: IncomingMessage>(msg: &M, expected: u64, which: &str) {
    if let Some(count) = msg.redelivery_count() {
        assert_eq!(
            count, expected,
            "redelivery_count on {which} must be {expected}: it counts the deliveries of one \
             message, the first one included, and a registration's max_attempts is spent against \
             it",
        );
    }
}

/// What a broker that moves a spent delivery itself promises: the cap and the dead-letter
/// destination a registration declares are applied, and a declaration it cannot apply is refused
/// at startup.
///
/// On a [`BrokerMoves`] descriptor nothing in the service counts attempts or publishes a copy:
/// the registration's `max_attempts(..)` and `dead_letter(..)` reach the descriptor, and the broker
/// alone keeps the message from circling forever. The check declares `attempts` and a
/// dead-letter destination the way the runtime does
/// ([`declare_retry`](SubscriptionSource::declare_retry), then
/// [`declare_retry_on`](SubscriptionSource::declare_retry_on), then `subscribe`), publishes one
/// message and requeues every delivery of it with `nack(true)`. It expects exactly `attempts`
/// deliveries on the subscription, then the message in the dead-letter destination, once, and
/// nowhere after that. Where a delivery reports a
/// [`redelivery_count`](IncomingMessage::redelivery_count) it is the delivery's number, from `1`.
///
/// A native dead-letter policy needs the cap and the destination together, so the check also
/// declares each half alone and expects the broker to refuse it at startup, from
/// `declare_retry_on` or from `subscribe`: a half that opens a subscription honours neither half,
/// and a service counting on it loses the messages it thinks are capped.
///
/// `make_source(name)` builds the descriptor that reads what a publish to `name` delivers,
/// creating whatever the broker needs for that. The check calls it for the dead-letter
/// destination too, before it declares the cap, so the destination exists and is read from the
/// start. Pass `attempts` within what the broker accepts (a Pub/Sub dead-letter policy takes 5
/// to 100). The names the check makes contain letters, digits and `-` only, which every broker's
/// queue, topic and subject grammar admits.
///
/// Run it with the broker's descriptor, and with the bare [`Name`](crate::Name) source where the
/// connected form's `Subscribe::Copies` is [`BrokerMoves`]: the name then carries the declaration
/// through `Subscribe::declare_retry`.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroU32;
///
/// use ruststream::conformance::retry;
/// use ruststream::{Broker, BrokerMoves, Connected, Publisher, SubscriptionSource, nonzero};
///
/// // A broker crate passes its production broker, its queue descriptor and its publisher.
/// async fn check<B, Src, Pub>(
///     make_broker: impl Fn() -> B,
///     make_source: impl Fn(&str) -> Src,
///     make_publisher: impl Fn(&Connected<B>) -> Pub,
/// ) where
///     B: Broker,
///     Src: SubscriptionSource<Connected<B>, Copies = BrokerMoves> + Send,
///     Pub: Publisher,
/// {
///     let attempts: NonZeroU32 = nonzero!(2u32);
///     retry::broker_moves(make_broker, make_source, make_publisher, attempts).await;
/// }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if a half declaration opens a subscription, the full one is
/// refused, the message is delivered more or fewer than `attempts` times, it does not reach the
/// dead-letter destination or reaches it more than once, or a reported delivery count is off.
pub async fn broker_moves<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
    attempts: NonZeroU32,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>, Copies = BrokerMoves> + Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    for (what, half) in half_declarations(attempts) {
        let connected = make_broker()
            .connect()
            .await
            .expect("broker must connect after synchronous construction");
        let source = make_source(&dashed("conformance-half")).declare_retry(&half);
        refuses_at_startup(source, &connected, &half, what).await;
        let _closed = connected
            .shutdown()
            .await
            .expect("broker must shut down cleanly");
    }

    let connected = make_broker()
        .connect()
        .await
        .expect("broker must connect after synchronous construction");
    let subject = dashed("conformance-moves");
    let dead = dashed("conformance-dead");
    let mut dead_letters = make_source(&dead)
        .subscribe(&connected)
        .await
        .expect("the dead-letter destination must open as a subscription of its own");
    let declared = RetryDeclaration::new()
        .with_max_attempts(attempts)
        .with_dead_letter(dead.clone());
    let source = make_source(&subject).declare_retry(&declared);
    source
        .declare_retry_on(&connected, &declared)
        .unwrap_or_else(|err| {
            panic!(
                "broker_moves: a declaration of max_attempts({attempts}) and dead_letter({dead:?}) \
                 was refused: {err}"
            )
        });
    let mut subscription = source.subscribe(&connected).await.unwrap_or_else(|err| {
        panic!(
            "broker_moves: the subscription declaring max_attempts({attempts}) and \
             dead_letter({dead:?}) did not open: {err:?}"
        )
    });
    make_publisher(&connected)
        .publish(OutgoingMessage::new(&subject, POISON), None)
        .await
        .expect("broker_moves: publish to the subscription's name failed");

    // Both are read at once: a broker or client that moves a message on its next receive needs
    // the subscription read while the dead-letter destination is awaited.
    let mut deliveries = std::pin::pin!(stream::select(
        subscription
            .stream()
            .map(|item| (Place::Subscription, item)),
        dead_letters.stream().map(|item| (Place::DeadLetter, item)),
    ));
    moves_at_the_cap(&mut deliveries, attempts).await;

    let _closed = connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}

/// Each half of a dead-letter policy on its own, with what it leaves out.
fn half_declarations(attempts: NonZeroU32) -> [(&'static str, RetryDeclaration); 2] {
    [
        (
            "max_attempts(..) with no dead_letter(..)",
            RetryDeclaration::new().with_max_attempts(attempts),
        ),
        (
            "dead_letter(..) with no max_attempts(..)",
            RetryDeclaration::new().with_dead_letter(dashed("conformance-dead")),
        ),
    ]
}

/// A declaration the broker cannot map fails the registration at startup: from
/// `declare_retry_on`, or from `subscribe`.
async fn refuses_at_startup<C, Src>(source: Src, connected: &C, half: &RetryDeclaration, what: &str)
where
    C: ConnectedBroker,
    Src: SubscriptionSource<C> + Send,
{
    if source.declare_retry_on(connected, half).is_err() {
        return;
    }
    let opened = source.subscribe(connected).await;
    assert!(
        opened.is_err(),
        "broker_moves: a registration declaring {what} opened its subscription; a broker that \
         moves spent deliveries itself maps the cap and the destination together, and refuses a \
         declaration it cannot map at startup instead of ignoring it",
    );
}

/// Requeues every delivery until the cap is spent, then expects the message in the dead-letter
/// destination once, and nothing after it.
async fn moves_at_the_cap<S, M, E>(deliveries: &mut S, attempts: NonZeroU32)
where
    S: Stream<Item = (Place, Result<M, E>)> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    for delivery in 1..=u64::from(attempts.get()) {
        let (place, msg) = next_move(deliveries, "a delivery of the message").await;
        assert_eq!(
            place,
            Place::Subscription,
            "broker_moves: the message reached the dead-letter destination after {} deliveries; \
             the registration declared max_attempts({attempts})",
            delivery - 1,
        );
        assert_eq!(msg.payload(), POISON, "broker_moves: an unexpected message");
        assert_count(
            &msg,
            delivery,
            "a delivery the broker counts against the cap",
        );
        if let Err(err) = msg.nack(true).await {
            panic!(
                "broker_moves: nack(requeue = true) on delivery {delivery} of \
                 max_attempts({attempts}) must succeed on a broker that counts the attempts \
                 itself, got: {err:?}"
            );
        }
    }
    let (place, moved) = next_move(deliveries, "the dead-lettered message").await;
    assert_eq!(
        place,
        Place::DeadLetter,
        "broker_moves: the message was delivered {} times; the registration declared \
         max_attempts({attempts}), after which it goes to the dead-letter destination",
        u64::from(attempts.get()) + 1,
    );
    assert_eq!(
        moved.payload(),
        POISON,
        "broker_moves: the dead-letter destination received a message the check never published",
    );
    settle_ack(moved, "broker_moves dead letter").await;
    if let Ok(next) = timeout(QUIET, deliveries.next()).await {
        let (place, _) = unwrap_delivery(next, "broker_moves");
        panic!(
            "broker_moves: after the message reached the dead-letter destination it arrived again \
             on {place}; a spent message is moved once and delivered nowhere else"
        );
    }
}

/// Where [`broker_moves`] read a delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    Subscription,
    DeadLetter,
}

impl fmt::Display for Place {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Subscription => "the subscription",
            Self::DeadLetter => "the dead-letter destination",
        })
    }
}

async fn next_move<S, M, E>(deliveries: &mut S, what: &str) -> (Place, M)
where
    S: Stream<Item = (Place, Result<M, E>)> + Unpin,
    E: fmt::Debug,
{
    let next = timeout(MOVE_TIMEOUT, deliveries.next())
        .await
        .unwrap_or_else(|_| panic!("broker_moves: {what} did not arrive within {MOVE_TIMEOUT:?}"));
    unwrap_delivery(next, "broker_moves")
}

fn unwrap_delivery<Tag, M, E: fmt::Debug>(
    next: Option<(Tag, Result<M, E>)>,
    label: &str,
) -> (Tag, M) {
    let (tag, item) = next.unwrap_or_else(|| panic!("{label}: a subscription stream ended"));
    let msg =
        item.unwrap_or_else(|err| panic!("{label}: a subscription yielded an error: {err:?}"));
    (tag, msg)
}

/// A name built like [`unique_subject`] with `-` where it puts `.`, because the dead-letter
/// destination is named by the check and some queue grammars (SQS) refuse a dot.
fn dashed(prefix: &str) -> String {
    unique_subject(prefix).replace('.', "-")
}

/// Acknowledges `msg`, accepting a transport that cannot acknowledge.
async fn settle_ack<M: IncomingMessage>(msg: M, label: &str) {
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("{label}: ack must succeed or be unsupported, got: {other:?}"),
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests;
