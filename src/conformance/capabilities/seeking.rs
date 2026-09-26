//! The seeking suites.

use futures::StreamExt;
use tokio::time::timeout;

use super::{
    DEFAULT_TIMEOUT, RESUBSCRIBE_TIMEOUT, SeekPosition, ack_or_unsupported, expect_within,
    promptly, publish_all,
};
use crate::conformance::harness::{expect_next, expect_no_more, on_foreign_runtime};
use crate::conformance::helpers::unique_subject;
use crate::{
    AckError, Broker, Connected, ConnectedBroker, IncomingMessage, OutgoingMessage, Positioned,
    Publisher, Seekable, Seeker, Subscriber, SubscriptionSource,
};

/// How many messages the first subscription reads before it starts seeking.
const SEEK_COUNT: u8 = 5;

/// Verifies the [`Seekable`] contract.
///
/// Positions captured from delivered messages ([`Positioned::position`]) follow the pinned
/// semantics: a seek back to a captured position redelivers exactly that message and the suffix
/// after it in order, a seek forward past queued deliveries skips them, and the subscription
/// keeps delivering new publishes after repositioning. The seeker is minted before the stream
/// borrows the subscriber, which is the shape the capability exists for.
///
/// A second subscription to the same subject then opens once the first is gone, and seeks right
/// away, before its first delivery, to a position the first one captured: that is what a
/// service's start position does at startup, and the first delivery must be the message at that
/// position, not the committed position or the tip. A seek issued from a current-thread runtime
/// that stops right after (a handler on a dedicated thread) repositions all the same, and a seek
/// through the seeker after the broker shut down returns an error.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemorySource, Retention};
/// use ruststream::nonzero;
///
/// capabilities::seeking(
///     || MemoryBroker::retaining(Retention::Messages(nonzero!(64))),
///     |name| MemorySource::new(name),
///     |broker| broker.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if any step violates the contract.
pub async fn seeking<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Seekable + Send,
    <Src::Subscriber as Subscriber>::Message:
        Positioned<Position = <<Src::Subscriber as Seekable>::Seeker as Seeker>::Position>,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.seeking");

    let connected = make_broker().connect().await.expect("broker must connect");
    let publisher = make_publisher(&connected);
    let first = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("subscription must open after connect");
    let (resume_at, replay_at) = seek_within_one_subscription(first, &publisher, &subject).await;

    // The second subscription seeks before its first delivery, the way a start position is
    // applied at startup: a broker that only honours a seek once the subscription has settled
    // (a consumer group assignment, a first fetch) reads from the committed position instead.
    let mut resumed = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("a second subscription to the same subject must open");
    let seeker = resumed.seeker();
    let after_shutdown =
        seek_on_fresh_subscription(&mut resumed, &seeker, resume_at, replay_at).await;

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");

    let refused = promptly(
        seeker.seek(after_shutdown),
        "seeking: a seek after shutdown",
    )
    .await;
    assert!(
        refused.is_err(),
        "seeking: a seek through a seeker that outlived the shutdown reported success; it must \
         return an error, never a reposition against the dead connection",
    );
}

/// Seeks back and forward within one subscription, and returns the positions of the last two
/// deliveries for the next subscription to seek to. The subscription is consumed, so it is gone
/// when this returns: on a broker whose subscriptions share a consumer group, the next one then
/// owns the log.
async fn seek_within_one_subscription<Sub, Pub>(
    mut subscriber: Sub,
    publisher: &Pub,
    subject: &str,
) -> (SeekPosition<Sub>, SeekPosition<Sub>)
where
    Sub: Seekable,
    Sub::Message: Positioned<Position = SeekPosition<Sub>>,
    Pub: Publisher,
{
    // Minted before `stream` borrows the subscriber; usable while the stream runs.
    let seeker = subscriber.seeker();

    for i in 0..SEEK_COUNT {
        publisher
            .publish(OutgoingMessage::new(subject, &[i]), None)
            .await
            .expect("publish failed");
    }

    let mut stream = std::pin::pin!(subscriber.stream());
    let mut positions = Vec::new();
    for i in 0..SEEK_COUNT {
        let msg = expect_next(&mut stream, "seeking: initial delivery").await;
        assert_eq!(
            msg.payload(),
            &[i],
            "initial deliveries must arrive in publish order",
        );
        positions.push(msg.position());
        match msg.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
    }
    expect_no_more(&mut stream, "seeking: after the initial drain").await;

    // Extraction order matters: swap_remove(4) pops the last element, which leaves the value
    // at index 1 in place for the second extraction.
    let forward_to = positions.swap_remove(4);
    let back_to = positions.swap_remove(1);

    seeker.seek(back_to).await.expect("seek back failed");
    let redelivered = expect_next(&mut stream, "seeking: after the seek back").await;
    assert_eq!(
        redelivered.payload(),
        &[1],
        "a seek back must redeliver the message at the captured position",
    );
    match redelivered.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
    // One more delivery pins the suffix: replaying only the sought message is not enough.
    let suffix = expect_next(&mut stream, "seeking: suffix after the seek back").await;
    assert_eq!(
        suffix.payload(),
        &[2],
        "a seek back must redeliver the ordered suffix after the captured position",
    );
    match suffix.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }

    // Delivery 3 is pending now; jumping to the captured position of 4 must skip it.
    seeker.seek(forward_to).await.expect("seek forward failed");
    let skipped_to = expect_next(&mut stream, "seeking: after the seek forward").await;
    assert_eq!(
        skipped_to.payload(),
        &[4],
        "a seek forward must skip the queued deliveries before the target",
    );
    let resume_at = skipped_to.position();
    match skipped_to.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
    expect_no_more(&mut stream, "seeking: after the forward target").await;

    publisher
        .publish(OutgoingMessage::new(subject, &[SEEK_COUNT]), None)
        .await
        .expect("publish failed");
    let live = expect_next(&mut stream, "seeking: after a new publish").await;
    assert_eq!(
        live.payload(),
        &[SEEK_COUNT],
        "the subscription must keep delivering new publishes after repositioning",
    );
    let replay_at = live.position();
    match live.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
    }
    (resume_at, replay_at)
}

/// A fresh subscription seeks right after it opens, to a position an earlier one captured, then
/// once more from another runtime; returns a position it captured, for the seek after shutdown.
async fn seek_on_fresh_subscription<Sub>(
    resumed: &mut Sub,
    seeker: &Sub::Seeker,
    resume_at: SeekPosition<Sub>,
    replay_at: SeekPosition<Sub>,
) -> SeekPosition<Sub>
where
    Sub: Seekable,
    Sub::Message: Positioned<Position = SeekPosition<Sub>>,
{
    seeker.seek(resume_at).await.unwrap_or_else(|err| {
        panic!(
            "seeking: a seek right after subscribing, before the first delivery, was refused: \
             {err}"
        )
    });
    let mut stream = std::pin::pin!(resumed.stream());
    let resumed_first = expect_within(
        &mut stream,
        RESUBSCRIBE_TIMEOUT,
        "seeking: a fresh subscription that sought before its first delivery",
        "a seek made right after subscribing must decide where the subscription starts",
    )
    .await;
    assert_eq!(
        resumed_first.payload(),
        &[SEEK_COUNT - 1],
        "seeking: a fresh subscription that sought right after subscribing to a position captured \
         on an earlier subscription must first deliver the message at that position, not read \
         from the committed position or the tip",
    );
    let after_shutdown = resumed_first.position();
    ack_or_unsupported(resumed_first, "seeking: a fresh subscription").await;
    let resumed_next =
        expect_next(&mut stream, "seeking: the suffix on a fresh subscription").await;
    assert_eq!(
        resumed_next.payload(),
        &[SEEK_COUNT],
        "seeking: after a seek on a fresh subscription the ordered suffix must follow",
    );
    ack_or_unsupported(resumed_next, "seeking: the suffix on a fresh subscription").await;
    expect_no_more(
        &mut stream,
        "seeking: after the suffix on a fresh subscription",
    )
    .await;

    // Issued from a runtime that stops right after, the way a handler on a dedicated thread
    // repositions its own subscription.
    let foreign = seeker.clone();
    on_foreign_runtime(async move || {
        foreign.seek(replay_at).await.unwrap_or_else(|err| {
            panic!("seeking: a seek from another runtime was refused: {err}")
        });
    })
    .await;
    let replayed = expect_within(
        &mut stream,
        DEFAULT_TIMEOUT,
        "seeking: after a seek from a runtime that has since stopped",
        "a seek made from a runtime that has since stopped must still reposition the \
         subscription; the broker must run its internal tasks on the runtime it connected on",
    )
    .await;
    assert_eq!(
        replayed.payload(),
        &[SEEK_COUNT],
        "seeking: a seek from another runtime must redeliver the message at the captured position",
    );
    ack_or_unsupported(replayed, "seeking: after a seek from another runtime").await;
    expect_no_more(&mut stream, "seeking: after a seek from another runtime").await;
    after_shutdown
}

/// Verifies that a seek to a position the subscription's log does not hold is refused.
///
/// The suite publishes three messages to a fresh subject and drains them, then seeks to the
/// position `make_unknown` builds for that subject: one the log cannot hold after those three
/// publishes, such as a position the broker's retention has already evicted or one on a
/// partition or shard the subject does not have. The seek must return an error, and the
/// subscription must not move: its next delivery is the next publish, not a replay from wherever
/// the broker fell back to. A broker that ends the subscription with the refusal passes as well.
///
/// A broker that answers such a seek by waiting at that position (a position past the tip)
/// passes `make_unknown` a position of the other kinds.
///
/// # Examples
///
/// The in-memory broker keeps two messages here, so after three publishes the first one's
/// position is evicted.
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySource, Retention};
/// use ruststream::nonzero;
///
/// capabilities::seeking_unknown_position(
///     || MemoryBroker::retaining(Retention::Messages(nonzero!(2))),
///     |name| MemorySource::new(name),
///     |broker| broker.publisher(),
///     |_subject| MemoryPosition::sequence(0),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if any step violates the contract.
pub async fn seeking_unknown_position<B, MkBroker, Src, MkSrc, Pub, MkPub, MkUnknown>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
    make_unknown: MkUnknown,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Seekable + Send,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
    MkUnknown: Fn(&str) -> SeekPosition<Src::Subscriber>,
{
    const LABEL: &str = "seeking_unknown_position";

    let subject = unique_subject("conformance.seeking_unknown_position");

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut subscriber = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("subscription must open after connect");
    let seeker = subscriber.seeker();
    let publisher = make_publisher(&connected);

    let sent: [&[u8]; 3] = [&[0], &[1], &[2]];
    publish_all(&publisher, &subject, &sent).await;
    let mut stream = std::pin::pin!(subscriber.stream());
    for expected in sent {
        let msg = expect_next(&mut stream, LABEL).await;
        assert_eq!(
            msg.payload(),
            expected,
            "{LABEL}: deliveries must arrive in publish order",
        );
        ack_or_unsupported(msg, LABEL).await;
    }

    let refused = promptly(
        seeker.seek(make_unknown(&subject)),
        "seeking_unknown_position: a seek to a position the log does not hold",
    )
    .await;
    assert!(
        refused.is_err(),
        "{LABEL}: a seek to a position the log does not hold reported success; it must return an \
         error rather than read from somewhere else",
    );

    // The subscription must not move. A broker that ends the subscription with the refusal has
    // read nothing from elsewhere either, so the end of the stream is an answer too; a delivery of
    // anything but the next publish is not.
    publish_all(&publisher, &subject, &[&[3]]).await;
    match timeout(DEFAULT_TIMEOUT, stream.next()).await {
        Ok(Some(Ok(next))) => {
            assert_eq!(
                next.payload(),
                &[3],
                "{LABEL}: a refused seek must not move the subscription; the next delivery must be \
                 the next publish, not a replay from wherever the broker fell back to",
            );
            ack_or_unsupported(next, LABEL).await;
            expect_no_more(&mut stream, LABEL).await;
        }
        Ok(None | Some(Err(_))) => {}
        Err(elapsed) => panic!(
            "{LABEL}: after a refused seek the next publish did not arrive and the subscription \
             did not end either ({elapsed})"
        ),
    }

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}
