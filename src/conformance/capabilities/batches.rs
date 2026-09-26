//! The batch suites: batching and settlement, and seeking a batch subscription.

use std::{fmt, num::NonZeroUsize};

use futures::{Stream, StreamExt};
use tokio::time::timeout;

use super::{
    DEFAULT_TIMEOUT, REDELIVERY_TIMEOUT, SeekPosition, SubscriberMessage, ack_or_unsupported,
    collect_batched, expect_no_batch, nack_requeue, payloads, publish_all,
};
use crate::conformance::harness::on_foreign_runtime;
use crate::conformance::helpers::unique_subject;
use crate::{
    AckError, BatchSubscriber, Broker, Connected, ConnectedBroker, IncomingMessage,
    OutgoingMessage, Positioned, Publisher, Seekable, Seeker, SubscriptionSource,
};

/// Verifies the [`BatchSubscriber`] contract.
///
/// Every published message arrives, in publish order, distributed over one or more non-empty
/// batches. The elements of one batch settle one by one: nacking one with requeue and acking the
/// rest brings back that one alone. A batch settled from a current-thread runtime that stops
/// right after (a batch handler on a dedicated thread) settles all the same: the requeued element
/// comes back. A transport that answers a requeue with [`AckError::Unsupported`] passes both
/// with nothing coming back.
///
/// The settling runtime is handed the delivered messages, so they are `'static`.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// capabilities::batches(
///     MemoryBroker::new,
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
pub async fn batches<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: BatchSubscriber + Send,
    SubscriberMessage<Src::Subscriber>: 'static,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    const COUNT: u32 = 10;
    // Smaller than the run, so a broker that ignores the size it is given is caught by the
    // batch-length assertion below rather than by luck of timing.
    const BATCH: NonZeroUsize = NonZeroUsize::new(3).unwrap();

    let subject = unique_subject("conformance.batches");

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut subscriber = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("subscription must open after connect");
    let publisher = make_publisher(&connected);

    for i in 0..COUNT {
        publisher
            .publish(
                OutgoingMessage::new(&subject, i.to_be_bytes().as_slice()),
                None,
            )
            .await
            .expect("publish failed");
    }

    let mut received = Vec::new();
    let mut stream = std::pin::pin!(subscriber.batches(BATCH));
    while received.len() < COUNT as usize {
        let batch = timeout(DEFAULT_TIMEOUT, stream.next())
            .await
            .expect("batches: stream timed out")
            .expect("batches: stream ended unexpectedly")
            .unwrap_or_else(|err| panic!("batches: stream yielded error: {err:?}"));

        let batch: Vec<_> = batch.into_iter().collect();
        assert!(!batch.is_empty(), "a yielded batch must not be empty");
        assert!(
            batch.len() <= BATCH.get(),
            "a batch must never carry more than the size it was opened with: got {}, asked {}",
            batch.len(),
            BATCH,
        );
        for msg in batch {
            received.push(msg.payload().to_vec());
            match msg.ack().await {
                Ok(()) | Err(AckError::Unsupported) => {}
                Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
            }
        }
    }

    let expected: Vec<Vec<u8>> = (0..COUNT).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_eq!(
        received, expected,
        "batched deliveries must preserve publish order across batches",
    );

    batch_settles_per_element(&publisher, &mut stream, &subject).await;
    batch_settles_on_foreign_runtime(&publisher, &mut stream, &subject).await;

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}

/// One batch settled element by element: the element nacked with requeue comes back, alone.
async fn batch_settles_per_element<Pub, S, Batch, M, E>(
    publisher: &Pub,
    stream: &mut S,
    subject: &str,
) where
    Pub: Publisher,
    S: Stream<Item = Result<Batch, E>> + Unpin,
    Batch: IntoIterator<Item = M>,
    M: IncomingMessage,
    E: fmt::Debug,
{
    const LABEL: &str = "batches: one batch settled element by element";

    publish_all(publisher, subject, &[b"keep-1", b"requeue", b"keep-2"]).await;
    let delivered = collect_batched(
        stream,
        3,
        DEFAULT_TIMEOUT,
        LABEL,
        "every published message must arrive",
    )
    .await;
    assert_eq!(
        payloads(&delivered),
        [b"keep-1".to_vec(), b"requeue".to_vec(), b"keep-2".to_vec()],
        "{LABEL}: batched deliveries must preserve publish order",
    );
    let [first, middle, last]: [M; 3] = delivered
        .try_into()
        .unwrap_or_else(|_| panic!("{LABEL}: exactly the three published messages must arrive"));
    ack_or_unsupported(first, LABEL).await;
    let requeued = nack_requeue(middle, LABEL).await;
    ack_or_unsupported(last, LABEL).await;

    if requeued {
        let again = collect_batched(
            stream,
            1,
            REDELIVERY_TIMEOUT,
            LABEL,
            "the element nacked with requeue must come back",
        )
        .await;
        assert_eq!(
            payloads(&again),
            [b"requeue".to_vec()],
            "{LABEL}: only the element nacked with requeue may come back; the elements acked \
             next to it must stay settled",
        );
        for msg in again {
            ack_or_unsupported(msg, LABEL).await;
        }
    }
    expect_no_batch(stream, LABEL).await;
}

/// One batch settled from a runtime that stops right after: the requeued element still comes
/// back, and the acked one does not.
async fn batch_settles_on_foreign_runtime<Pub, S, Batch, M, E>(
    publisher: &Pub,
    stream: &mut S,
    subject: &str,
) where
    Pub: Publisher,
    S: Stream<Item = Result<Batch, E>> + Unpin,
    Batch: IntoIterator<Item = M>,
    M: IncomingMessage + 'static,
    E: fmt::Debug,
{
    const LABEL: &str = "batches: a batch settled from a runtime that has since stopped";

    publish_all(publisher, subject, &[b"foreign-requeue", b"foreign-ack"]).await;
    let delivered = collect_batched(
        stream,
        2,
        DEFAULT_TIMEOUT,
        LABEL,
        "every published message must arrive",
    )
    .await;
    assert_eq!(
        payloads(&delivered),
        [b"foreign-requeue".to_vec(), b"foreign-ack".to_vec()],
        "{LABEL}: batched deliveries must preserve publish order",
    );
    // Settled the way a batch handler on a dedicated thread settles: the runtime the settlement
    // ran on is gone before the suite looks for the requeued element.
    let requeued = on_foreign_runtime(async move || {
        let [requeue, acked]: [M; 2] = delivered
            .try_into()
            .unwrap_or_else(|_| panic!("{LABEL}: exactly the two published messages must arrive"));
        let requeued = nack_requeue(requeue, LABEL).await;
        ack_or_unsupported(acked, LABEL).await;
        requeued
    })
    .await;

    if requeued {
        let again = collect_batched(
            stream,
            1,
            REDELIVERY_TIMEOUT,
            LABEL,
            "an element nacked with requeue from a runtime that has since stopped must still \
             come back; the broker must run its internal tasks on the runtime it connected on",
        )
        .await;
        assert_eq!(
            payloads(&again),
            [b"foreign-requeue".to_vec()],
            "{LABEL}: only the element nacked with requeue may come back",
        );
        for msg in again {
            ack_or_unsupported(msg, LABEL).await;
        }
    }
    expect_no_batch(stream, LABEL).await;
}

/// Verifies that a subscription both batched and seekable repositions its batches.
///
/// A seek back to a position captured from a batched delivery makes the next batches start at
/// that message and carry the ordered suffix after it, and nothing else. It is the batch form of
/// [`seeking`](fn@super::seeking), for a broker whose batch subscription is also [`Seekable`].
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
/// capabilities::batch_seeking(
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
pub async fn batch_seeking<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: BatchSubscriber + Seekable + Send,
    SubscriberMessage<Src::Subscriber>: Positioned<Position = SeekPosition<Src::Subscriber>>,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    const LABEL: &str = "batch_seeking";
    const BATCH: NonZeroUsize = NonZeroUsize::new(2).unwrap();

    let subject = unique_subject("conformance.batch_seeking");

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut subscriber = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("subscription must open after connect");
    // Minted before `batches` borrows the subscriber; usable while the stream runs.
    let seeker = subscriber.seeker();
    let publisher = make_publisher(&connected);

    let sent: [&[u8]; 5] = [&[0], &[1], &[2], &[3], &[4]];
    publish_all(&publisher, &subject, &sent).await;

    let mut stream = std::pin::pin!(subscriber.batches(BATCH));
    let delivered = collect_batched(
        &mut stream,
        sent.len(),
        DEFAULT_TIMEOUT,
        LABEL,
        "every published message must arrive",
    )
    .await;
    assert_eq!(
        payloads(&delivered),
        sent.map(<[u8]>::to_vec),
        "{LABEL}: batched deliveries must arrive in publish order",
    );
    let back_to = delivered[1].position();
    for msg in delivered {
        ack_or_unsupported(msg, LABEL).await;
    }
    expect_no_batch(&mut stream, LABEL).await;

    seeker
        .seek(back_to)
        .await
        .unwrap_or_else(|err| panic!("{LABEL}: a seek back on a batch subscription failed: {err}"));
    let replayed = collect_batched(
        &mut stream,
        sent.len() - 1,
        DEFAULT_TIMEOUT,
        LABEL,
        "a seek back must make the next batches replay from the captured position",
    )
    .await;
    assert_eq!(
        payloads(&replayed),
        sent[1..]
            .iter()
            .map(|payload| payload.to_vec())
            .collect::<Vec<_>>(),
        "{LABEL}: after a seek back the batches must start at the captured position and carry \
         the ordered suffix after it",
    );
    for msg in replayed {
        ack_or_unsupported(msg, LABEL).await;
    }
    expect_no_batch(&mut stream, LABEL).await;

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");
}
