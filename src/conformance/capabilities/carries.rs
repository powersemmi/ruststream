//! The carried lane's suites: a delivery lends the value that was published, a delivery without
//! one lends none, and a batch lends its values in the order it yields its deliveries.

use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::time::timeout;

use super::{
    DEFAULT_TIMEOUT, REDELIVERY_TIMEOUT, SubscriberMessage, ack_or_unsupported, expect_within,
    nack_requeue, within,
};
use crate::conformance::helpers::unique_subject;
use crate::conformance::lifecycle::shutdown_within;
use crate::{
    AckError, BatchSubscriber, Broker, Carries, CarriesBatch, Connected, IncomingMessage,
    Subscriber, SubscriptionSource,
};

/// How many rounds the batch suite gives the broker to put two waiting values in one batch.
const ROUNDS: usize = 5;

/// The batch size the batch suite opens its subscription with: room for every value it
/// publishes at once.
const BATCH: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// Verifies the [`Carries`] contract: a delivery lends the value that was published, and a
/// delivery without one lends none.
///
/// The suite opens a subscription from `make_source(subject)`, publishes each of `values` through
/// `publish`, and reads them back: every delivery lends one of them, each exactly once. Order is
/// not part of the claim. It then makes one delivery whose value is gone through
/// `publish_without_value` (a claimed id whose row was deleted) and expects that delivery to
/// lend nothing: the runtime settles it by the subscription's decode-failure policy, and a value
/// lent in its place would reach a handler as if it were real. The suite drops it with
/// `nack(false)`, the way that policy's default does; a transport with no settlement answers
/// [`AckError::Unsupported`], which passes.
///
/// `values` holds at least two distinct values, so a delivery that lends one value twice shows.
///
/// # Examples
///
/// A subscription that reads each delivery of the in-memory broker as a row, the way a database
/// driver reads a row; a broker crate runs the suite with its own descriptor and publisher.
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # mod demo {
/// # use std::future::Future;
/// # use futures::{Stream, StreamExt};
/// # use ruststream::memory::{ConnectedMemoryBroker, MemoryError, MemoryMessage, MemorySubscriber};
/// # use ruststream::{AckError, Carries, HeaderMap, IncomingMessage, NamedCopies, Subscribe};
/// # use ruststream::{Subscriber, SubscriptionSource};
/// # #[derive(Debug, Clone, PartialEq)]
/// # pub struct Row(pub String);
/// # pub struct Rows(pub String);
/// # pub struct RowSubscriber(MemorySubscriber);
/// # pub struct RowDelivery(MemoryMessage, Option<Row>);
/// # impl SubscriptionSource<ConnectedMemoryBroker> for Rows {
/// #     type Subscriber = RowSubscriber;
/// #     type Copies = NamedCopies;
/// #     fn name(&self) -> &str { &self.0 }
/// #     async fn subscribe(self, c: &ConnectedMemoryBroker) -> Result<RowSubscriber, MemoryError> {
/// #         Ok(RowSubscriber(Subscribe::subscribe(c, &self.0).await?))
/// #     }
/// # }
/// # impl Subscriber for RowSubscriber {
/// #     type Message = RowDelivery;
/// #     type Error = std::convert::Infallible;
/// #     fn stream(&mut self) -> impl Stream<Item = Result<RowDelivery, Self::Error>> + Send + '_ {
/// #         self.0.stream().map(|one| one.map(|msg| {
/// #             let row = std::str::from_utf8(msg.payload()).ok()
/// #                 .filter(|text| *text != "gone").map(|text| Row(text.to_owned()));
/// #             RowDelivery(msg, row)
/// #         }))
/// #     }
/// # }
/// # impl Carries<Row> for RowDelivery {
/// #     fn carried(&self) -> Option<&Row> { self.1.as_ref() }
/// # }
/// # impl IncomingMessage for RowDelivery {
/// #     fn payload(&self) -> &[u8] { self.0.payload() }
/// #     fn headers(&self) -> &HeaderMap { self.0.headers() }
/// #     fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send { self.0.ack() }
/// #     fn nack(self, r: bool) -> impl Future<Output = Result<(), AckError>> + Send {
/// #         self.0.nack(r)
/// #     }
/// # }
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::{OutgoingMessage, Publisher};
///
/// pub async fn rows_conform() {
///     capabilities::carries(
///         MemoryBroker::new,
///         |name| Rows(name.to_owned()),
///         async |connected: &ConnectedMemoryBroker, subject: &str, row: &Row| {
///             let msg = OutgoingMessage::new(subject, row.0.as_bytes());
///             connected.publisher().publish(msg, None).await
///         },
///         async |connected: &ConnectedMemoryBroker, subject: &str| {
///             let msg = OutgoingMessage::new(subject, b"gone".as_slice());
///             connected.publisher().publish(msg, None).await
///         },
///         &[Row("ann".to_owned()), Row("bob".to_owned())],
///     )
///     .await;
/// }
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if any step violates the contract, or when `values` holds
/// fewer than two values.
// A check is awaited on the test's own task and never spawned, so the caller's factories and
// values need not be `Send` or `Sync`.
#[allow(clippy::future_not_send)]
pub async fn carries<B, Value, MkBroker, Src, MkSrc, Publish, PublishError, Gone, GoneError>(
    make_broker: MkBroker,
    make_source: MkSrc,
    publish: Publish,
    publish_without_value: Gone,
    values: &[Value],
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    SubscriberMessage<Src::Subscriber>: Carries<Value>,
    MkSrc: Fn(&str) -> Src,
    Publish: AsyncFn(&Connected<B>, &str, &Value) -> Result<(), PublishError>,
    PublishError: fmt::Debug,
    Gone: AsyncFn(&Connected<B>, &str) -> Result<(), GoneError>,
    GoneError: fmt::Debug,
    Value: PartialEq + fmt::Debug,
{
    const LABEL: &str = "carries";
    assert!(
        values.len() >= 2,
        "{LABEL}: pass at least two distinct values, so a delivery lending one twice shows",
    );

    let subject = unique_subject("conformance.carries");
    let connected = within(make_broker().connect(), "carries: connect")
        .await
        .expect("broker must connect");
    let mut subscriber = within(
        make_source(&subject).subscribe(&connected),
        "carries: subscribe",
    )
    .await
    .expect("subscription must open after connect");

    for value in values {
        within(publish(&connected, &subject, value), "carries: a publish")
            .await
            .expect("publishing a value failed");
    }
    let mut stream = std::pin::pin!(subscriber.stream());
    let mut unseen: Vec<&Value> = values.iter().collect();
    for _ in values {
        let msg = expect_within(
            &mut stream,
            DEFAULT_TIMEOUT,
            LABEL,
            "every published value must arrive",
        )
        .await;
        let lent = msg.carried().unwrap_or_else(|| {
            panic!("{LABEL}: a delivery of a published value must lend it, and it lent none")
        });
        take_published(&mut unseen, lent, LABEL);
        ack_or_unsupported(msg, LABEL).await;
    }

    within(
        publish_without_value(&connected, &subject),
        "carries: publishing a delivery without a value",
    )
    .await
    .expect("making a delivery without a value failed");
    let msg = expect_within(
        &mut stream,
        DEFAULT_TIMEOUT,
        LABEL,
        "the delivery without a value must arrive",
    )
    .await;
    if let Some(lent) = msg.carried() {
        panic!(
            "{LABEL}: a delivery without a value must lend none, so the runtime settles it by \
             the decode-failure policy; it lent {lent:?}, which a handler would take for real",
        );
    }
    drop_unread(msg, LABEL).await;

    shutdown_within(connected, LABEL).await;
}

/// Verifies the [`CarriesBatch`] contract: a batch lends one value per delivery, in the order it
/// yields its deliveries, and never more values than it delivers.
///
/// The suite opens a batch subscription from `make_source(subject)` and publishes each of
/// `values` through `publish`. Across the batches that come back, every value is lent exactly
/// once, every delivery of a published value lends one, and no batch lends more values than it
/// delivers.
///
/// The order is checked through a settlement, because the runtime settles delivery `i` by the
/// verdict on value `i`. Two values go out together until a batch carries both; the first
/// delivery of that batch is nacked with requeue and the other acked, and the delivery that comes
/// back must lend the value the batch lent first. A subscription that hands out one value per
/// batch in five rounds is within its contract, and a transport that answers the requeue with
/// [`AckError::Unsupported`] has nothing to bring back; the order goes unchecked for either.
///
/// Last, one delivery whose value is gone (through `publish_without_value`) must come in a batch
/// that lends no value for it: it sits past the end of the slice, where the runtime settles it by
/// the decode-failure policy.
///
/// # Examples
///
/// The same in-memory subscription as on [`carries`], whose batches keep the rows they read in
/// one vector beside the deliveries.
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # mod demo {
/// # use std::future::Future;
/// # use std::num::NonZeroUsize;
/// # use futures::{Stream, StreamExt};
/// # use ruststream::memory::{ConnectedMemoryBroker, MemoryError, MemoryMessage, MemorySubscriber};
/// # use ruststream::{AckError, BatchSubscriber, CarriesBatch, HeaderMap, IncomingMessage};
/// # use ruststream::{NamedCopies, Subscribe, Subscriber, SubscriptionSource};
/// # #[derive(Debug, Clone, PartialEq)]
/// # pub struct Row(pub String);
/// # pub struct Rows(pub String);
/// # pub struct RowSubscriber(MemorySubscriber);
/// # pub struct Page { rows: Vec<Row>, deliveries: Vec<MemoryMessage> }
/// # impl SubscriptionSource<ConnectedMemoryBroker> for Rows {
/// #     type Subscriber = RowSubscriber;
/// #     type Copies = NamedCopies;
/// #     fn name(&self) -> &str { &self.0 }
/// #     async fn subscribe(self, c: &ConnectedMemoryBroker) -> Result<RowSubscriber, MemoryError> {
/// #         Ok(RowSubscriber(Subscribe::subscribe(c, &self.0).await?))
/// #     }
/// # }
/// # impl Subscriber for RowSubscriber {
/// #     type Message = MemoryMessage;
/// #     type Error = std::convert::Infallible;
/// #     fn stream(&mut self) -> impl Stream<Item = Result<MemoryMessage, Self::Error>> + Send + '_ {
/// #         self.0.stream()
/// #     }
/// # }
/// # impl BatchSubscriber for RowSubscriber {
/// #     type Batch = Page;
/// #     fn batches(&mut self, size: NonZeroUsize)
/// #         -> impl Stream<Item = Result<Page, Self::Error>> + Send + '_ {
/// #         self.0.batches(size).map(|one| one.map(|page| {
/// #             let (mut rows, mut kept, mut gone) = (Vec::new(), Vec::new(), Vec::new());
/// #             for msg in page {
/// #                 match std::str::from_utf8(msg.payload()).ok().filter(|t| *t != "gone") {
/// #                     Some(text) => { rows.push(Row(text.to_owned())); kept.push(msg) }
/// #                     None => gone.push(msg),
/// #                 }
/// #             }
/// #             kept.extend(gone);
/// #             Page { rows, deliveries: kept }
/// #         }))
/// #     }
/// # }
/// # impl CarriesBatch<Row> for Page {
/// #     fn carried(&self) -> &[Row] { &self.rows }
/// # }
/// # impl IntoIterator for Page {
/// #     type Item = MemoryMessage;
/// #     type IntoIter = std::vec::IntoIter<MemoryMessage>;
/// #     fn into_iter(self) -> Self::IntoIter { self.deliveries.into_iter() }
/// # }
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::{OutgoingMessage, Publisher};
///
/// pub async fn pages_conform() {
///     capabilities::carries_batch(
///         MemoryBroker::new,
///         |name| Rows(name.to_owned()),
///         async |connected: &ConnectedMemoryBroker, subject: &str, row: &Row| {
///             let msg = OutgoingMessage::new(subject, row.0.as_bytes());
///             connected.publisher().publish(msg, None).await
///         },
///         async |connected: &ConnectedMemoryBroker, subject: &str| {
///             let msg = OutgoingMessage::new(subject, b"gone".as_slice());
///             connected.publisher().publish(msg, None).await
///         },
///         &[Row("ann".to_owned()), Row("bob".to_owned())],
///     )
///     .await;
/// }
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if any step violates the contract, or when `values` holds
/// fewer than two values.
// A check is awaited on the test's own task and never spawned, so the caller's factories and
// values need not be `Send` or `Sync`.
#[allow(clippy::future_not_send)]
pub async fn carries_batch<B, Value, MkBroker, Src, MkSrc, Publish, PublishError, Gone, GoneError>(
    make_broker: MkBroker,
    make_source: MkSrc,
    publish: Publish,
    publish_without_value: Gone,
    values: &[Value],
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: BatchSubscriber + Send,
    <Src::Subscriber as BatchSubscriber>::Batch: CarriesBatch<Value>,
    MkSrc: Fn(&str) -> Src,
    Publish: AsyncFn(&Connected<B>, &str, &Value) -> Result<(), PublishError>,
    PublishError: fmt::Debug,
    Gone: AsyncFn(&Connected<B>, &str) -> Result<(), GoneError>,
    GoneError: fmt::Debug,
    Value: PartialEq + Clone + fmt::Debug,
{
    const LABEL: &str = "carries_batch";
    assert!(
        values.len() >= 2,
        "{LABEL}: pass at least two distinct values, so a batch lending one twice shows",
    );

    let subject = unique_subject("conformance.carries_batch");
    let connected = within(make_broker().connect(), "carries_batch: connect")
        .await
        .expect("broker must connect");
    let mut subscriber = within(
        make_source(&subject).subscribe(&connected),
        "carries_batch: subscribe",
    )
    .await
    .expect("subscription must open after connect");

    for value in values {
        within(
            publish(&connected, &subject, value),
            "carries_batch: a publish",
        )
        .await
        .expect("publishing a value failed");
    }
    let mut stream = std::pin::pin!(subscriber.batches(BATCH));
    let mut unseen: Vec<&Value> = values.iter().collect();
    while !unseen.is_empty() {
        let (lent, deliveries) = next_page(
            &mut stream,
            DEFAULT_TIMEOUT,
            LABEL,
            "every published value must arrive",
        )
        .await;
        assert_eq!(
            lent.len(),
            deliveries.len(),
            "{LABEL}: every delivery of a published value must lend it",
        );
        for value in &lent {
            take_published(&mut unseen, value, LABEL);
        }
        for msg in deliveries {
            ack_or_unsupported(msg, LABEL).await;
        }
    }

    pages_keep_their_order(&connected, &publish, &mut stream, &subject, values).await;

    within(
        publish_without_value(&connected, &subject),
        "carries_batch: publishing a delivery without a value",
    )
    .await
    .expect("making a delivery without a value failed");
    let (lent, deliveries) = next_page(
        &mut stream,
        DEFAULT_TIMEOUT,
        LABEL,
        "the delivery without a value must arrive",
    )
    .await;
    assert!(
        lent.len() < deliveries.len(),
        "{LABEL}: a delivery without a value must be lent none: it goes past the end of the \
         slice, where the runtime settles it by the decode-failure policy; the batch lent \
         {lent:?} for {} deliveries",
        deliveries.len(),
    );
    for msg in deliveries {
        drop_unread(msg, LABEL).await;
    }

    shutdown_within(connected, LABEL).await;
}

/// Two values go out together until a batch carries both; the batch's first delivery is
/// requeued and the second acked, and what comes back must lend the value the batch lent first.
// A check is awaited on the test's own task and never spawned, so the caller's factories and
// values need not be `Send` or `Sync`.
#[allow(clippy::future_not_send)]
async fn pages_keep_their_order<Conn, Value, Publish, PublishError, S, Batch, M, E>(
    connected: &Conn,
    publish: &Publish,
    stream: &mut S,
    subject: &str,
    values: &[Value],
) where
    Publish: AsyncFn(&Conn, &str, &Value) -> Result<(), PublishError>,
    PublishError: fmt::Debug,
    S: Stream<Item = Result<Batch, E>> + Unpin,
    Batch: CarriesBatch<Value> + IntoIterator<Item = M>,
    M: IncomingMessage,
    E: fmt::Debug,
    Value: PartialEq + Clone + fmt::Debug,
{
    const LABEL: &str = "carries_batch: a batch lends its values in delivery order";

    for _ in 0..ROUNDS {
        for value in &values[..2] {
            within(
                publish(connected, subject, value),
                "carries_batch: a publish",
            )
            .await
            .expect("publishing a value failed");
        }
        let mut pages = Vec::new();
        let mut delivered = 0;
        while delivered < 2 {
            let page = next_page(
                &mut *stream,
                DEFAULT_TIMEOUT,
                LABEL,
                "both published values must arrive",
            )
            .await;
            delivered += page.1.len();
            pages.push(page);
        }
        let Some(index) = pages.iter().position(|(lent, _)| lent.len() > 1) else {
            for (_, deliveries) in pages {
                for msg in deliveries {
                    ack_or_unsupported(msg, LABEL).await;
                }
            }
            continue;
        };
        let first = pages[index].0[0].clone();
        let mut requeued = false;
        for (at, (_, deliveries)) in pages.into_iter().enumerate() {
            for (element, msg) in deliveries.into_iter().enumerate() {
                if at == index && element == 0 {
                    requeued = nack_requeue(msg, LABEL).await;
                } else {
                    ack_or_unsupported(msg, LABEL).await;
                }
            }
        }
        if !requeued {
            return;
        }
        let (lent, deliveries) = next_page(
            &mut *stream,
            REDELIVERY_TIMEOUT,
            LABEL,
            "the delivery nacked with requeue must come back",
        )
        .await;
        assert_eq!(
            lent,
            [first],
            "{LABEL}: the value at index `i` must belong to the `i`-th delivery: the first \
             delivery was nacked with requeue, and what came back must lend the value the batch \
             lent first",
        );
        for msg in deliveries {
            ack_or_unsupported(msg, LABEL).await;
        }
        return;
    }
}

/// The next batch off `stream`: the values it lent, cloned, and its deliveries. A batch that lends
/// more values than it delivers fails here, because the runtime settles delivery `i` by the
/// verdict on value `i`.
async fn next_page<S, Batch, M, E, Value>(
    stream: &mut S,
    within: Duration,
    label: &str,
    expected: &str,
) -> (Vec<Value>, Vec<M>)
where
    S: Stream<Item = Result<Batch, E>> + Unpin,
    Batch: CarriesBatch<Value> + IntoIterator<Item = M>,
    E: fmt::Debug,
    Value: Clone + fmt::Debug,
{
    let batch = timeout(within, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label}: nothing arrived within {within:?}; {expected}"))
        .unwrap_or_else(|| panic!("{label}: the batch stream ended; {expected}"))
        .unwrap_or_else(|err| panic!("{label}: the batch stream yielded an error: {err:?}"));
    let lent = batch.carried().to_vec();
    let deliveries: Vec<M> = batch.into_iter().collect();
    assert!(
        lent.len() <= deliveries.len(),
        "{label}: a batch must never lend more values than it delivers: it lent {lent:?} for {} \
         deliveries",
        deliveries.len(),
    );
    (lent, deliveries)
}

/// Strikes `lent` off the values still unseen, failing when it was never published or already
/// lent once.
fn take_published<Value: PartialEq + fmt::Debug>(
    unseen: &mut Vec<&Value>,
    lent: &Value,
    label: &str,
) {
    let position = unseen
        .iter()
        .position(|value| *value == lent)
        .unwrap_or_else(|| {
            panic!(
                "{label}: a delivery lent a value that was not published, or lent one value twice: \
             {lent:?}",
            )
        });
    unseen.remove(position);
}

/// Drops a delivery its handler never saw, the way the runtime's default decode policy settles
/// it, accepting a transport that has no settlement.
async fn drop_unread<M: IncomingMessage>(msg: M, label: &str) {
    match within(msg.nack(false), &format!("{label}: a nack")).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("{label}: nack must succeed or be unsupported, got: {other:?}"),
    }
}
