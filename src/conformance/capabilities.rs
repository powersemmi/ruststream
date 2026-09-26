//! Optional conformance suites, one per capability trait.
//!
//! A broker crate that implements a capability ([`RequestReply`](crate::RequestReply),
//! [`BatchSubscriber`](crate::BatchSubscriber),
//! [`TransactionalPublisher`](crate::TransactionalPublisher),
//! [`OwnedTransactions`](crate::OwnedTransactions), [`Seekable`]) runs the matching suite to
//! prove the implementation honours the trait contract; brokers without the capability simply do
//! not call it. Like [`harness::lifecycle`](super::harness::lifecycle), every suite is built from
//! caller-supplied factories so it stays broker-agnostic, and the in-memory broker is the
//! executable reference that passes all of them.
//!
//! Every suite names its subject through [`unique_subject`](super::helpers::unique_subject)
//! rather than fixing one, so a run reads only its own messages. A fixed subject would pass once
//! and fail on the second run against any broker that keeps what the first left: a retained log, a durable queue, a key namespace. The
//! suites are therefore re-runnable against one server, and two of them can run at once in one
//! process.
//!
//! Each suite also calls the capability the way a service reaches it: from a current-thread
//! runtime on a thread of its own that stops right after the call (a handler on a dedicated
//! thread), and through a handle that outlives the broker's shutdown. What the broker starts on
//! such a call has to run on the runtime it connected on, and a call after shutdown has to return
//! an error at once, because a silent success there is a lost message.

use std::{fmt, future::Future, time::Duration};

use futures::{Stream, StreamExt};
use tokio::time::timeout;

use super::harness::expect_next;
use crate::{AckError, IncomingMessage, OutgoingMessage, Publisher, Seekable, Seeker, Subscriber};

#[cfg(all(test, feature = "memory"))]
mod tests;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const MISS_TIMEOUT: Duration = Duration::from_millis(100);
/// How long a message nacked with requeue may take to come back. Generous, because some
/// transports redeliver through a visibility or backoff window rather than at once.
const REDELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the first delivery of a subscription opened mid-suite may take: a consumer group
/// rebalances before it hands out anything.
const RESUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a call through a handle that outlived the shutdown may stay pending before the suite
/// calls it a hang on the dead connection.
const AFTER_SHUTDOWN_LIMIT: Duration = Duration::from_secs(5);
/// The timeout of the request made after shutdown: far past [`AFTER_SHUTDOWN_LIMIT`], so a
/// requester that waits for a reply instead of failing the publish is caught.
const AFTER_SHUTDOWN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The position a seekable subscription's seeker takes.
type SeekPosition<S> = <<S as Seekable>::Seeker as Seeker>::Position;

/// The message type a subscriber yields.
type SubscriberMessage<S> = <S as Subscriber>::Message;

mod batches;
mod request_reply;
mod seeking;
mod transactions;

pub use batches::{batch_seeking, batches};
pub use request_reply::request_reply;
pub use seeking::{seeking, seeking_unknown_position};
pub use transactions::{owned_transactions, transactions};

/// Collects the payloads of the next `count` deliveries in arrival order, acking each.
async fn collect_payloads<S, M, E>(stream: &mut S, count: usize, label: &str) -> Vec<Vec<u8>>
where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        let msg = expect_next(&mut *stream, label).await;
        payloads.push(msg.payload().to_vec());
        match msg.ack().await {
            Ok(()) | Err(AckError::Unsupported) => {}
            Err(other) => panic!("ack must succeed or be unsupported, got: {other:?}"),
        }
    }
    payloads
}

/// Publishes each payload to `subject`, in order, through the plain publish.
async fn publish_all<Pub: Publisher>(publisher: &Pub, subject: &str, payloads: &[&[u8]]) {
    for payload in payloads {
        publisher
            .publish(OutgoingMessage::new(subject, payload), None)
            .await
            .expect("publish failed");
    }
}

/// Acks `msg`, accepting a transport that has no acknowledgement.
async fn ack_or_unsupported<M: IncomingMessage>(msg: M, label: &str) {
    match msg.ack().await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("{label}: ack must succeed or be unsupported, got: {other:?}"),
    }
}

/// Nacks `msg` with requeue and reports whether the transport performs the requeue; a transport
/// that has none answers [`AckError::Unsupported`].
async fn nack_requeue<M: IncomingMessage>(msg: M, label: &str) -> bool {
    match msg.nack(true).await {
        Ok(()) => true,
        Err(AckError::Unsupported) => false,
        Err(other) => panic!("{label}: nack must succeed or be unsupported, got: {other:?}"),
    }
}

/// The payloads of `messages`, in order.
fn payloads<M: IncomingMessage>(messages: &[M]) -> Vec<Vec<u8>> {
    messages.iter().map(|msg| msg.payload().to_vec()).collect()
}

/// Awaits the next delivery for up to `within`, and panics naming what the broker failed to do
/// when nothing arrives.
async fn expect_within<S, M, E>(stream: &mut S, within: Duration, label: &str, expected: &str) -> M
where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    timeout(within, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label}: nothing arrived within {within:?}; {expected}"))
        .unwrap_or_else(|| panic!("{label}: the stream ended; {expected}"))
        .unwrap_or_else(|err| panic!("{label}: the stream yielded an error: {err:?}"))
}

/// Collects the payloads of the next `count` deliveries, acking each, and panics naming what the
/// broker failed to do when they do not all arrive.
async fn collect_payloads_within<S, M, E>(
    stream: &mut S,
    count: usize,
    label: &str,
    expected: &str,
) -> Vec<Vec<u8>>
where
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        let msg = expect_within(&mut *stream, DEFAULT_TIMEOUT, label, expected).await;
        payloads.push(msg.payload().to_vec());
        ack_or_unsupported(msg, label).await;
    }
    payloads
}

/// Collects whole batches until at least `count` deliveries arrived, each batch within `within`,
/// and returns every delivery of those batches: an extra one in the last batch is the caller's
/// to catch.
async fn collect_batched<S, Batch, M, E>(
    stream: &mut S,
    count: usize,
    within: Duration,
    label: &str,
    expected: &str,
) -> Vec<M>
where
    S: Stream<Item = Result<Batch, E>> + Unpin,
    Batch: IntoIterator<Item = M>,
    E: fmt::Debug,
{
    let mut messages = Vec::with_capacity(count);
    while messages.len() < count {
        let batch = timeout(within, stream.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{label}: {} of {count} deliveries arrived, then nothing within {within:?}; \
                     {expected}",
                    messages.len(),
                )
            })
            .unwrap_or_else(|| panic!("{label}: the batch stream ended; {expected}"))
            .unwrap_or_else(|err| panic!("{label}: the batch stream yielded an error: {err:?}"));
        messages.extend(batch);
    }
    messages
}

/// Asserts that no batch arrives within the negative wait.
async fn expect_no_batch<S, Batch, E>(stream: &mut S, label: &str)
where
    S: Stream<Item = Result<Batch, E>> + Unpin,
{
    let result = timeout(MISS_TIMEOUT, stream.next()).await;
    assert!(
        result.is_err(),
        "{label}: expected no further batches within {MISS_TIMEOUT:?}",
    );
}

/// Awaits a call made through a handle that outlived the shutdown, and panics when it hangs on
/// the dead connection instead of returning.
async fn promptly<Work: Future>(work: Work, label: &str) -> Work::Output {
    timeout(AFTER_SHUTDOWN_LIMIT, work)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{label}: still pending after {AFTER_SHUTDOWN_LIMIT:?}; a call after shutdown must \
             return an error at once, not wait on the dead connection",
            )
        })
}
