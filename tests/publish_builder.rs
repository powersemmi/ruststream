//! The publish builder: one call shape over every surface, with the positions the message
//! type's `#[derive(Outgoing)]` declaration leaves open.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use ruststream::OutgoingMessage;
use ruststream::codec::JsonCodec;
use ruststream::memory::prelude::*;
use ruststream::testing::{TestApp, TestableBroker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Outgoing, PartialEq, Serialize, Deserialize)]
struct Job {
    id: u64,
}

/// The headers contract of [`ChunkDone`]: an ordinary serde struct the derive does not touch.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct DoneMeta {
    task_id: u64,
}

/// A message bound to one name.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "chunks.progress")]
struct Progress {
    percent: u8,
}

/// A message bound to one name, with a header contract.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "chunks.done", headers = DoneMeta)]
struct ChunkDone {
    output_key: String,
}

/// A message bound to a space of names.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders.{tenant}.{region}.v1")]
struct OrderPlaced {
    id: u64,
}

/// A message that is simply sent where the caller says.
#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
struct OrderArchived {
    id: u64,
}

/// A message carrying its own bytes: the wire a call site reaches for when the payload is not a
/// model. `Serialized` is what says the bytes already are the payload, so no codec runs on them
/// and the builder offers no codec position for it.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

impl Wire {
    /// The wire form of `bytes`, for the call sites that hold a literal.
    fn of(bytes: impl AsRef<[u8]>) -> Self {
        Self(bytes.as_ref().to_vec())
    }
}

#[derive(OutSlot)]
#[publishes(ChunkDone, Progress, OrderPlaced, OrderArchived)]
struct Events;

/// The self-carrying wire goes out through a slot of its own, so the models keep the inline
/// declaration form at its widest.
#[derive(OutSlot)]
#[publishes(Wire)]
struct Frames;

/// Every destination form, through one slot - plus the serialized wire through a second, which
/// is also what keeps the four-element inline declaration exercised.
#[subscriber("jobs.in")]
async fn convert(
    job: &Job,
    Out(out): Out<impl Publisher, Events, (ChunkDone, Progress, OrderPlaced, OrderArchived)>,
    Out(frames): Out<impl Publisher, Frames, Wire>,
) -> HandlerOutcome {
    // Fixed name: nothing to say about the destination.
    if out
        .message(&Progress { percent: 50 })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    // Fixed name plus the declared header contract.
    let done = ChunkDone {
        output_key: format!("out/{}", job.id),
    };
    if out
        .message(&done)
        .with_headers(&DoneMeta { task_id: job.id })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    // Templated name: one setter per placeholder, in declaration order.
    if out
        .message(&OrderPlaced { id: job.id })
        .to()
        .tenant("acme")
        .region(7_u32)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    // Declared with the derive alone: the call site names the destination.
    if out
        .message(&OrderArchived { id: job.id })
        .to("orders.archived")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    // A self-carrying model: the same shape without a codec position.
    let mut headers = HeaderMap::new();
    headers.insert("source", "jobs.in");
    if frames
        .message(&Wire::of(b"frame"))
        .with_headers(headers)
        .to("chunks.raw")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_destination_form_resolves_through_one_builder() {
    let app =
        RustStream::new(AppInfo::new("builder", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(convert)
                .out(Events, Publish)
                .out(Frames, Publish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Job { id: 3 })
        .to("jobs.in")
        .publish()
        .await
        .expect("publish");

    let broker = tb.broker::<MemoryBroker>();
    broker
        .published::<Progress>("chunks.progress")
        .assert_called_once()
        .with(&Progress { percent: 50 });
    // The templated address is rendered per publish, placeholders in declaration order.
    broker
        .published::<OrderPlaced>("orders.acme.7.v1")
        .assert_called_once()
        .with(&OrderPlaced { id: 3 });
    broker
        .published::<OrderArchived>("orders.archived")
        .assert_called_once()
        .with(&OrderArchived { id: 3 });

    // The declared contract lands in the header map, one entry per field.
    let done = broker
        .published::<ChunkDone>("chunks.done")
        .assert_called_once();
    assert_eq!(done.messages()[0].headers().get_str("task_id"), Some("3"));

    // The serialized wire carries its map as it is.
    let raw = tb.out::<Frames>();
    let framed = raw
        .messages()
        .iter()
        .find(|msg| msg.name() == "chunks.raw")
        .expect("the raw publish is attributed to the slot");
    assert_eq!(framed.payload(), b"frame");
    assert_eq!(framed.headers().get_str("source"), Some("jobs.in"));
}

/// A bare publisher reaches the same builder through [`PublishExt`], encoding with the crate
/// default codec unless the call names one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_publisher_publishes_through_the_builder() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.expect("connect");
    let publisher = connected.publisher();

    publisher
        .message(&Progress { percent: 100 })
        .publish()
        .await
        .expect("fixed name");
    publisher
        .message(&OrderArchived { id: 9 })
        .with_codec(JsonCodec)
        .to("orders.archived".to_owned())
        .publish()
        .await
        .expect("call-level codec and a computed name");
    publisher
        .message(&Wire::of(b"bytes"))
        .to("audit")
        .publish()
        .await
        .expect("carried bytes");

    assert_eq!(connected.published("chunks.progress").len(), 1);
    assert_eq!(connected.published("orders.archived").len(), 1);
    let audit = connected.published("audit");
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].payload(), b"bytes");
}

/// A bare publisher and both transaction surfaces carry the same builder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_and_its_transactions_carry_the_builder() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.expect("connect");
    let publisher = connected.publisher();

    publisher
        .message(&Progress { percent: 10 })
        .publish()
        .await
        .expect("bare publisher");

    // The borrowed transaction kind.
    let scope = publisher.begin().await.expect("begin");
    scope
        .message(&Progress { percent: 20 })
        .publish()
        .await
        .expect("in transaction");
    scope
        .message(&Wire::of(b"trace"))
        .to("audit.trail")
        .publish()
        .await
        .expect("carried bytes in transaction");
    scope.commit().await.expect("commit");

    publisher
        .message(&Wire::of(b"wire"))
        .to("audit.wire")
        .publish()
        .await
        .expect("carried bytes through the bare publisher");

    // The owned transaction kind.
    let mut owned = publisher
        .owned_transaction()
        .await
        .expect("owned transaction");
    owned
        .message(&OrderArchived { id: 1 })
        .to("orders.archived")
        .publish()
        .await
        .expect("in owned transaction");
    owned
        .message(&Wire::of(b"ledger"))
        .to("audit.ledger")
        .publish()
        .await
        .expect("carried bytes in owned transaction");
    owned.commit().await.expect("commit");

    assert_eq!(connected.published("chunks.progress").len(), 2);
    assert_eq!(connected.published("audit.trail").len(), 1);
    assert_eq!(connected.published("audit.wire").len(), 1);
    assert_eq!(connected.published("orders.archived").len(), 1);
    assert_eq!(connected.published("audit.ledger").len(), 1);
}

/// The batch publishing path carries the builder too: the reply travels its own wiring while
/// the handler's own publishes go through the slot, in one handler.
#[subscriber("jobs.bulk", publish("jobs.settled"))]
async fn settle(
    jobs: &[Job],
    Out(out): Out<impl Publisher, Events, Progress>,
) -> Result<Vec<Job>, HandlerOutcome> {
    for job in jobs {
        if out
            .message(&Progress {
                percent: u8::try_from(job.id).unwrap_or(u8::MAX),
            })
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
    }
    Ok(jobs.iter().map(|job| Job { id: job.id }).collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_publishing_handler_carries_the_builder() {
    let app =
        RustStream::new(AppInfo::new("bulk", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(settle.batch(nonzero!(8)))
                .out(Reply, Publish)
                .out(Events, Publish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Job { id: 4 })
        .to("jobs.bulk")
        .publish()
        .await
        .expect("publish");

    tb.out::<Events>().assert_called_once();
    tb.broker::<MemoryBroker>()
        .published::<Progress>("chunks.progress")
        .assert_called_once()
        .with(&Progress { percent: 4 });
}

/// A builder in flight keeps its wiring out of Debug: it holds a live publisher, and a
/// diagnostic dump must not print one.
#[test]
fn a_publish_builder_hides_its_wiring() {
    let publisher = MemoryBroker::new().publisher();
    let pending = publisher.message(&Progress { percent: 1 });
    assert_eq!(format!("{pending:?}"), "PublishBuilder { .. }");
}

/// The typed headers of a message with no contract are rejected, but an arbitrary transport map
/// still travels with it: the map stands for no declaration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_contract_less_message_still_carries_a_header_map() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.expect("connect");

    let mut headers = HeaderMap::new();
    headers.insert("x-trace", "abc");
    connected
        .publisher()
        .message(&Progress { percent: 5 })
        .with_headers(headers)
        .publish()
        .await
        .expect("map headers on a contract-less message");

    let published = connected.published("chunks.progress");
    assert_eq!(published[0].headers().get_str("x-trace"), Some("abc"));
}

/// A publisher handle carrying an argument for every message it sends: the shape a broker
/// adapter takes when the base reaches the builder.
struct Tenanted<P>(P, HeaderMap);

impl<P: Publisher> Publisher for Tenanted<P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.0.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.1)
    }
}

impl<P: OwnedTransactions> OwnedTransactions for Tenanted<P> {
    type Transaction = Tagged<P::Transaction>;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        Ok(Tagged(self.0.transaction().await?, self.1.clone()))
    }
}

/// The transaction the tenanted handle opens: the same argument, on the buffered path.
struct Tagged<T>(T, HeaderMap);

impl<T: Transaction> Transaction for Tagged<T> {
    type Error = T::Error;

    async fn publish(&mut self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.0.publish(msg).await
    }

    async fn commit(self) -> Result<(), Self::Error> {
        self.0.commit().await
    }

    async fn abort(self) -> Result<(), Self::Error> {
        self.0.abort().await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.1)
    }
}

/// The base a tenanted handle contributes to every publish.
fn tenant_base() -> HeaderMap {
    [("tenant", "acme"), ("x-trace", "handle")]
        .into_iter()
        .collect()
}

/// A publisher that names no base leaves the outgoing map exactly as the call site built it:
/// the position still writes into an empty map, so nothing about an existing publish moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_without_a_base_sends_only_the_call_sites_headers() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.expect("connect");

    let mut headers = HeaderMap::new();
    headers.insert("x-trace", "call");
    connected
        .publisher()
        .message(&Wire::of(b"bytes"))
        .to("audit")
        .with_headers(headers)
        .publish()
        .await
        .expect("map headers without a base");
    connected
        .publisher()
        .message(&Wire::of(b"bytes"))
        .to("audit.bare")
        .publish()
        .await
        .expect("no headers at all");

    let sent = connected.published("audit");
    assert_eq!(sent[0].headers().get_str("x-trace"), Some("call"));
    assert_eq!(
        sent[0].headers().len(),
        1,
        "a publisher with no base adds nothing of its own",
    );
    assert!(connected.published("audit.bare")[0].headers().is_empty());
}

/// The handle's base travels with a publish that names no headers, and a call-site map wins key
/// by key: the keys it names are overwritten, the ones it leaves alone survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_call_site_wins_over_the_handles_base_key_by_key() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.expect("connect");
    let publisher = Tenanted(connected.publisher(), tenant_base());

    publisher
        .message(&Progress { percent: 1 })
        .publish()
        .await
        .expect("the base alone");

    let mut headers = HeaderMap::new();
    headers.insert("x-trace", "call");
    headers.insert("x-request-id", "r-1");
    publisher
        .message(&Progress { percent: 2 })
        .with_headers(headers)
        .publish()
        .await
        .expect("a map over the base");

    let sent = connected.published("chunks.progress");
    assert_eq!(sent[0].headers().get_str("tenant"), Some("acme"));
    assert_eq!(sent[0].headers().get_str("x-trace"), Some("handle"));

    let merged = sent[1].headers();
    assert_eq!(
        merged.get_str("x-trace"),
        Some("call"),
        "the call site has the last word on a key both name",
    );
    assert_eq!(
        merged.get_str("tenant"),
        Some("acme"),
        "a base key the call does not name survives",
    );
    assert_eq!(merged.get_str("x-request-id"), Some("r-1"));
}

/// A message declaring a header contract publishes over the handle's base: the contract fields
/// win on the keys they name, the base carries the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_contract_serializes_over_the_handles_base() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.expect("connect");
    let mut base = tenant_base();
    base.insert("task_id", "0");
    let publisher = Tenanted(connected.publisher(), base);

    publisher
        .message(&ChunkDone {
            output_key: "out/1".to_owned(),
        })
        .with_headers(&DoneMeta { task_id: 7 })
        .publish()
        .await
        .expect("a contract over the base");

    let sent = connected.published("chunks.done");
    let headers = sent[0].headers();
    assert_eq!(
        headers.get_str("task_id"),
        Some("7"),
        "the contract field overwrites the base's placeholder",
    );
    assert_eq!(headers.get_str("tenant"), Some("acme"));
    assert_eq!(headers.get_str("x-trace"), Some("handle"));
}

/// A transaction behaves like the handle it came from: its base rides every buffered publish,
/// under whatever the call site names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_carries_its_own_base_under_the_call_site() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.expect("connect");
    let publisher = Tenanted(connected.publisher(), tenant_base());

    let mut txn = publisher
        .owned_transaction()
        .await
        .expect("owned transaction");
    txn.message(&Progress { percent: 3 })
        .publish()
        .await
        .expect("the base alone, buffered");
    let mut headers = HeaderMap::new();
    headers.insert("x-trace", "call");
    txn.message(&Wire::of(b"ledger"))
        .to("audit.ledger")
        .with_headers(headers)
        .publish()
        .await
        .expect("a map over the base, buffered");
    txn.commit().await.expect("commit");

    let progress = connected.published("chunks.progress");
    assert_eq!(progress[0].headers().get_str("tenant"), Some("acme"));
    assert_eq!(progress[0].headers().get_str("x-trace"), Some("handle"));

    let ledger = connected.published("audit.ledger");
    assert_eq!(ledger[0].headers().get_str("x-trace"), Some("call"));
    assert_eq!(ledger[0].headers().get_str("tenant"), Some("acme"));
}
