// The declared-message fixtures below pin the declaration surface through their trait impls,
// which the compiler checks whether or not a test constructs one.
#![allow(dead_code)]

use std::collections::HashMap;

use serde::Serialize;

use super::*;
#[cfg(feature = "memory")]
use crate::runtime::PublishExt;
use crate::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination, WithHeaders};

#[derive(Debug)]
struct A;
impl OutSlot for A {
    const NAME: &'static str = "A";
}

#[derive(Debug)]
struct B;
impl OutSlot for B {
    const NAME: &'static str = "B";
}

#[test]
fn binds_slots_in_any_order() {
    let init = <(A, B) as InitSlots>::init();
    // Bind the second marker first, then the first: positions are found by marker.
    let step = BindSlot::<B, &str, SlotPos<1>>::bind(init, "b");
    let done = BindSlot::<A, &str, SlotPos<0>>::bind(step, "a");
    let (a, b) = done;
    assert_eq!(a.into_source(), "a");
    assert_eq!(b.into_source(), "b");
}

/// A marker carrying a publish dictionary, standing in for `#[derive(OutSlot)]` with
/// `#[publishes(..)]` (the derive lives in the macros crate).
#[derive(Debug)]
struct Events;

impl OutSlot for Events {
    const NAME: &'static str = "Events";

    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new("events.progress", "Progress")]
    }
}

/// A dictionary message with no header contract, and a payload JSON cannot encode: an
/// object key must be a string, and this map's are tuples.
#[derive(Serialize)]
struct Progress {
    samples: HashMap<(u8, u8), u8>,
}

impl Progress {
    fn new() -> Self {
        Self {
            samples: HashMap::from([((1, 2), 3)]),
        }
    }
}

impl MessageHeaders for Progress {
    type Contract = NoHeaders;
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "events.progress")]` declares, so the same
// fixture drives the builder's error arms.
impl OutgoingDestination for Progress {
    type Form = FixedName;
    const ADDRESS: &'static str = "events.progress";
}

impl PublishedThrough<Events> for Progress {}

/// Headers a header map can carry: one scalar field.
#[derive(Serialize)]
struct Meta {
    task_id: u64,
}

/// Headers it cannot: entries are scalars, and this one nests a struct.
#[derive(Serialize)]
struct NestedMeta {
    inner: Meta,
}

/// A contract-carrying message that encodes, so the headers are what fails.
#[derive(Serialize)]
struct Done {
    key: &'static str,
}

impl MessageHeaders for Done {
    type Contract = WithHeaders<NestedMeta>;
}

impl OutgoingDestination for Done {
    type Form = FixedName;
    const ADDRESS: &'static str = "events.done";
}

impl PublishedThrough<Events> for Done {}

/// The unrestricted declaration documents whatever the marker declares, so a handler that
/// pins no message set still contributes the slot's dictionary to the document.
#[test]
fn an_unrestricted_declaration_documents_the_whole_dictionary() {
    let declared = <() as OutMessages<Events>>::outgoing();
    assert_eq!(declared.len(), 1);
    assert_eq!(declared[0].channel, "events.progress");
    assert_eq!(declared[0].message_type, "Progress");
}

/// The slot wrapper is transparent for the whole transaction protocol: what the handler
/// aborts through the slot never reaches the bus.
#[cfg(feature = "memory")]
#[tokio::test]
async fn a_slot_publisher_delegates_the_transaction_protocol() {
    use futures::StreamExt;

    use crate::Subscriber;
    use crate::memory::MemoryBroker;

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("slots.ledger");
    let slot = SlotPublisher::<_, Events>::new(broker.publisher());

    slot.begin_transaction().await.expect("begin failed");
    slot.raw(b"staged")
        .to("slots.ledger")
        .publish()
        .await
        .expect("publish failed");
    slot.abort().await.expect("abort failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(
        futures::poll!(stream.next()).is_pending(),
        "an aborted transaction discards what it staged",
    );
}

/// Request / reply rides the arena entry unchanged, through the typed layer as well: the
/// correlated reply comes back to the caller.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn a_slot_entry_delegates_request_reply() {
    use futures::StreamExt;

    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;
    use crate::runtime::Slot;
    use crate::{IncomingMessage, Subscriber};

    let broker = MemoryBroker::new();
    let mut service = broker.subscribe("slots.echo");
    let responder = broker.publisher();
    let slot = Slot::<Events, _, _>::test_entry(broker.requester(), JsonCodec);

    let respond = async {
        let mut stream = std::pin::pin!(service.stream());
        let msg = stream
            .next()
            .await
            .expect("request missing")
            .expect("memory subscriber never errors");
        let reply_to = msg
            .headers()
            .reply_to()
            .expect("a request carries reply-to")
            .to_owned();
        responder
            .raw(msg.payload())
            .to(reply_to)
            .publish()
            .await
            .expect("reply publish failed");
        msg.ack().await.expect("ack failed");
    };
    let request = slot.request(
        OutgoingMessage::new("slots.echo", b"ping".as_slice()),
        Duration::from_secs(5),
    );

    let (reply, ()) = futures::join!(request, respond);
    assert_eq!(
        reply.expect("the request must resolve").payload(),
        b"ping",
        "the reply travels back through the slot wrapper untouched",
    );
}

/// The arena entry delegates the borrowed transaction protocol too, so a capability-refined
/// `Out` slot settles through the wrapper.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn a_slot_entry_delegates_the_transaction_protocol() {
    use futures::StreamExt;

    use crate::Subscriber;
    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;
    use crate::runtime::Slot;

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("slots.ledger");
    let slot = Slot::<Events, _, _>::test_entry(broker.publisher(), JsonCodec);

    slot.begin_transaction().await.expect("begin failed");
    slot.raw(b"staged")
        .to("slots.ledger")
        .publish()
        .await
        .expect("publish failed");
    slot.abort().await.expect("abort failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(
        futures::poll!(stream.next()).is_pending(),
        "an aborted transaction discards what it staged",
    );
}

/// The builder reports both pre-broker failures, each in its own arm, and stops before the
/// broker sees anything.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn the_builder_separates_the_encode_and_the_headers_failure() {
    use futures::StreamExt;

    use crate::Subscriber;
    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;
    use crate::runtime::{PublishError, Slot};

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("events.done");
    let slot = Slot::<Events, _, _>::test_entry(broker.publisher(), JsonCodec);

    let encode = slot
        .message(&Progress::new())
        .publish()
        .await
        .expect_err("the codec cannot encode this payload");
    assert!(
        matches!(encode, PublishError::Encode(_)),
        "the encode arm must be distinguishable from a broker rejection: {encode:?}",
    );

    let headers = NestedMeta {
        inner: Meta { task_id: 7 },
    };
    let rejected = slot
        .message(&Done { key: "out/1" })
        .with_headers(&headers)
        .publish()
        .await
        .expect_err("a nested struct is not a header value");
    assert!(
        matches!(rejected, PublishError::Headers(_)),
        "the headers arm names what failed: {rejected:?}",
    );
    assert!(
        rejected
            .to_string()
            .contains("serializing the typed headers"),
        "the message must point at the headers: {rejected}",
    );

    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(
        futures::poll!(stream.next()).is_pending(),
        "nothing may be published once a position fails",
    );
}
