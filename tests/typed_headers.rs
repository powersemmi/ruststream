//! Typed message headers end to end: the `FromHeaders` extractor parses the delivery headers
//! before the body runs (failing by the subscriber's decode policy), and the `Out` slot
//! dictionary publishes typed messages - destination from the marker's `#[publishes(..)]`
//! declaration, headers from the message's `#[message(headers(..))]` contract.

#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, FromHeaders, HandlerResult, Out, RustStream};
use ruststream::testing::TestApp;
use ruststream::{
    Message, OutMessages, OutSlot, OutgoingMessage, Publisher, TransactionalPublisher, subscriber,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Chunk {
    seq: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ChunkMeta {
    task_id: u64,
    chunk_no: u32,
    trace: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct DoneMeta {
    task_id: u64,
}

#[derive(Message, Serialize, Deserialize, Debug, PartialEq)]
#[message(headers(DoneMeta))]
struct ChunkDone {
    output_key: String,
}

#[derive(Message, Serialize, Deserialize, Debug, PartialEq)]
struct Progress {
    percent: u8,
}

#[derive(OutSlot)]
#[publishes(
    ChunkDone = "chunks.done",
    Progress = "chunks.progress",
    Vec<Frame> = "chunks.frames"
)]
struct Events;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Frame {
    offset: u64,
}

// A declared message set as an enum: the variants' models are the set; the enum itself is a
// type-level declaration and is never constructed. A bare collection works as a model (its
// header contract is none by definition; a newtype declares one).
#[derive(OutMessages)]
enum ConvertSends {
    #[allow(dead_code)]
    Progress(Progress),
    #[allow(dead_code)]
    Done(ChunkDone),
    #[allow(dead_code)]
    Frames(Vec<Frame>),
}

// --- the full path: extract typed headers, publish through the slot dictionary; the third
// Out position declares the set as a #[derive(OutMessages)] enum ---

#[subscriber("chunks.raw")]
async fn convert(
    chunk: &Chunk,
    FromHeaders(meta): FromHeaders<ChunkMeta>,
    Out(events): Out<impl Publisher, Events, ConvertSends>,
) -> HandlerResult {
    // No headers contract on Progress: publish directly.
    if events
        .publish_typed(&Progress { percent: 100 })
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    // ChunkDone declares DoneMeta: with_headers is required by the contract.
    let done = ChunkDone {
        output_key: format!("out/{}/{}", meta.task_id, meta.chunk_no),
    };
    let done_meta = DoneMeta {
        task_id: meta.task_id,
    };
    if events
        .with_headers(&done_meta)
        .publish_typed(&done)
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    // A Vec payload publishes like any declared model.
    let frames = vec![Frame { offset: chunk.seq }];
    if events.publish_typed(&frames).await.is_err() {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn from_headers_extracts_and_dictionary_publishes() {
    let app =
        RustStream::new(AppInfo::new("chunks", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(convert).out(Events, MemoryPublish).mount();
        });
    let tb = TestApp::start(app).await.expect("start");

    let meta = ChunkMeta {
        task_id: 7,
        chunk_no: 3,
        trace: None,
    };
    tb.broker::<MemoryBroker>()
        .publish_with_headers("chunks.raw", &Chunk { seq: 1 }, &meta)
        .await
        .expect("publish");

    let broker = tb.broker::<MemoryBroker>();
    broker
        .subscriber("chunks.raw")
        .assert_called_once()
        .settled(HandlerResult::Ack);

    // The destinations come from the dictionary, not from handler code.
    broker
        .published::<Progress>("chunks.progress")
        .assert_called_once()
        .with(&Progress { percent: 100 });
    let done = broker
        .published::<ChunkDone>("chunks.done")
        .assert_called_once();
    assert_eq!(
        done.decoded(),
        vec![ChunkDone {
            output_key: "out/7/3".to_owned()
        }]
    );
    // The typed headers landed on the wire, string-encoded and flattened per field.
    let messages = done.messages().to_vec();
    assert_eq!(messages[0].headers().get_str("task_id"), Some("7"));

    // The Vec payload went to its own declared channel.
    broker
        .published::<Vec<Frame>>("chunks.frames")
        .assert_called_once()
        .with(&vec![Frame { offset: 1 }]);

    // Slot attribution sees all three declared publishes.
    assert_eq!(tb.out::<Events>().messages().len(), 3);
}

// --- capability composition: the first Out position stays the capability vocabulary. The
// bound demands a transactional live publisher (statically checked against the policy at the
// include site), the declared publishes (an inline tuple this time) ride inside the
// transaction, and the whole capability surface stays reachable on the value, so a computed
// destination keeps the byte-level escape hatch. ---

#[subscriber("txn.raw")]
async fn transactional_convert(
    chunk: &Chunk,
    Out(events): Out<impl TransactionalPublisher, Events, (ChunkDone, Progress)>,
) -> HandlerResult {
    if events.begin_transaction().await.is_err() {
        return HandlerResult::retry();
    }
    let done = ChunkDone {
        output_key: format!("txn/{}", chunk.seq),
    };
    let done_meta = DoneMeta { task_id: chunk.seq };
    if events
        .publish_typed(&Progress { percent: 50 })
        .await
        .is_err()
        || events
            .with_headers(&done_meta)
            .publish_typed(&done)
            .await
            .is_err()
        || events.commit().await.is_err()
    {
        return HandlerResult::retry();
    }
    // The Publisher supertrait: a per-message computed destination stays available.
    let audit = format!("audit.{}", chunk.seq);
    if events
        .publish(OutgoingMessage::new(&audit, b"seen"))
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_out_composes_with_transactional_capability() {
    let app =
        RustStream::new(AppInfo::new("chunks", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(transactional_convert)
                .out(Events, MemoryPublish)
                .mount();
        });
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    broker
        .publish("txn.raw", &Chunk { seq: 9 })
        .await
        .expect("publish");
    broker
        .subscriber("txn.raw")
        .assert_called_once()
        .settled(HandlerResult::Ack);

    broker
        .published::<Progress>("chunks.progress")
        .assert_called_once()
        .with(&Progress { percent: 50 });
    broker
        .published::<ChunkDone>("chunks.done")
        .assert_called_once();
    broker.published::<()>("audit.9").assert_called_once();
}

// --- failure policy: a header contract violation follows on_failure(decode = ..) ---

#[subscriber("audit")]
async fn strict(_chunk: &Chunk, FromHeaders(meta): FromHeaders<ChunkMeta>) -> HandlerResult {
    let _ = meta;
    HandlerResult::Ack
}

#[subscriber("lenient", on_failure(decode = skip))]
async fn lenient(_chunk: &Chunk, FromHeaders(meta): FromHeaders<ChunkMeta>) -> HandlerResult {
    let _ = meta;
    HandlerResult::retry()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn header_contract_violation_follows_decode_policy() {
    let app =
        RustStream::new(AppInfo::new("chunks", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(strict);
            b.include(lenient);
        });
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    // Missing headers: the default policy drops, and the body never runs.
    broker
        .publish("audit", &Chunk { seq: 1 })
        .await
        .expect("publish");
    broker
        .subscriber("audit")
        .assert_called_once()
        .settled(HandlerResult::drop())
        .assert_last_failed_to_decode();

    // on_failure(decode = skip) covers the header contract too: the delivery is acked past,
    // and the body (which would retry) never runs.
    broker
        .publish("lenient", &Chunk { seq: 2 })
        .await
        .expect("publish");
    broker
        .subscriber("lenient")
        .assert_called_once()
        .settled(HandlerResult::Ack);
}

// --- raw input composes with FromHeaders: bytes body, typed header contract ---

#[subscriber("frames", raw, on_failure(decode = skip))]
async fn frame(payload: &[u8], FromHeaders(meta): FromHeaders<ChunkMeta>) -> HandlerResult {
    assert!(!payload.is_empty());
    assert_eq!(meta.chunk_no, 3);
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_input_composes_with_from_headers() {
    let app =
        RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(frame);
        });
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    let meta = ChunkMeta {
        task_id: 9,
        chunk_no: 3,
        trace: Some("t-1".to_owned()),
    };
    broker
        .publish_with_headers("frames", &Chunk { seq: 5 }, &meta)
        .await
        .expect("publish");
    broker
        .subscriber("frames")
        .assert_called_once()
        .settled(HandlerResult::Ack);

    // And the raw handler still applies the decode policy to a broken contract.
    broker
        .publish_raw("frames", b"\x00")
        .await
        .expect("publish");
    broker
        .subscriber("frames")
        .assert_called(2)
        .settled(HandlerResult::Ack);
}

// --- no declared set: publish_typed stays available, gated by the dictionary alone ---

#[subscriber("unrestricted.raw")]
async fn unrestricted(chunk: &Chunk, Out(events): Out<impl Publisher, Events>) -> HandlerResult {
    let _ = chunk;
    if events
        .publish_typed(&Progress { percent: 1 })
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrestricted_slot_publishes_any_dictionary_type() {
    let app =
        RustStream::new(AppInfo::new("chunks", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(unrestricted).out(Events, MemoryPublish).mount();
        });
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    broker
        .publish("unrestricted.raw", &Chunk { seq: 3 })
        .await
        .expect("publish");
    broker
        .subscriber("unrestricted.raw")
        .assert_called_once()
        .settled(HandlerResult::Ack);
    broker
        .published::<Progress>("chunks.progress")
        .assert_called_once()
        .with(&Progress { percent: 1 });
}

// --- regression: derive(Message) compiles on lifetime-, type-, and const-generic types (the
// emitted OutMessages impl adds its own type parameter after the user's generics) ---

mod generic_message_derives {
    #![allow(dead_code)]

    use ruststream::Message;

    #[derive(Message)]
    struct Borrowed<'a> {
        s: &'a str,
    }

    #[derive(Message)]
    struct Wrapper<T: Clone>(T);

    #[derive(Message)]
    struct WithWhere<T>(T)
    where
        T: Send;

    #[derive(Message)]
    struct Fixed<const N: usize>([u8; N]);
}
