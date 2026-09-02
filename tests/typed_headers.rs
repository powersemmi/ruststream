//! Typed message headers end to end: the `Headers` extractor parses the delivery headers
//! before the body runs (failing by the subscriber's decode policy), and a handler publishing
//! through an `Out` slot fills the headers position from the message's declared contract.
//!
//! The publish builder's own surface (every destination form, every publisher kind) is covered
//! by `tests/publish_builder.rs`; what these cases add is the header half next to it.

#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Headers, Out, Router, RustStream};
use ruststream::testing::TestApp;
use ruststream::{
    Buffered, Name, OutMessages, OutSlot, Outgoing, Publisher, TransactionalPublisher, nonzero,
    subscriber,
};
use serde::{Deserialize, Serialize};

#[derive(Outgoing, Serialize, Deserialize, Debug, PartialEq)]
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

#[derive(Outgoing, Serialize, Deserialize, Debug, PartialEq)]
#[outgoing(name = "chunks.done", headers = DoneMeta)]
struct ChunkDone {
    output_key: String,
}

#[derive(Outgoing, Serialize, Deserialize, Debug, PartialEq)]
#[outgoing(name = "chunks.progress")]
struct Progress {
    percent: u8,
}

#[derive(OutSlot)]
#[publishes(ChunkDone, Progress, Frames)]
struct Events;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Frame {
    offset: u64,
}

// A foreign collection carries no declaration of its own, so a newtype makes it a message: the
// derive declares the destination once, and the payload stays the bare sequence.
#[derive(Outgoing, Serialize, Deserialize, Debug, PartialEq)]
#[outgoing(name = "chunks.frames")]
struct Frames(Vec<Frame>);

// A declared message set as an enum: the variants' models are the set; the enum itself is a
// type-level declaration and is never constructed.
#[derive(OutMessages)]
enum ConvertSends {
    #[allow(dead_code)]
    Progress(Progress),
    #[allow(dead_code)]
    Done(ChunkDone),
    #[allow(dead_code)]
    Frames(Frames),
}

// --- the full path: extract typed headers, publish through the slot; the third Out position
// declares the set as a #[derive(OutMessages)] enum ---

#[subscriber("chunks.raw")]
async fn convert(
    chunk: &Chunk,
    Headers(meta): Headers<ChunkMeta>,
    Out(events): Out<impl Publisher, Events, ConvertSends>,
) -> HandlerOutcome {
    // No headers contract on Progress: publish directly.
    if events
        .message(&Progress { percent: 100 })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    // ChunkDone declares DoneMeta: with_headers is required by the contract.
    let done = ChunkDone {
        output_key: format!("out/{}/{}", meta.task_id, meta.chunk_no),
    };
    let done_meta = DoneMeta {
        task_id: meta.task_id,
    };
    if events
        .message(&done)
        .with_headers(&done_meta)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    // A sequence payload publishes like any declared model.
    let frames = Frames(vec![Frame { offset: chunk.seq }]);
    if events.message(&frames).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn from_headers_extracts_and_declared_messages_publish() {
    let app =
        RustStream::new(AppInfo::new("chunks", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(convert).out(Events, MemoryPublish).build();
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
        .settled(HandlerOutcome::ack());

    // The destinations come from each message's own declaration, not from handler code.
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

    // The sequence payload went to its own declared channel.
    broker
        .published::<Frames>("chunks.frames")
        .assert_called_once()
        .with(&Frames(vec![Frame { offset: 1 }]));

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
) -> HandlerOutcome {
    if events.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    let done = ChunkDone {
        output_key: format!("txn/{}", chunk.seq),
    };
    let done_meta = DoneMeta { task_id: chunk.seq };
    if events
        .message(&Progress { percent: 50 })
        .publish()
        .await
        .is_err()
        || events
            .message(&done)
            .with_headers(&done_meta)
            .publish()
            .await
            .is_err()
        || events.commit().await.is_err()
    {
        return HandlerOutcome::retry();
    }
    // The Publisher supertrait: a per-message computed destination stays available.
    let audit = format!("audit.{}", chunk.seq);
    if events.raw(b"seen").to(audit).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_out_composes_with_transactional_capability() {
    let app =
        RustStream::new(AppInfo::new("chunks", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(transactional_convert)
                .out(Events, MemoryPublish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    broker
        .message(&Chunk { seq: 9 })
        .to("txn.raw")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("txn.raw")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

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
async fn strict(_chunk: &Chunk, Headers(meta): Headers<ChunkMeta>) -> HandlerOutcome {
    let _ = meta;
    HandlerOutcome::ack()
}

#[subscriber("lenient", on_failure(decode = skip))]
async fn lenient(_chunk: &Chunk, Headers(meta): Headers<ChunkMeta>) -> HandlerOutcome {
    let _ = meta;
    HandlerOutcome::retry()
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
        .message(&Chunk { seq: 1 })
        .to("audit")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("audit")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();

    // on_failure(decode = skip) covers the header contract too: the delivery is acked past,
    // and the body (which would retry) never runs.
    broker
        .message(&Chunk { seq: 2 })
        .to("lenient")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("lenient")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
}

// --- raw input composes with Headers: bytes body, typed header contract ---

#[subscriber("frames", on_failure(decode = skip))]
async fn frame(payload: &[u8], Headers(meta): Headers<ChunkMeta>) -> HandlerOutcome {
    assert!(!payload.is_empty());
    assert_eq!(meta.chunk_no, 3);
    HandlerOutcome::ack()
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
        .settled(HandlerOutcome::ack());

    // And the raw handler still applies the decode policy to a broken contract.
    broker
        .raw(b"\x00")
        .to("frames")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("frames")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
}

// --- no declared set: the publish stays available, gated by the marker's list alone ---

#[subscriber("unrestricted.raw")]
async fn unrestricted(chunk: &Chunk, Out(events): Out<impl Publisher, Events>) -> HandlerOutcome {
    let _ = chunk;
    if events
        .message(&Progress { percent: 1 })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrestricted_slot_publishes_any_listed_type() {
    let app =
        RustStream::new(AppInfo::new("chunks", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(unrestricted).out(Events, MemoryPublish).build();
        });
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    broker
        .message(&Chunk { seq: 3 })
        .to("unrestricted.raw")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("unrestricted.raw")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    broker
        .published::<Progress>("chunks.progress")
        .assert_called_once()
        .with(&Progress { percent: 1 });
}

// --- regression: the message derives compile on lifetime-, type-, and const-generic types
// (the OutMessages impl `#[derive(Outgoing)]` emits adds its own type parameter after the
// user's generics) ---

mod generic_message_derives {
    #![allow(dead_code)]

    use ruststream::{MessageInfo, Outgoing};

    #[derive(MessageInfo)]
    struct Borrowed<'a> {
        s: &'a str,
    }

    #[derive(MessageInfo)]
    struct Wrapper<T: Clone>(T);

    #[derive(Outgoing)]
    struct SentBorrowed<'a> {
        s: &'a str,
    }

    #[derive(Outgoing)]
    struct SentWrapper<T: Clone>(T);

    #[derive(Outgoing)]
    struct WithWhere<T>(T)
    where
        T: Send;

    #[derive(Outgoing)]
    struct Fixed<const N: usize>([u8; N]);
}

/// Records the shape of each batch invocation: the payload slice next to the header vector, so
/// the test can assert they line up element for element.
/// One batch invocation: the payload sequence numbers next to the header contracts behind them.
type BatchShape = (Vec<u64>, Vec<(u64, u32)>);

static BATCH_SEEN: std::sync::Mutex<Vec<BatchShape>> = std::sync::Mutex::new(Vec::new());

// A size-capped buffer, so a batch closes on the cap rather than on delivery timing and the
// per-element alignment is actually exercised across more than one element. The wait bound stays
// at its 10 ms default: a longer one would only make the suite wait for the tail batch.
#[subscriber(Buffered::<Name>::new(Name::new("chunks.bulk")).max_size(nonzero!(2)))]
async fn bulk(chunks: &[Chunk], Headers(meta): Headers<Vec<ChunkMeta>>) -> HandlerOutcome {
    let mut seen = BATCH_SEEN.lock().expect("the test holds no poisoned lock");
    seen.push((
        chunks.iter().map(|chunk| chunk.seq).collect(),
        meta.iter().map(|m| (m.task_id, m.chunk_no)).collect(),
    ));
    drop(seen);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_reads_one_header_contract_per_element() {
    let app = RustStream::new(AppInfo::new("typed-headers-batch", "1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(bulk);
        },
    );
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    // The second delivery carries no contract at all, so it is the one the decode policy drops
    // from inside an otherwise good batch.
    for seq in [1u64, 2, 3, 4] {
        if seq == 2 {
            broker
                .message(&Chunk { seq })
                .to("chunks.bulk")
                .publish()
                .await
                .expect("publish");
            continue;
        }
        let meta = ChunkMeta {
            task_id: 7,
            chunk_no: u32::try_from(seq).expect("small"),
            trace: None,
        };
        broker
            .publish_with_headers("chunks.bulk", &Chunk { seq }, &meta)
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let seen = BATCH_SEEN
        .lock()
        .expect("the test holds no poisoned lock")
        .clone();
    for (payloads, headers) in &seen {
        assert_eq!(
            payloads.len(),
            headers.len(),
            "the header vector must have one entry per delivered element",
        );
        for (seq, (_, chunk_no)) in payloads.iter().zip(headers) {
            assert_eq!(
                u64::from(*chunk_no),
                *seq,
                "header {chunk_no} landed against payload {seq}",
            );
        }
    }
    let delivered: Vec<u64> = seen
        .iter()
        .flat_map(|(payloads, _)| payloads)
        .copied()
        .collect();
    assert_eq!(
        delivered,
        vec![1, 3, 4],
        "the element failing the header contract must be dropped, the rest handled in order",
    );
    tb.shutdown().await.expect("shutdown");
}

/// What the router-mounted handler saw, so the Router path is proven to carry the contracts too.
static ROUTED_SEEN: std::sync::Mutex<Vec<(u64, u32)>> = std::sync::Mutex::new(Vec::new());

#[subscriber("chunks.routed")]
async fn routed(chunks: &[Chunk], Headers(meta): Headers<Vec<ChunkMeta>>) -> HandlerOutcome {
    let mut seen = ROUTED_SEEN.lock().expect("the test holds no poisoned lock");
    for (chunk, meta) in chunks.iter().zip(&meta) {
        seen.push((chunk.seq, meta.chunk_no));
    }
    drop(seen);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_router_path_carries_the_batch_header_contract() {
    // The chain codec form: `with_codec` and the default-codec entry point share one mount.
    let router = Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .include(routed);
    let app = RustStream::new(AppInfo::new("typed-headers-router", "1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include_router(router);
        },
    );
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    let meta = ChunkMeta {
        task_id: 4,
        chunk_no: 9,
        trace: None,
    };
    broker
        .publish_with_headers("chunks.routed", &Chunk { seq: 5 }, &meta)
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    let seen = ROUTED_SEEN
        .lock()
        .expect("the test holds no poisoned lock")
        .clone();
    assert_eq!(seen, vec![(5, 9)]);
    tb.shutdown().await.expect("shutdown");
}
