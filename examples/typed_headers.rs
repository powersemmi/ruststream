//! Typed message headers: one struct declares the header contract, and it drives all three
//! surfaces at once - runtime extraction (`Headers`), the outgoing typed publish path (the
//! `Out` slot dictionary and the reply form), and the generated `AsyncAPI` document (headers
//! schemas next to the payloads). Driven through the real dispatch path with the in-process
//! `TestApp` harness.
//!
//! ```text
//! cargo run --example typed_headers --features testing,macros,memory,json,asyncapi
//! ```

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, Headers, Message, Out, RustStream};
use ruststream::schemars::JsonSchema;
use ruststream::testing::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

// The header contracts: flat structs whose fields name headers. On the wire every value is a
// string-encoded header entry; the schema describes the logical types.
// --8<-- [start:contracts]
#[derive(Serialize, Deserialize, JsonSchema)]
struct ChunkMeta {
    task_id: u64,
    chunk_no: u32,
    chunks_total: u32,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct DoneMeta {
    task_id: u64,
    duration_ms: u64,
}
// --8<-- [end:contracts]

// The outgoing messages: one derive declares everything about being sent - the destination, and
// the header contract that comes with it - so the publish builder demands the right headers and
// the AsyncAPI document shows them next to the payload.
// --8<-- [start:messages]
#[derive(Outgoing, Serialize, Deserialize, JsonSchema)]
#[outgoing(name = "chunks.done", headers = DoneMeta)]
struct ChunkDone {
    output_key: String,
}

#[derive(Outgoing, Serialize, Deserialize, JsonSchema)]
#[outgoing(name = "chunks.progress")]
struct Progress {
    percent: u8,
}
// --8<-- [end:messages]

// The slot lists what it may publish; where each type goes is the type's own declaration.
// --8<-- [start:dictionary]
#[derive(OutSlot)]
#[publishes(ChunkDone, Progress)]
struct Events;
// --8<-- [end:dictionary]

// Headers parses the delivery headers into the contract before the body runs; a missing or
// unparsable header settles the delivery by `on_failure(decode = ..)` (drop by default). The
// Out parameter's optional third position declares the message types this handler publishes:
// destinations come from each type's declaration, headers from its contract - `Progress`
// publishes bare, `ChunkDone` does not compile without `.with_headers(&meta)`.
// --8<-- [start:handler]
/// The body keeps the chunk as bytes, so the input is a type of its own rather than a decoded
/// model: the derive gives the newtype the delivery's payload as it arrives.
#[derive(Deserialized)]
struct Chunk<'a>(&'a [u8]);

#[subscriber("chunks.raw")]
async fn convert(
    chunk: &Chunk<'_>,
    Headers(meta): Headers<ChunkMeta>,
    Out(events): Out<impl Publisher, Events, (ChunkDone, Progress)>,
) -> HandlerOutcome {
    let percent = u8::try_from(meta.chunk_no * 100 / meta.chunks_total.max(1)).unwrap_or(100);
    if events
        .message(&Progress { percent })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }

    let done = ChunkDone {
        output_key: format!("chunks/{}/{}.part", meta.task_id, meta.chunk_no),
    };
    let done_meta = DoneMeta {
        task_id: meta.task_id,
        duration_ms: chunk.0.len() as u64,
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
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// The reply form gets the same treatment from the reply type's contract: the generated document
// declares a send operation for "jobs.status" with `DoneMeta` as the headers schema. At runtime
// reply headers stay with `PublishTransform`, which can serialize a contract with
// `headers_mut().insert_typed(&meta)`.
// --8<-- [start:reply]
#[derive(Deserialize, JsonSchema)]
struct StatusRequest {
    task_id: u64,
}

// The reply is sent where the `publish(..)` clause says, so the type declares no name of its
// own - only the contract that travels with it.
#[derive(Outgoing, Serialize, JsonSchema)]
#[outgoing(headers = DoneMeta)]
struct StatusReply {
    done: bool,
}

#[subscriber("jobs.status-requests", publish("jobs.status"))]
async fn status(req: &StatusRequest) -> StatusReply {
    StatusReply {
        done: req.task_id.is_multiple_of(2),
    }
}
// --8<-- [end:reply]

// Headers stay per-delivery on a batch too, so the page pairs each element with its own
// contract: a `Message<ChunkMeta, Progress>` element carries its decoded headers next to its
// payload, and an element failing either the payload decode or the contract is settled by the
// decode policy instead of reaching the handler.
// --8<-- [start:batch]
#[subscriber("chunks.bulk")]
async fn bulk(reports: &[Message<ChunkMeta, Progress>]) -> HandlerOutcome {
    for report in reports {
        let meta = &report.headers;
        println!(
            "task {}: chunk {} of {} at {}%",
            meta.task_id, meta.chunk_no, meta.chunks_total, report.body.percent,
        );
    }
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:mounts]
    let app = RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(convert).out(Events, MemoryPublish).build();
            b.include(status);
            b.include(bulk);
        },
    );
    // --8<-- [end:mounts]

    let tb = TestApp::start(app).await?;
    // --8<-- [start:drive]
    let meta = ChunkMeta {
        task_id: 7,
        chunk_no: 3,
        chunks_total: 12,
    };
    tb.broker::<MemoryBroker>()
        .raw(&[0_u8; 16])
        .with_headers(&meta)
        .to("chunks.raw")
        .publish()
        .await?;

    let done = tb
        .broker::<MemoryBroker>()
        .published::<ChunkDone>("chunks.done")
        .assert_called_once();
    println!("published: {:?}", done.decoded()[0].output_key);
    println!(
        "task_id header: {:?}",
        done.messages()[0].headers().get_str("task_id")
    );
    // --8<-- [end:drive]
    Ok(())
}
