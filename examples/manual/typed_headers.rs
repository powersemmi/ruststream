//! Typed message headers without the `macros` feature: the same declarations `#[derive(Outgoing)]`,
//! `#[derive(OutSlot)]` and `#[subscriber]` emit, written out. One contract struct still drives all
//! three surfaces - runtime extraction (`Headers`), the outgoing typed publish path (the `Out` slot
//! dictionary and the reply form), and the generated `AsyncAPI` document - because each surface is a
//! public trait a type implements. Driven through the real dispatch path with the in-process
//! `TestApp` harness.
//!
//! ```text
//! cargo run --example manual_typed_headers --no-default-features --features testing,memory,json,asyncapi
//! ```

use std::any::type_name;
use std::convert::Infallible;
use std::future::{Future, ready};

use ruststream::codec::Codec;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::runtime::{
    ContainsMessage, Input, MessageWire, OutMessages, OutgoingMessageMetadata, PublishedThrough,
    SerializedWire, SlotPos, SoloDeserialized,
};
use ruststream::schemars::{JsonSchema, schema_for};
use ruststream::testing::TestApp;
use ruststream::{
    CallerName, FixedName, MessageHeaders, NoHeaders, OutgoingDestination, WithHeaders,
};
use serde::{Deserialize, Serialize};

/// The schema a declaration contributes to the document. The derives reach it through an autoref
/// probe (so a type without `JsonSchema` still compiles); a hand-written declaration knows its own
/// types and asks `schemars` directly.
fn schema_of<T: JsonSchema>() -> String {
    schema_for!(T).as_value().to_string()
}

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

// The outgoing messages. What the derive writes for `#[outgoing(name = "chunks.done", headers =
// DoneMeta)]` is these four impls: where the type goes (`OutgoingDestination`, with the form that
// decides whether the publish builder still asks for a destination), the header contract that comes
// with it (`MessageHeaders`), its document metadata (`MessageInfo`), and its membership in a
// one-element message set (`ContainsMessage` / `OutMessages`), so the type can be named alone as a
// slot's declared set.
// --8<-- [start:messages]
#[derive(Serialize, Deserialize, JsonSchema)]
struct ChunkDone {
    output_key: String,
}

impl OutgoingDestination for ChunkDone {
    type Form = FixedName;
    const ADDRESS: &'static str = "chunks.done";
}

impl MessageHeaders for ChunkDone {
    type Contract = WithHeaders<DoneMeta>;
}

impl MessageInfo for ChunkDone {
    const NAME: &'static str = "ChunkDone";
}

impl ContainsMessage<Self, SlotPos<0>> for ChunkDone {}

impl<M: OutSlot> OutMessages<M> for ChunkDone {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new(Self::ADDRESS, type_name::<Self>())
                .with_message_name(Some(Self::NAME))
                .with_payload_schema(Some(schema_of::<Self>()))
                .with_headers_schema(Some(schema_of::<DoneMeta>())),
        ]
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct Progress {
    percent: u8,
}

impl OutgoingDestination for Progress {
    type Form = FixedName;
    const ADDRESS: &'static str = "chunks.progress";
}

// No `headers = ..` in the declaration: the publish builder demands no contract for this type.
impl MessageHeaders for Progress {
    type Contract = NoHeaders;
}

impl MessageInfo for Progress {
    const NAME: &'static str = "Progress";
}

impl ContainsMessage<Self, SlotPos<0>> for Progress {}

impl<M: OutSlot> OutMessages<M> for Progress {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new(Self::ADDRESS, type_name::<Self>())
                .with_message_name(Some(Self::NAME))
                .with_payload_schema(Some(schema_of::<Self>())),
        ]
    }
}
// --8<-- [end:messages]

// The slot lists what it may publish; where each type goes is the type's own declaration. The
// `#[publishes(..)]` list expands into one `PublishedThrough` impl per member (what the publish
// builder admits) plus the `outgoing()` override (what the document reports), so the two cannot
// drift apart.
// --8<-- [start:dictionary]
struct Events;

impl OutSlot for Events {
    const NAME: &'static str = "Events";

    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        let mut entries = <ChunkDone as OutMessages<Self>>::outgoing();
        entries.extend(<Progress as OutMessages<Self>>::outgoing());
        entries
    }
}

impl PublishedThrough<Events> for ChunkDone {}
impl PublishedThrough<Events> for Progress {}
// --8<-- [end:dictionary]

// The producer's side of the byte input below: a chunk as it arrives from outside, with no model
// of its own. What `#[derive(Serialized)]` writes is the bytes plus the wire spelling that routes
// the type onto the serialized wire, so no codec touches them; the contract is the same one the
// handler extracts, and no destination is declared, so the call site names one.
struct RawChunk(Vec<u8>);

impl Serialized for RawChunk {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageWire for RawChunk {
    type Wire = SerializedWire;
}

impl OutgoingDestination for RawChunk {
    type Form = CallerName;
}

impl MessageHeaders for RawChunk {
    type Contract = WithHeaders<ChunkMeta>;
}

// `Headers<ChunkMeta>` is an extractor, so the body resolves it before its own work, under the
// subscriber's `on_failure(decode = ..)` policy (drop by default) - the call the attribute inserts
// for a `Headers` parameter. It is what a raw body reaches for: the `Message<H, P>` input pairs a
// contract with a *decoded* payload, and this one keeps the chunk as bytes. The slot arena is the
// body's injections axis: the body is generic over the slot's wired publisher, so the concrete
// type is inferred from the policy the mount site attaches, and the marker carries the declared
// message set - destinations come from each type's declaration, headers from its contract, so
// `Progress` publishes bare and `ChunkDone` does not compile without `.with_headers(&meta)`.
// --8<-- [start:handler]
// The raw input type. What `#[derive(Deserialized)]` would write is these two impls: the
// construction, which borrows the delivery's bytes, and the `Input` spelling that routes
// `&Chunk<'_>` onto the self-deserializing lane.
struct Chunk<'a>(&'a [u8]);

impl Deserialized for Chunk<'_> {
    type Output<'a> = Chunk<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Chunk<'_>, Self::Error> {
        Ok(Chunk(payload))
    }
}

impl Input for Chunk<'_> {
    type Axis = SoloDeserialized<Chunk<'static>>;
}

#[derive(Clone, Copy)]
struct Convert;

impl<'p, P, Enc> Handle<Chunk<'p>, (), Outs<(Slot<Events, P, Enc>,)>> for Convert
where
    P: Publisher,
    Enc: Codec + Send + Sync,
{
    async fn handle(
        &self,
        chunk: &Chunk<'p>,
        outs: &Outs<(Slot<Events, P, Enc>,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        // Read the policy before the extraction takes the mutable borrow.
        let policy = ctx.decode_policy();
        let Headers(meta) = Headers::<ChunkMeta>::extract(&mut *ctx, policy)?;
        let events = outs.get(Events);

        let percent = u8::try_from(meta.chunk_no * 100 / meta.chunks_total.max(1)).unwrap_or(100);
        if events
            .message(&Progress { percent })
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
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
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

// The reply form gets the same treatment from the reply type's contract: `MessageHeaders` is what
// travels with `StatusReply`, and at runtime reply headers stay with `PublishTransform`, which can
// serialize a contract with `headers_mut().insert_typed(&meta)`. Where the attribute's
// `publish("jobs.status")` clause names the destination, `.reply().to(..)` names it, and the
// registration is documented by default, so the send operation it declares reports its schemas.
// --8<-- [start:reply]
#[derive(Deserialize, JsonSchema)]
struct StatusRequest {
    task_id: u64,
}

// The reply is sent where `reply_name` says, so the type declares no name of its own - the
// `CallerName` form - only the contract that travels with it.
#[derive(Serialize, JsonSchema)]
struct StatusReply {
    done: bool,
}

impl OutgoingDestination for StatusReply {
    type Form = CallerName;
}

impl MessageHeaders for StatusReply {
    type Contract = WithHeaders<DoneMeta>;
}

impl MessageInfo for StatusReply {
    const NAME: &'static str = "StatusReply";
}

struct Status;

impl Handle<StatusRequest, StatusReply> for Status {
    fn handle(
        &self,
        req: &StatusRequest,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<StatusReply, HandlerOutcome>> {
        ready(Ok(StatusReply {
            done: req.task_id.is_multiple_of(2),
        }))
    }
}
// --8<-- [end:reply]

// Headers stay per-delivery on a batch too, so the contracts arrive as one per element: a page of
// `Message<ChunkMeta, Progress>` pairs each payload with its own contract, and an element failing
// either the payload decode or the contract is settled by the decode policy instead of reaching the
// handler. The input spelling is the whole declaration - nothing else names the batch.
// --8<-- [start:batch]
struct Bulk;

impl Handle<[Message<ChunkMeta, Progress>]> for Bulk {
    fn handle(
        &self,
        reports: &[Message<ChunkMeta, Progress>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        for report in reports {
            let meta = &report.headers;
            println!(
                "task {}: chunk {} of {} at {}%",
                meta.task_id, meta.chunk_no, meta.chunks_total, report.body.percent,
            );
        }
        ready(Ok(()))
    }
}
// --8<-- [end:batch]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:mounts]
    // Every registration is documented by default, so the schemas follow from the axes each body
    // declared: the pair input of `Bulk` carries both the payload and the contract schema, and the
    // reply of `Status` carries its own.
    let app = RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("chunks.raw", Convert).build())
                .out(Events, MemoryPublish)
                .build();
            b.include(
                subscriber("jobs.status-requests", Status)
                    .reply()
                    .to("jobs.status")
                    .build(),
            );
            b.include(subscriber("chunks.bulk", Bulk).build());
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
        .message(&RawChunk(vec![0; 16]))
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
