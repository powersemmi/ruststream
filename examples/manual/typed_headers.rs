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
use std::future::{Future, ready};
use std::marker::PhantomData;

use ruststream::codec::Codec;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::runtime::{
    BatchDef, BatchWithHeadersDef, BindSlots, ContainsMessage, Decoded, HasSlots, IncludeDef,
    InjectCall, InjectDef, IntoBatchResult, OutMessages, OutgoingMessageMetadata, PublishedThrough,
    PublishingCall, PublishingDef, RawBytes, SliceHandlerWithHeaders, SlotPos, SlotPublisher,
    forms,
};
use ruststream::schemars::{JsonSchema, schema_for};
use ruststream::testing::TestApp;
use ruststream::{
    CallerName, ConnectedBroker, FixedName, MessageHeaders, NoHeaders, OutgoingDestination,
    WithHeaders,
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
// with it (`MessageHeaders`), its document metadata (`Message`), and its membership in a one-element
// message set (`ContainsMessage` / `OutMessages`), so the type can be named alone as an `Out`
// parameter's declared set.
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

impl Message for ChunkDone {
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

impl Message for Progress {
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

// `Headers<ChunkMeta>` is an extractor, so the body resolves it before its own work, under the
// subscriber's `on_failure(decode = ..)` policy (drop by default) - the call the attribute inserts
// for a `Headers` parameter. The `Out` parameter is a startup injection: the definition traits live
// on a publisher-generic struct, so the concrete publisher type is inferred from the policy the
// mount site attaches, and the declared message set rides in the injection type - destinations come
// from each type's declaration, headers from its contract, so `Progress` publishes bare and
// `ChunkDone` does not compile without `.with_headers(&meta)`.
//
// `with_slots(source, body)` covers the decoded slot form; this handler borrows the payload
// undecoded and declares the headers schema itself, so it writes the definition traits out.
// --8<-- [start:handler]
#[derive(Clone, Copy)]
struct Convert;

struct ConvertDef<Slot, EncodeCodec>(PhantomData<fn() -> (Slot, EncodeCodec)>);

impl IncludeDef for Convert {
    type Form = forms::Out;
}

impl HasSlots for Convert {
    type Markers = (Events,);
}

impl<Broker, Policy, EncodeCodec> BindSlots<Broker, ((Policy, EncodeCodec),)> for Convert
where
    Broker: ConnectedBroker,
    Policy: PublishPolicy<Broker>,
{
    type Bound = ConvertDef<SlotPublisher<Policy::Live, Events>, EncodeCodec>;
    type Extra = ((Policy, EncodeCodec),);

    fn bind(self, sources: ((Policy, EncodeCodec),)) -> (Self::Bound, Self::Extra) {
        (ConvertDef(PhantomData), sources)
    }
}

impl<Slot, EncodeCodec> InjectDef for ConvertDef<Slot, EncodeCodec>
where
    Slot: Publisher + Send + Sync + 'static,
    EncodeCodec: Codec + Send + Sync + 'static,
{
    type Input = RawBytes;
    type Context = ();
    type Source = Name;
    type Injections = (Out<Slot, Events, (ChunkDone, Progress), EncodeCodec>,);

    fn source(&self) -> Name {
        Name::new("chunks.raw")
    }

    fn headers_schema(&self) -> Option<String> {
        Some(schema_of::<ChunkMeta>())
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        let mut entries = <ChunkDone as OutMessages<Events>>::outgoing();
        entries.extend(<Progress as OutMessages<Events>>::outgoing());
        entries
    }
}

impl<Slot, EncodeCodec, State> InjectCall<State> for ConvertDef<Slot, EncodeCodec>
where
    Slot: Publisher + Send + Sync + 'static,
    EncodeCodec: Codec + Send + Sync + 'static,
    State: Send + Sync,
{
    async fn call(
        &self,
        chunk: &[u8],
        injections: &Self::Injections,
        ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        // Read the policy before the extraction takes the mutable borrow.
        let policy = ctx.decode_policy();
        let Headers(meta) = match Headers::<ChunkMeta>::extract(&mut *ctx, policy) {
            Ok(value) => value,
            Err(rejection) => return rejection.into(),
        };
        let Out(events) = &injections.0;

        let percent = u8::try_from(meta.chunk_no * 100 / meta.chunks_total.max(1)).unwrap_or(100);
        if events
            .message(&Progress { percent })
            .publish()
            .await
            .is_err()
        {
            return HandlerResult::retry().into();
        }

        let done = ChunkDone {
            output_key: format!("chunks/{}/{}.part", meta.task_id, meta.chunk_no),
        };
        let done_meta = DoneMeta {
            task_id: meta.task_id,
            duration_ms: chunk.len() as u64,
        };
        if events
            .message(&done)
            .with_headers(&done_meta)
            .publish()
            .await
            .is_err()
        {
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
    }
}
// --8<-- [end:handler]

// The reply form gets the same treatment from the reply type's contract: the generated document
// declares a send operation for "jobs.status" with `DoneMeta` as the headers schema. Where the
// attribute's `publish("jobs.status")` clause names the destination, the definition names it in
// `reply_name` and lists the send operation in `outgoing`. At runtime reply headers stay with
// `PublishTransform`, which can serialize a contract with `headers_mut().insert_typed(&meta)`.
//
// `replying(source, body).to(..)` covers the reply form itself, but its `documented` opt-in reports
// payload schemas only; a send operation carrying a headers schema is declared by the definition.
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

impl Message for StatusReply {
    const NAME: &'static str = "StatusReply";
}

struct Status;

impl IncludeDef for Status {
    type Form = forms::Publishing;
}

impl PublishingDef for Status {
    type Input = Decoded<StatusRequest>;
    type Injections = ();
    type Reply = StatusReply;
    type Context = ();
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("jobs.status-requests")
    }

    fn reply_name(&self) -> &'static str {
        "jobs.status"
    }

    fn input_schema(&self) -> Option<String> {
        Some(schema_of::<StatusRequest>())
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<StatusReply>())
                .with_message_name(Some(StatusReply::NAME))
                .with_payload_schema(Some(schema_of::<StatusReply>()))
                .with_headers_schema(Some(schema_of::<DoneMeta>())),
        ]
    }
}

impl<State: Send + Sync> PublishingCall<State> for Status {
    // Nothing to await, so the future is returned directly (see `manual/quickstart.rs`).
    fn call(
        &self,
        req: &StatusRequest,
        _injections: &(),
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Result<StatusReply, HandlerResult>> + Send {
        ready(Ok(StatusReply {
            done: req.task_id.is_multiple_of(2),
        }))
    }
}
// --8<-- [end:reply]

// Headers stay per-delivery on a batch too, so the contracts arrive as one per element: the batch
// handler trait takes them as a second argument (this is what a `Headers<Vec<ChunkMeta>>` parameter
// selects), the two slices line up index for index, and an element failing either the payload decode
// or the contract is settled by the decode policy instead of reaching the handler. The batch form
// with a headers contract has no value constructor, so it stays on the definition traits.
// --8<-- [start:batch]
struct Bulk;

impl IncludeDef for Bulk {
    type Form = forms::BatchWithHeaders;
}

impl<State: Send + Sync> SliceHandlerWithHeaders<Progress, ChunkMeta, State> for Bulk {
    fn handle_slice(
        &self,
        reports: &[Progress],
        headers: Vec<ChunkMeta>,
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = BatchResult> + Send {
        for (report, meta) in reports.iter().zip(&headers) {
            println!(
                "task {}: chunk {} of {} at {}%",
                meta.task_id, meta.chunk_no, meta.chunks_total, report.percent,
            );
        }
        ready(HandlerResult::Ack.into_batch_result())
    }
}

impl BatchWithHeadersDef for Bulk {
    type Headers = ChunkMeta;
}

impl BatchDef for Bulk {
    type Input = Decoded<Progress>;
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("chunks.bulk")
    }

    fn input_schema(&self) -> Option<String> {
        Some(schema_of::<Progress>())
    }

    fn headers_schema(&self) -> Option<String> {
        Some(schema_of::<ChunkMeta>())
    }

    fn into_handler(self) -> Self {
        self
    }
}
// --8<-- [end:batch]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:mounts]
    let app = RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(Convert).out(Events, MemoryPublish).mount();
            b.include(Status);
            b.include(Bulk);
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
