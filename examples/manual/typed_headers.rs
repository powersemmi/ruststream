//! Typed headers, written without the `macros` feature: a serde struct is the header contract a
//! handler reads and a publish writes. `MessageHeaders` declares the contract of a message type,
//! `Headers<T>` resolves through `FromContext` in the body, and `Message<H, T>` is an input of its
//! own. The tests are the ones of `examples/typed_headers.rs`.
//!
//! ```text
//! cargo test --example manual_typed_headers --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::any::type_name;

use serde::{Deserialize, Serialize};

use ruststream::memory::prelude::*;

// --8<-- [start:contracts]
/// What travels in the headers of a chunk: each field is one header, its value a string.
#[derive(Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
struct ChunkMeta {
    task_id: u64,
    chunk_no: u32,
    chunks_total: u32,
}

/// What travels in the headers of a finished chunk.
#[derive(Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
struct DoneMeta {
    task_id: u64,
    chunk_no: u32,
}
// --8<-- [end:contracts]

// --8<-- [start:messages]
/// A chunk: publishing one does not compile without its `ChunkMeta`.
#[derive(Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
struct Chunk {
    size: u64,
}

impl OutgoingDestination for Chunk {
    type Form = FixedName;

    const DESTINATION: &'static str = "chunks";
}

impl MessageHeaders for Chunk {
    type Contract = WithHeaders<ChunkMeta>;
}

/// A finished chunk: publishing one does not compile without its `DoneMeta`.
#[derive(Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
struct ChunkDone {
    output_key: String,
}

impl OutgoingDestination for ChunkDone {
    type Form = FixedName;

    const DESTINATION: &'static str = "chunks.done";
}

impl MessageHeaders for ChunkDone {
    type Contract = WithHeaders<DoneMeta>;
}
// --8<-- [end:messages]

// --8<-- [start:dictionary]
/// The slot the converter publishes through, and what it may publish.
struct Events;

impl OutSlot for Events {
    const NAME: &'static str = "Events";
    type Destination = Reads;

    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        <ChunkDone as OutMessages<Self>>::outgoing()
    }
}

impl PublishedThrough<Events> for ChunkDone {}

impl<M: OutSlot> OutMessages<M> for ChunkDone {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            Self::DESTINATION,
            type_name::<Self>(),
        )]
    }
}
// --8<-- [end:dictionary]

fn meta(chunk_no: u32) -> ChunkMeta {
    ChunkMeta {
        task_id: 7,
        chunk_no,
        chunks_total: 12,
    }
}

mod handler {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde_json::json;

    use super::{Chunk, ChunkDone, ChunkMeta, DoneMeta, Events, meta};

    // --8<-- [start:handler]
    /// The contract is parsed before the work starts; what the body publishes carries its own.
    struct Convert;

    impl<Entry> Handle<Chunk, (), Outs<(Entry,)>> for Convert
    where
        Entry: OutEntry<Events, Wire: Publisher>,
    {
        async fn handle(
            &self,
            chunk: &Chunk,
            outs: &Outs<(Entry,)>,
            ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            let Headers(meta) = Headers::<ChunkMeta>::from_context(ctx).await?;
            tracing::info!(meta.task_id, meta.chunk_no, chunk.size, "converting");
            let done = ChunkDone {
                output_key: format!("chunks/{}/{}.part", meta.task_id, meta.chunk_no),
            };
            let done_meta = DoneMeta {
                task_id: meta.task_id,
                chunk_no: meta.chunk_no,
            };
            let sent = outs
                .get(Events)
                .message(&done)
                .with_headers(&done_meta)
                .publish()
                .await;
            sent.map_err(|_| HandlerOutcome::retry())
        }
    }
    // --8<-- [end:handler]

    fn app() -> impl App {
        RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("chunks", Convert).build())
                .out(Events, Publish)
                .build();
        })
    }

    #[tokio::test]
    async fn the_contract_comes_in_and_goes_out() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Chunk { size: 16 })
            .with_headers(&meta(3))
            .publish()
            .await?;
        tb.broker::<MemoryBroker>()
            .published::<ChunkDone>("chunks.done")
            .assert_called_once()
            .with_header("task_id", "7")
            .with_header("chunk_no", "3");

        // A chunk without its headers is bad input: the extractor drops it.
        tb.publish("chunks", &json!({ "size": 16 })).await?;
        tb.broker::<MemoryBroker>()
            .subscriber("chunks")
            .assert_called(2)
            .settled(HandlerOutcome::drop());
        Ok(())
    }
}

mod message {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::{Chunk, ChunkMeta, meta};

    // --8<-- [start:message]
    /// The payload and its contract arrive decoded together.
    struct Store;

    impl Handle<Message<ChunkMeta, Chunk>> for Store {
        fn handle(
            &self,
            chunk: &Message<ChunkMeta, Chunk>,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            let meta = &chunk.headers;
            if meta.chunk_no >= meta.chunks_total {
                // A chunk past the end of its task.
                return ready(Err(HandlerOutcome::drop()));
            }
            tracing::info!(meta.task_id, meta.chunk_no, chunk.body.size, "stored");
            ready(Ok(()))
        }
    }
    // --8<-- [end:message]

    fn app() -> impl App {
        RustStream::new(AppInfo::new("store", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("chunks", Store).build());
        })
    }

    #[tokio::test]
    async fn the_body_reads_the_headers_it_declared() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for chunk_no in [3, 12] {
            tb.message(&Chunk { size: 16 })
                .with_headers(&meta(chunk_no))
                .publish()
                .await?;
        }

        assert_eq!(
            tb.broker::<MemoryBroker>().subscriber("chunks").outcomes(),
            [Outcome::Ack, Outcome::Drop],
        );
        Ok(())
    }
}

mod stamp {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::runtime::{Outgoing, SlotContext};
    use ruststream::testing::TestApp;
    use serde::Serialize;

    use super::{Chunk, ChunkDone, ChunkMeta, DoneMeta, Events, meta};

    /// The converter of the first example; what this one adds is the transform.
    struct Convert;

    impl<Entry> Handle<Chunk, (), Outs<(Entry,)>> for Convert
    where
        Entry: OutEntry<Events, Wire: Publisher>,
    {
        async fn handle(
            &self,
            _chunk: &Chunk,
            outs: &Outs<(Entry,)>,
            ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            let Headers(meta) = Headers::<ChunkMeta>::from_context(ctx).await?;
            let done = ChunkDone {
                output_key: format!("chunks/{}/{}.part", meta.task_id, meta.chunk_no),
            };
            let done_meta = DoneMeta {
                task_id: meta.task_id,
                chunk_no: meta.chunk_no,
            };
            let sent = outs
                .get(Events)
                .message(&done)
                .with_headers(&done_meta)
                .publish()
                .await;
            sent.map_err(|_| HandlerOutcome::retry())
        }
    }

    // --8<-- [start:stamp]
    /// Where the service runs, as a contract stamped on everything the slot sends.
    #[derive(Clone, Serialize)]
    struct Origin {
        region: &'static str,
        canary: bool,
    }

    struct Stamp(Origin);

    impl<Options> PublishTransform<ForSlot, Options> for Stamp {
        type Destination = Reads;

        fn apply(
            &self,
            out: &mut Outgoing<'_>,
            _options: &mut Option<Options>,
            _cx: &SlotContext<'_>,
        ) {
            if let Err(error) = out.headers_mut().insert_typed(&self.0) {
                tracing::warn!(%error, "origin headers left out");
            }
        }
    }

    fn app() -> impl App {
        let origin = Origin {
            region: "eu-west",
            canary: false,
        };
        RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("chunks", Convert).build())
                .out(Events, Publish)
                .transform(Stamp(origin))
                .build();
        })
    }
    // --8<-- [end:stamp]

    #[tokio::test]
    async fn every_publish_carries_the_origin() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Chunk { size: 16 })
            .with_headers(&meta(3))
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .published::<ChunkDone>("chunks.done")
            .assert_called_once()
            .with_header("task_id", "7")
            .with_header("region", "eu-west")
            .with_header("canary", "false");
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
