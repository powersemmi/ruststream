//! Typed headers: a serde struct is the header contract a handler reads and a publish writes.
//! `Headers<T>` and `Message<H, T>` parse it from a delivery, `#[outgoing(headers = ..)]` makes a
//! publish demand it, and `HeaderMap::insert_typed` writes one from a transform. Each scenario
//! is a test.
//!
//! ```text
//! cargo test --example typed_headers --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream::memory::prelude::*;

// --8<-- [start:contracts]
/// What travels in the headers of a chunk: each field is one header, its value a string.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ChunkMeta {
    task_id: u64,
    chunk_no: u32,
    chunks_total: u32,
}

/// What travels in the headers of a finished chunk.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct DoneMeta {
    task_id: u64,
    chunk_no: u32,
}
// --8<-- [end:contracts]

// --8<-- [start:messages]
/// A chunk: publishing one does not compile without its `ChunkMeta`.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "chunks", headers = ChunkMeta)]
struct Chunk {
    size: u64,
}

/// A finished chunk: publishing one does not compile without its `DoneMeta`.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "chunks.done", headers = DoneMeta)]
struct ChunkDone {
    output_key: String,
}
// --8<-- [end:messages]

// --8<-- [start:dictionary]
/// The slot the converter publishes through, and what it may publish.
#[derive(OutSlot)]
#[publishes(ChunkDone)]
struct Events;
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
    /// The contract is parsed before the body runs; what the body publishes carries its own.
    #[subscriber("chunks")]
    async fn convert(
        chunk: &Chunk,
        Headers(meta): Headers<ChunkMeta>,
        Out(events): Out<impl Publisher, Events>,
    ) -> HandlerOutcome {
        tracing::info!(meta.task_id, meta.chunk_no, chunk.size, "converting");
        let done = ChunkDone {
            output_key: format!("chunks/{}/{}.part", meta.task_id, meta.chunk_no),
        };
        let done_meta = DoneMeta {
            task_id: meta.task_id,
            chunk_no: meta.chunk_no,
        };
        match events
            .message(&done)
            .with_headers(&done_meta)
            .publish()
            .await
        {
            Ok(()) => HandlerOutcome::ack(),
            Err(_) => HandlerOutcome::retry(),
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(convert).out(Events, Publish).build();
        })
    }
    // --8<-- [end:handler]

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

        // A chunk without its headers is bad input: the decode policy drops it.
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

    use ruststream::memory::prelude::*;
    use ruststream::testing::{Outcome, TestApp};

    use super::{Chunk, ChunkMeta, meta};

    // --8<-- [start:message]
    /// The payload and its contract arrive decoded together.
    #[subscriber("chunks")]
    async fn store(chunk: &Message<ChunkMeta, Chunk>) -> HandlerOutcome {
        let meta = &chunk.headers;
        if meta.chunk_no >= meta.chunks_total {
            // A chunk past the end of its task.
            return HandlerOutcome::drop();
        }
        tracing::info!(meta.task_id, meta.chunk_no, chunk.body.size, "stored");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("store", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(store);
        })
    }
    // --8<-- [end:message]

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
    // The derive in the prelude and the value a transform edits share the name `Outgoing`.
    use ruststream::runtime::{Outgoing, SlotContext};
    use ruststream::testing::TestApp;
    use serde::Serialize;

    use super::{Chunk, ChunkDone, ChunkMeta, DoneMeta, Events, meta};

    /// The converter of the first example; what this one adds is the transform.
    #[subscriber("chunks")]
    async fn convert(
        _chunk: &Chunk,
        Headers(meta): Headers<ChunkMeta>,
        Out(events): Out<impl Publisher, Events>,
    ) -> HandlerOutcome {
        let done = ChunkDone {
            output_key: format!("chunks/{}/{}.part", meta.task_id, meta.chunk_no),
        };
        let done_meta = DoneMeta {
            task_id: meta.task_id,
            chunk_no: meta.chunk_no,
        };
        match events
            .message(&done)
            .with_headers(&done_meta)
            .publish()
            .await
        {
            Ok(()) => HandlerOutcome::ack(),
            Err(_) => HandlerOutcome::retry(),
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
            b.include(convert)
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
