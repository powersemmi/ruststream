//! The manual path with the `macros` feature off, imported from the prelude alone: the lane
//! impls the derives would have written, spelled exactly as the traits' own rustdoc shows them,
//! mounted as a service on the in-memory broker. The `macros` glob re-exports are gated, so
//! this build is the one where a lane spelling missing from the prelude has nowhere else to
//! come from; the file builds with no codec feature either, since neither lane needs one.
#![cfg(all(not(feature = "macros"), feature = "memory", feature = "testing"))]

use std::convert::Infallible;

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::testing::TestApp;

/// Deliberately not valid JSON (or UTF-8): a decode step anywhere on the path would fail it.
const FRAME: &[u8] = b"\x00\x01raw \xffbytes";

/// `#[derive(Deserialized)]` by hand, as the trait's rustdoc spells it: the construction that
/// borrows the delivery's bytes, and the input spelling that routes `&Frame<'_>` onto the
/// codec-free lane.
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

/// `#[derive(Serialized)]` by hand, as the trait's rustdoc spells it: the bytes, the wire
/// spelling for a typed publish, and the shape for the reply position.
struct Export(Vec<u8>);

impl Serialized for Export {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageWire for Export {
    type Wire = SerializedWire;
}

impl ReplyShape for Export {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

// `#[derive(Outgoing)]` by hand on the same type, so the test client injects it through the
// typed entry: no name declared, so each call site names its destination.
impl OutgoingDestination for Export {
    type Form = CallerName;
}

impl MessageHeaders for Export {
    type Contract = NoHeaders;
}

/// The reply form: bytes in, bytes out, both on the user's own lane.
struct Relay;

impl<'p> Handle<Frame<'p>, Export> for Relay {
    async fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<Export, HandlerOutcome> {
        let mut reply = frame.0.to_vec();
        reply.reverse();
        Ok(Export(reply))
    }
}

/// The far end of the relay, so the round trip is observable as a delivery.
struct Absorb;

impl<'p> Handle<Frame<'p>> for Absorb {
    async fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let _ = frame.0.len();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_prelude_carries_the_hand_written_lanes() {
    let app =
        RustStream::new(AppInfo::new("lanes", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(
                subscriber("frames", Relay)
                    .reply()
                    .to("exports")
                    .publisher(MemoryPublish)
                    .build(),
            );
            b.include(subscriber("exports", Absorb).build());
        });

    let tb = TestApp::start(app).await.expect("harness start");
    tb.broker::<MemoryBroker>()
        .message(&Export(FRAME.to_vec()))
        .to("frames")
        .publish()
        .await
        .expect("publish");

    let mut reversed = FRAME.to_vec();
    reversed.reverse();
    tb.broker::<MemoryBroker>()
        .subscriber("frames")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .subscriber("exports")
        .assert_called_once()
        .with_raw(&reversed)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("graceful shutdown");
}
