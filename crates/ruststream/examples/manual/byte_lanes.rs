//! The service of `examples/byte_lanes.rs` written without the `macros` feature: the lane impls
//! the derives write, spelled out.
//!
//! ```text
//! cargo test --example manual_byte_lanes --no-default-features --features testing,memory
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::convert::Infallible;
use std::error::Error;
use std::future::ready;

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

/// Not JSON and not UTF-8: a codec anywhere on the path would reject it.
const FRAME: &[u8] = b"\x00\x01raw \xffbytes";

// --8<-- [start:lanes]
/// The delivery's bytes, borrowed from the broker's buffer.
struct Frame<'a>(&'a [u8]);

// `#[derive(Deserialized)]` by hand: the view borrows the payload, and `&Frame<'_>` takes the
// lane that reads no codec.
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

/// Bytes that leave as they are.
struct Export(Vec<u8>);

// `#[derive(Serialized)]` by hand: the bytes, the wire of a typed publish, the shape of a reply.
impl Serialized for Export {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
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

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Export {
    type Form = CallerName;
}

impl MessageHeaders for Export {
    type Contract = NoHeaders;
}

struct Reverse;

impl<'p> Handle<Frame<'p>, Export> for Reverse {
    fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Export, HandlerOutcome>> {
        tracing::info!(len = frame.0.len(), "reversed");
        let mut bytes = frame.0.to_vec();
        bytes.reverse();
        ready(Ok(Export(bytes)))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("frames", Reverse).reply().to("exports").build())
    })
}
// --8<-- [end:lanes]

#[tokio::test]
async fn the_bytes_pass_untouched() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let broker = tb.broker::<MemoryBroker>();

    broker
        .message(&Export(FRAME.to_vec()))
        .to("frames")
        .publish()
        .await?;

    let mut reversed = FRAME.to_vec();
    reversed.reverse();
    broker
        .subscriber("frames")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerOutcome::ack());
    broker
        .published::<Export>("exports")
        .assert_called_once()
        .with_raw(&reversed);
    Ok(())
}

// The scenario is the test above; the command in the header runs it.
fn main() {}
