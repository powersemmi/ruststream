//! A service on the byte lanes: the handler reads the delivery's bytes as they arrived, and its
//! reply leaves as the bytes it returns, with no codec on either side. The scenario is a test.
//!
//! ```text
//! cargo test --example byte_lanes --no-default-features --features testing,macros,memory
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

/// Not JSON and not UTF-8: a codec anywhere on the path would reject it.
const FRAME: &[u8] = b"\x00\x01raw \xffbytes";

// --8<-- [start:lanes]
/// The delivery's bytes, borrowed from the broker's buffer.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// Bytes that leave as they are.
#[derive(Outgoing, Serialized)]
struct Export(Vec<u8>);

#[subscriber("frames", reply("exports"))]
async fn reverse(frame: &Frame<'_>) -> Export {
    tracing::info!(len = frame.0.len(), "reversed");
    let mut bytes = frame.0.to_vec();
    bytes.reverse();
    Export(bytes)
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("frames", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(reverse))
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
