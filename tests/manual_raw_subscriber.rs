//! The macro-free counterpart of `tests/raw_subscriber.rs`: the raw handler forms written out as
//! named types. The plain form binds through the `raw` constructor; the byte-reply form through
//! `raw_replying(..).to(..)`, over the `Reply` body the `publish_raw(..)` clause would have
//! emitted - byte-shaped `Out`, and the input kind read off the body's own parameter.
//!
//! The codec-free path is what the plain and the byte-reply sections pin: `RawBytes` on the input
//! side means no `Codec` bound reaches the mount, so this file also builds with every codec
//! feature off (the typed-input module below is the one exception, and it is gated).
#![cfg(all(feature = "memory", feature = "testing"))]

use std::future::{Future, ready};
use std::sync::Mutex;

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::testing::TestApp;

/// Deliberately not valid JSON (or UTF-8): a decode step anywhere on the path would fail it.
const FRAME: &[u8] = b"\x00\x01raw \xffbytes";

// --- the plain form: the handler sees the exact published bytes ---

static FRAMES: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

// --8<-- [start:raw]
/// The raw form by hand. `Handler<[u8]>` is what the attribute implements for a `&[u8]`
/// parameter: the mount adapter lends the delivery's payload itself, so nothing decodes. The
/// `raw` constructor is what binds it to its subscription and tells the mount to skip the codec.
struct OnFrame;

impl Handler<[u8]> for OnFrame {
    // The body awaits nothing, so it returns the future rather than being an `async fn`.
    fn handle(&self, frame: &[u8], _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        FRAMES.lock().expect("frame log").push(frame.to_vec());
        ready(HandlerResult::Ack.into())
    }
}
// --8<-- [end:raw]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_handler_receives_exact_bytes() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(raw("frames", OnFrame)));

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .raw(FRAME)
        .to("frames")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("frames")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerResult::Ack);
    assert_eq!(
        FRAMES.lock().expect("frame log").as_slice(),
        &[FRAME.to_vec()],
        "the handler saw the published bytes untouched"
    );
}

// --- the reply form: the returned bytes are republished as-is ---

// --8<-- [start:raw_reply]
/// The byte-reply form by hand: a `Reply<[u8]>` body with `Vec<u8>` out, so bytes in and bytes
/// out. `raw_replying` is what picks the bare-publisher commit, so the reply leaves without a
/// codec, and the body's `Err` arm is the reply the attribute lets a handler skip.
struct Relay;

impl Reply<[u8]> for Relay {
    type Out = Vec<u8>;

    fn reply(
        &self,
        frame: &[u8],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<u8>, HandlerResult>> + Send {
        let mut reply = frame.to_vec();
        reply.reverse();
        ready(Ok(reply))
    }
}
// --8<-- [end:raw_reply]

/// The far end of the relay, so the round trip is observable as a delivery, not just a publish.
struct RelayCapture;

impl Handler<[u8]> for RelayCapture {
    fn handle(&self, _frame: &[u8], _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        ready(HandlerResult::Ack.into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_round_trips_exact_bytes() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(raw_replying("relay-in", Relay).to("relay-out"))
            .publisher(MemoryPublish);
        b.include(raw("relay-out", RelayCapture));
    });

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .raw(FRAME)
        .to("relay-in")
        .publish()
        .await
        .expect("publish");

    let mut expected = FRAME.to_vec();
    expected.reverse();
    tb.broker::<MemoryBroker>()
        .subscriber("relay-in")
        .assert_called_once()
        .with_raw(FRAME)
        .settled(HandlerResult::Ack);
    tb.broker::<MemoryBroker>()
        .subscriber("relay-out")
        .assert_called_once()
        .with_raw(&expected)
        .settled(HandlerResult::Ack);
}

// --- publish_raw with a TYPED input: decode with the scope codec, reply bytes as-is ---

#[cfg(feature = "json")]
mod typed_in {
    use std::future::{Future, ready};

    use ruststream::memory::{MemoryBroker, MemoryPublish};
    use ruststream::prelude::*;
    use ruststream::testing::TestApp;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Wrap {
        id: u32,
    }

    // --8<-- [start:raw_reply_typed]
    /// The gateway shape: a structured message in, a self-produced wire format out. Only the
    /// body's input parameter changes from the byte-reply form above - `&Wrap` instead of
    /// `&[u8]`, which is what selects the decode - so the decode codec is resolved from the mount
    /// while the reply still leaves unencoded.
    struct Gateway;

    impl Reply<Wrap> for Gateway {
        type Out = Vec<u8>;

        fn reply(
            &self,
            wrap: &Wrap,
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Vec<u8>, HandlerResult>> + Send {
            ready(Ok(wrap.id.to_be_bytes().to_vec()))
        }
    }
    // --8<-- [end:raw_reply_typed]

    /// Asserts inside the delivery that the reply bytes arrived untouched.
    struct GatewayCapture;

    impl Handler<[u8]> for GatewayCapture {
        fn handle(
            &self,
            frame: &[u8],
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Settle> + Send {
            // The assertion lives inside the future: the dispatcher's unwind guard wraps the
            // future it is handed, not the call that builds it.
            let frame = frame.to_vec();
            async move {
                assert_eq!(frame, 7_u32.to_be_bytes(), "the reply bytes arrive as-is");
                HandlerResult::Ack.into()
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn typed_input_replies_raw_bytes() {
        let app = RustStream::new(AppInfo::new("gateway", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(raw_replying("gateway-in", Gateway).to("gateway-out"))
                    .publisher(MemoryPublish);
                b.include(raw("gateway-out", GatewayCapture));
            },
        );

        let tb = TestApp::start(app).await.expect("start");
        tb.broker::<MemoryBroker>()
            .publish("gateway-in", &serde_json::json!({"id": 7}))
            .await
            .expect("publish");

        tb.broker::<MemoryBroker>()
            .subscriber("gateway-in")
            .assert_called_once()
            .settled(HandlerResult::Ack);
        tb.broker::<MemoryBroker>()
            .subscriber("gateway-out")
            .assert_called_once()
            .with_raw(7_u32.to_be_bytes().as_slice())
            .settled(HandlerResult::Ack);
    }
}
