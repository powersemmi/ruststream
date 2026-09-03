//! The macro-free counterpart of `tests/raw_subscriber.rs`: the raw handler forms written out as
//! named types. The plain form is a body over `Payload<'_>`; the byte-reply form declares
//! `Vec<u8>` as its reply and wires it with `.reply().to(..).publisher(Bare(..))`, which is what
//! the `publish_raw(..)` clause would have emitted - the input kind is read off the body's own
//! parameter either way.
//!
//! The codec-free path is what the plain and the byte-reply sections pin: raw bytes on the input
//! side mean no `Codec` bound reaches the mount, so this file also builds with every codec
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
/// The raw form by hand. `Handle<Payload<'_>>` is what the attribute implements for a `&[u8]`
/// parameter: the input spelling itself tells the mount to skip the codec, and the adapter lends
/// the delivery's payload rather than decoding it.
struct OnFrame;

impl<'p> Handle<Payload<'p>> for OnFrame {
    fn handle(
        &self,
        frame: &Payload<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        FRAMES.lock().expect("frame log").push(frame.to_vec());
        ready(Ok(()))
    }
}
// --8<-- [end:raw]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_handler_receives_exact_bytes() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("frames", OnFrame).build());
    });

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
        .settled(HandlerOutcome::ack());
    assert_eq!(
        FRAMES.lock().expect("frame log").as_slice(),
        &[FRAME.to_vec()],
        "the handler saw the published bytes untouched"
    );
}

// --- the reply form: the returned bytes are republished as-is ---

// --8<-- [start:raw_reply]
/// The byte-reply form by hand: a body over `Payload<'_>` declaring `Vec<u8>` as its reply, so
/// bytes in and bytes out. The publisher step's `Bare(..)` form is what picks the bare-publisher
/// commit, so the reply leaves without a codec, and the body's `Err` arm is the reply the
/// attribute lets a handler skip.
struct Relay;

impl<'p> Handle<Payload<'p>, Vec<u8>> for Relay {
    fn handle(
        &self,
        frame: &Payload<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<u8>, HandlerOutcome>> {
        let mut reply = frame.to_vec();
        reply.reverse();
        ready(Ok(reply))
    }
}
// --8<-- [end:raw_reply]

/// The far end of the relay, so the round trip is observable as a delivery, not just a publish.
struct RelayCapture;

impl<'p> Handle<Payload<'p>> for RelayCapture {
    fn handle(
        &self,
        _frame: &Payload<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_reply_round_trips_exact_bytes() {
    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(
            subscriber("relay-in", Relay)
                .reply()
                .to("relay-out")
                .publisher(Bare(MemoryPublish))
                .build(),
        );
        b.include(subscriber("relay-out", RelayCapture).build());
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
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .subscriber("relay-out")
        .assert_called_once()
        .with_raw(&expected)
        .settled(HandlerOutcome::ack());
}

// --- publish_raw with a TYPED input: decode with the scope codec, reply bytes as-is ---

#[cfg(feature = "json")]
mod typed_in {
    use std::future::{Future, ready};

    use ruststream::memory::{MemoryBroker, MemoryPublish};
    use ruststream::prelude::*;
    use ruststream::testing::TestApp;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct Wrap {
        id: u32,
    }

    // --8<-- [start:raw_reply_typed]
    /// The gateway shape: a structured message in, a self-produced wire format out. Only the
    /// body's input parameter changes from the byte-reply form above - `&Wrap` instead of
    /// `&Payload<'_>`, which is what selects the decode - so the decode codec is resolved from the
    /// mount while the reply still leaves unencoded.
    struct Gateway;

    impl Handle<Wrap, Vec<u8>> for Gateway {
        fn handle(
            &self,
            wrap: &Wrap,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Vec<u8>, HandlerOutcome>> {
            ready(Ok(wrap.id.to_be_bytes().to_vec()))
        }
    }
    // --8<-- [end:raw_reply_typed]

    /// Asserts inside the delivery that the reply bytes arrived untouched.
    struct GatewayCapture;

    impl<'p> Handle<Payload<'p>> for GatewayCapture {
        fn handle(
            &self,
            frame: &Payload<'p>,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            // The adapter builds this future inside the dispatcher's unwind guard, so a failed
            // assertion is caught like any other handler panic.
            assert_eq!(
                &frame[..],
                7_u32.to_be_bytes(),
                "the reply bytes arrive as-is"
            );
            ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn typed_input_replies_raw_bytes() {
        let app = RustStream::new(AppInfo::new("gateway", "0.1.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(
                    subscriber("gateway-in", Gateway)
                        .reply()
                        .to("gateway-out")
                        .publisher(Bare(MemoryPublish))
                        .build(),
                );
                b.include(subscriber("gateway-out", GatewayCapture).build());
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
            .settled(HandlerOutcome::ack());
        tb.broker::<MemoryBroker>()
            .subscriber("gateway-out")
            .assert_called_once()
            .with_raw(7_u32.to_be_bytes().as_slice())
            .settled(HandlerOutcome::ack());
    }
}
