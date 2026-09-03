//! The macro-free counterpart of `tests/raw_subscriber.rs`: the raw handler forms written out as
//! named types, including the lane traits the derives would have written. The plain form is a
//! body over a `Deserialized` payload view; the byte-reply form declares a `Serialized` type as
//! its reply and wires it with `.reply().to(..).publisher(..)` - the wire is read off the two
//! message types either way, on this path exactly as on the attribute's.
//!
//! The codec-free path is what the plain and the byte-reply sections pin: bytes on the input
//! side mean no `Codec` bound reaches the mount, so this file also builds with every codec
//! feature off (the typed-input module below is the one exception, and it is gated).
#![cfg(all(feature = "memory", feature = "testing"))]

use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Mutex;

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::runtime::{Input, MessageWire, SerializedReply, SerializedWire, SoloDeserialized};
use ruststream::testing::TestApp;
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination};

/// Deliberately not valid JSON (or UTF-8): a decode step anywhere on the path would fail it.
const FRAME: &[u8] = b"\x00\x01raw \xffbytes";

/// The wire this suite injects its frames through, with the impls `#[derive(Serialized)]` and
/// `#[derive(Outgoing)]` write: publishing is typed, so bytes that are not a model still travel
/// as a declared type, and the serialized wire is what keeps every codec off them. It declares
/// no name, so each call site names its own destination.
struct Wire(Vec<u8>);

impl Serialized for Wire {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageWire for Wire {
    type Wire = SerializedWire;
}

impl OutgoingDestination for Wire {
    type Form = CallerName;
}

impl MessageHeaders for Wire {
    type Contract = NoHeaders;
}

impl Wire {
    /// The wire form of `bytes`, for the call sites that hold a slice or a literal.
    fn of(bytes: impl AsRef<[u8]>) -> Self {
        Self(bytes.as_ref().to_vec())
    }
}

// --- the plain form: the handler sees the exact published bytes ---

static FRAMES: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

// --8<-- [start:raw]
/// The payload view the raw bodies below take, with the pair of impls `#[derive(Deserialized)]`
/// writes: the construction that borrows the delivery's bytes, and the input spelling that
/// routes `&Frame<'_>` onto the codec-free lane.
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

/// The raw form by hand. `Handle<Frame<'_>>` is what the attribute implements for a
/// `&Frame<'_>` parameter: the input type itself tells the mount to skip the codec, and the
/// adapter lends the delivery's payload rather than decoding it.
struct OnFrame;

impl<'p> Handle<Frame<'p>> for OnFrame {
    fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        FRAMES.lock().expect("frame log").push(frame.0.to_vec());
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
        .message(&Wire::of(FRAME))
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
/// The reply type the byte-reply form returns, with the impls `#[derive(Serialized)]` writes
/// for the reply position: the bytes that leave, and the shape that routes them onto the
/// serialized wire.
struct Export(Vec<u8>);

impl Serialized for Export {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl ReplyShape for Export {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

/// The byte-reply form by hand: a body over `Frame<'_>` declaring `Export` as its reply, so
/// bytes in and bytes out. The reply type is what picks the codec-free commit, so the reply
/// leaves as it was returned, and the body's `Err` arm is the reply the attribute lets a
/// handler skip.
struct Relay;

impl<'p> Handle<Frame<'p>, Export> for Relay {
    fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Export, HandlerOutcome>> {
        let mut reply = frame.0.to_vec();
        reply.reverse();
        ready(Ok(Export(reply)))
    }
}
// --8<-- [end:raw_reply]

/// The far end of the relay, so the round trip is observable as a delivery, not just a publish.
struct RelayCapture;

impl<'p> Handle<Frame<'p>> for RelayCapture {
    fn handle(
        &self,
        _frame: &Frame<'p>,
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
                .publisher(MemoryPublish)
                .build(),
        );
        b.include(subscriber("relay-out", RelayCapture).build());
    });

    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(FRAME))
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

// --- a Serialized reply with a TYPED input: decode with the scope codec, reply bytes as-is ---

#[cfg(feature = "json")]
mod typed_in {
    use std::future::{Future, ready};

    use ruststream::memory::{MemoryBroker, MemoryPublish};
    use ruststream::prelude::*;
    use ruststream::testing::TestApp;
    use serde::Deserialize;

    use super::{Export, Frame};

    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct Wrap {
        id: u32,
    }

    // --8<-- [start:raw_reply_typed]
    /// The gateway shape: a structured message in, a self-produced wire format out. Only the
    /// body's input parameter changes from the byte-reply form above - `&Wrap` instead of
    /// `&Frame<'_>`, which is what selects the decode - so the decode codec is resolved from the
    /// mount while the reply still leaves unencoded.
    struct Gateway;

    impl Handle<Wrap, Export> for Gateway {
        fn handle(
            &self,
            wrap: &Wrap,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Export, HandlerOutcome>> {
            ready(Ok(Export(wrap.id.to_be_bytes().to_vec())))
        }
    }
    // --8<-- [end:raw_reply_typed]

    /// Asserts inside the delivery that the reply bytes arrived untouched.
    struct GatewayCapture;

    impl<'p> Handle<Frame<'p>> for GatewayCapture {
        fn handle(
            &self,
            frame: &Frame<'p>,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            // The adapter builds this future inside the dispatcher's unwind guard, so a failed
            // assertion is caught like any other handler panic.
            assert_eq!(frame.0, 7_u32.to_be_bytes(), "the reply bytes arrive as-is");
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
                        .publisher(MemoryPublish)
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
