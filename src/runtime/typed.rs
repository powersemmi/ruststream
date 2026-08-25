//! Typed handler adapter: turns a handler over an input kind's borrowed target into a
//! [`Handler<M>`](Handler) by materializing the input from each delivery via the input axis
//! ([`InputKind`]).
//!
//! This is the decode boundary between the two middleware levels: raw (pre-decode) middleware
//! wrap the produced `Handler<M>`; typed (post-decode) middleware wrap the `inner: Handler<T>`
//! passed in here. Both use the same [`Layer`](super::Layer) / [`HandlerExt`](super::HandlerExt)
//! machinery, just at different inputs.

use std::{fmt, marker::PhantomData};

use crate::IncomingMessage;
use crate::codec::Codec;
use serde::de::DeserializeOwned;
use tracing::warn;

use super::context::Context;
use super::failure::FailurePolicy;
use super::handler::{Handler, HandlerResult, Settle};
use super::input::{DecodeWith, Decoded};

/// Build a `Handler<M>` that decodes the payload with `codec` into `T` and forwards `&T` to
/// `inner`.
///
/// `inner` is any [`Handler<T>`](Handler) - a closure `Fn(&T) -> _` or a typed middleware stack
/// built with [`HandlerExt::with`](super::HandlerExt::with). The general form over any input
/// kind (a raw `&[u8]` handler included) is [`Typed::over`].
pub fn typed<M, T, C, H>(codec: C, inner: H) -> Typed<M, Decoded<T>, C, H>
where
    M: IncomingMessage,
    T: DeserializeOwned + Send + Sync + 'static,
    C: Codec,
{
    Typed::over(codec, inner)
}

/// Handler produced by [`typed`] (or [`Typed::over`] for a non-decoding input kind). Override
/// the decode-failure policy with [`Typed::on_decode_failure`].
pub struct Typed<M, Input, DecodeCodec, Inner> {
    codec: DecodeCodec,
    inner: Inner,
    decode: FailurePolicy,
    _phantom: PhantomData<fn(M, Input)>,
}

impl<M, Input, DecodeCodec, Inner> Typed<M, Input, DecodeCodec, Inner> {
    /// Builds the adapter for any input kind: [`Decoded<T>`] decodes with `codec`,
    /// [`RawBytes`](super::RawBytes) ignores it (pass `()`) and lends the payload itself.
    #[must_use]
    pub fn over(codec: DecodeCodec, inner: Inner) -> Self
    where
        Input: DecodeWith<DecodeCodec>,
    {
        Self {
            codec,
            inner,
            decode: FailurePolicy::Drop,
            _phantom: PhantomData,
        }
    }

    /// Sets the [`FailurePolicy`] applied when the codec fails to decode an incoming payload. The
    /// default is [`FailurePolicy::Drop`].
    #[must_use]
    pub fn on_decode_failure(mut self, decode: FailurePolicy) -> Self {
        self.decode = decode;
        self
    }
}

impl<M, Input, DecodeCodec, Inner> fmt::Debug for Typed<M, Input, DecodeCodec, Inner> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Typed")
            .field("decode", &self.decode)
            .finish_non_exhaustive()
    }
}

impl<M, Input, DecodeCodec, Inner, Cx, St> Handler<M, Cx, St>
    for Typed<M, Input, DecodeCodec, Inner>
where
    M: IncomingMessage,
    Input: DecodeWith<DecodeCodec>,
    DecodeCodec: Send + Sync,
    Cx: Send,
    St: Send + Sync,
    Inner: Handler<Input::Target, Cx, St>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, Cx, St>) -> Settle {
        // The decode product lives on this stack frame and the handler borrows its view, so the
        // input path allocates nothing of its own (a raw input borrows the payload straight out
        // of the broker's buffer).
        match Input::decode(&self.codec, msg.payload()) {
            Ok(owned) => {
                self.inner
                    .handle(Input::view(&owned, msg.payload()), ctx)
                    .await
            }
            Err(err) => {
                warn!(
                    target: "ruststream::dispatch",
                    subscription = %ctx.name(),
                    message_type = Input::input_label(),
                    error = %err,
                    "codec decode failed",
                );
                #[cfg(any(feature = "testing", feature = "otel"))]
                ctx.mark_decode_failed();
                match self.decode {
                    FailurePolicy::FailFast => {
                        ctx.fail_fast(&format!("decode failed: {err}"));
                        HandlerResult::drop()
                    }
                    other => other.settlement().unwrap_or_else(HandlerResult::drop),
                }
                .into()
            }
        }
    }
}

#[cfg(all(test, feature = "json"))]
mod tests {
    use std::future::ready;
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use super::typed;
    use crate::codec::JsonCodec;
    use crate::runtime::context::Context;
    use crate::runtime::dispatch::Delivery;
    use crate::runtime::failure::FailurePolicy;
    use crate::runtime::handler::{Handler, HandlerResult};
    use crate::{AckError, Headers, IncomingMessage};

    struct StubMsg(Vec<u8>, Headers);

    impl IncomingMessage for StubMsg {
        fn payload(&self) -> &[u8] {
            &self.0
        }

        fn headers(&self) -> &Headers {
            &self.1
        }

        fn ack(self) -> impl Future<Output = Result<(), AckError>> {
            ready(Ok(()))
        }

        fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
            ready(Ok(()))
        }
    }

    fn counting_inner(seen: &Arc<AtomicU32>) -> impl Handler<u32> {
        let seen = Arc::clone(seen);
        move |value: &u32, _ctx: &mut Context| {
            let seen = Arc::clone(&seen);
            let value = *value;
            async move {
                seen.store(value, Ordering::SeqCst);
                HandlerResult::Ack
            }
        }
    }

    // Plain #[tokio::test]: nothing is spawned, the handler future is awaited inline.
    #[tokio::test]
    async fn decoded_value_reaches_inner() {
        let seen = Arc::new(AtomicU32::new(0));
        let handler = typed(JsonCodec, counting_inner(&seen));
        let state = ();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, (), &delivery);

        let msg = StubMsg(b"7".to_vec(), Headers::new());
        assert_eq!(
            handler.handle(&msg, &mut ctx).await.outcome(),
            HandlerResult::Ack
        );
        assert_eq!(seen.load(Ordering::SeqCst), 7);
    }

    #[tokio::test]
    async fn raw_bytes_lend_the_payload_itself() {
        use super::Typed;
        use crate::runtime::input::RawBytes;

        let seen = Arc::new(AtomicU32::new(0));
        let inner = {
            let seen = Arc::clone(&seen);
            move |bytes: &[u8], _ctx: &mut Context| {
                let seen = Arc::clone(&seen);
                let len = u32::try_from(bytes.len()).unwrap();
                async move {
                    seen.store(len, Ordering::SeqCst);
                    HandlerResult::Ack
                }
            }
        };
        // No codec anywhere: the raw kind decodes with `()`.
        let handler = Typed::<StubMsg, RawBytes, (), _>::over((), inner);
        let state = ();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("frames", &headers, &state, (), &delivery);

        let msg = StubMsg(b"not json at all".to_vec(), Headers::new());
        assert_eq!(
            handler.handle(&msg, &mut ctx).await.outcome(),
            HandlerResult::Ack
        );
        assert_eq!(seen.load(Ordering::SeqCst), 15);
    }

    #[tokio::test]
    async fn decode_failure_drops_by_default() {
        let seen = Arc::new(AtomicU32::new(0));
        let handler = typed(JsonCodec, counting_inner(&seen));
        let state = ();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, (), &delivery);

        let msg = StubMsg(b"not json".to_vec(), Headers::new());
        assert_eq!(
            handler.handle(&msg, &mut ctx).await.outcome(),
            HandlerResult::drop()
        );
        assert_eq!(seen.load(Ordering::SeqCst), 0, "inner must not run");
    }

    #[tokio::test]
    async fn decode_failure_requeues_when_overridden() {
        let seen = Arc::new(AtomicU32::new(0));
        let handler =
            typed(JsonCodec, counting_inner(&seen)).on_decode_failure(FailurePolicy::Retry);
        let state = ();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, (), &delivery);

        let msg = StubMsg(b"not json".to_vec(), Headers::new());
        assert_eq!(
            handler.handle(&msg, &mut ctx).await.outcome(),
            HandlerResult::retry()
        );
        assert_eq!(seen.load(Ordering::SeqCst), 0, "inner must not run");
    }

    #[tokio::test]
    async fn typed_handler_is_debug_and_stub_acks() {
        let seen = Arc::new(AtomicU32::new(0));
        let handler = typed(JsonCodec, counting_inner(&seen));
        let state = ();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, (), &delivery);
        // Drive one delivery to pin the message type, then check the Debug rendering.
        let msg = StubMsg(b"5".to_vec(), Headers::new());
        let _ = handler.handle(&msg, &mut ctx).await;
        assert!(format!("{handler:?}").contains("Typed"));

        // Exercise the StubMsg fixture's own IncomingMessage surface.
        let other = StubMsg(b"x".to_vec(), Headers::new());
        assert!(other.headers().is_empty());
        other.ack().await.unwrap();
        StubMsg(Vec::new(), Headers::new())
            .nack(true)
            .await
            .unwrap();
    }

    // The decode diagnostic carries the subscription name and the target type (needs a tracing
    // subscriber, hence the `logging` feature gate).
    #[cfg(feature = "logging")]
    #[tokio::test]
    async fn decode_failure_log_names_subscription_and_type() {
        use crate::testkit::log_capture;

        let (events, guard) = log_capture::start();

        let seen = Arc::new(AtomicU32::new(0));
        let handler = typed(JsonCodec, counting_inner(&seen));
        let state = ();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("orders.inbound", &headers, &state, (), &delivery);
        let msg = StubMsg(b"not json".to_vec(), Headers::new());
        assert_eq!(
            handler.handle(&msg, &mut ctx).await.outcome(),
            HandlerResult::drop()
        );
        drop(guard);

        let decode_event = log_capture::find(&events, "codec decode failed");
        assert_eq!(
            decode_event.get("subscription").map(String::as_str),
            Some("orders.inbound")
        );
        assert_eq!(
            decode_event.get("message_type").map(String::as_str),
            Some("u32")
        );
    }
}
