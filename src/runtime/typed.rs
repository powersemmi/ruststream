//! Typed handler adapter: turns a [`Handler<T>`](Handler) over a decoded value into a
//! [`Handler<M>`](Handler) by decoding the message payload via a [`Codec`].
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
use super::handler::{Handler, HandlerResult};

/// Build a `Handler<M>` that decodes the payload with `codec` into `T` and forwards `&T` to
/// `inner`.
///
/// `inner` is any [`Handler<T>`](Handler) - a closure `Fn(&T) -> _` or a typed middleware stack
/// built with [`HandlerExt::with`](super::HandlerExt::with).
pub fn typed<M, T, C, H>(codec: C, inner: H) -> Typed<M, T, C, H>
where
    M: IncomingMessage,
    T: DeserializeOwned + Send + Sync,
    C: Codec,
    H: Handler<T>,
{
    Typed {
        codec,
        inner,
        decode: FailurePolicy::Drop,
        _phantom: PhantomData,
    }
}

/// Handler produced by [`typed`]. Override the decode-failure policy with
/// [`Typed::on_decode_failure`].
pub struct Typed<M, T, C, H> {
    codec: C,
    inner: H,
    decode: FailurePolicy,
    _phantom: PhantomData<fn(M, T)>,
}

impl<M, T, C, H> Typed<M, T, C, H> {
    /// Sets the [`FailurePolicy`] applied when the codec fails to decode an incoming payload. The
    /// default is [`FailurePolicy::Drop`].
    #[must_use]
    pub fn on_decode_failure(mut self, decode: FailurePolicy) -> Self {
        self.decode = decode;
        self
    }
}

impl<M, T, C, H> fmt::Debug for Typed<M, T, C, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Typed")
            .field("decode", &self.decode)
            .finish_non_exhaustive()
    }
}

impl<M, T, C, H> Handler<M> for Typed<M, T, C, H>
where
    M: IncomingMessage,
    T: DeserializeOwned + Send + Sync,
    C: Codec,
    H: Handler<T>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> HandlerResult {
        match self.codec.decode::<T>(msg.payload()) {
            Ok(value) => self.inner.handle(&value, ctx).await,
            Err(err) => {
                warn!(
                    target: "ruststream::dispatch",
                    subscription = %ctx.name(),
                    message_type = std::any::type_name::<T>(),
                    error = %err,
                    "codec decode failed",
                );
                match self.decode {
                    FailurePolicy::FailFast => {
                        ctx.fail_fast(&format!("decode failed: {err}"));
                        HandlerResult::drop()
                    }
                    other => other.settlement().unwrap_or_else(HandlerResult::drop),
                }
            }
        }
    }
}

#[cfg(all(test, feature = "json"))]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use super::typed;
    use crate::codec::JsonCodec;
    use crate::runtime::context::{Context, State};
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

        async fn ack(self) -> Result<(), AckError> {
            Ok(())
        }

        async fn nack(self, _requeue: bool) -> Result<(), AckError> {
            Ok(())
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
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, &delivery);

        let msg = StubMsg(b"7".to_vec(), Headers::new());
        assert_eq!(handler.handle(&msg, &mut ctx).await, HandlerResult::Ack);
        assert_eq!(seen.load(Ordering::SeqCst), 7);
    }

    #[tokio::test]
    async fn decode_failure_drops_by_default() {
        let seen = Arc::new(AtomicU32::new(0));
        let handler = typed(JsonCodec, counting_inner(&seen));
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, &delivery);

        let msg = StubMsg(b"not json".to_vec(), Headers::new());
        assert_eq!(handler.handle(&msg, &mut ctx).await, HandlerResult::drop());
        assert_eq!(seen.load(Ordering::SeqCst), 0, "inner must not run");
    }

    #[tokio::test]
    async fn decode_failure_requeues_when_overridden() {
        let seen = Arc::new(AtomicU32::new(0));
        let handler =
            typed(JsonCodec, counting_inner(&seen)).on_decode_failure(FailurePolicy::Retry);
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, &delivery);

        let msg = StubMsg(b"not json".to_vec(), Headers::new());
        assert_eq!(handler.handle(&msg, &mut ctx).await, HandlerResult::retry());
        assert_eq!(seen.load(Ordering::SeqCst), 0, "inner must not run");
    }

    #[tokio::test]
    async fn typed_handler_is_debug_and_stub_acks() {
        let seen = Arc::new(AtomicU32::new(0));
        let handler = typed(JsonCodec, counting_inner(&seen));
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, &delivery);
        // Drive one delivery to pin the message type, then check the Debug rendering.
        let msg = StubMsg(b"5".to_vec(), Headers::new());
        handler.handle(&msg, &mut ctx).await;
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

    // Captures the fields of the one event emitted on a decode failure, so the test can assert the
    // diagnostic carries the subscription name and target type (needs a tracing subscriber, hence
    // the `logging` feature gate).
    #[cfg(feature = "logging")]
    #[tokio::test]
    async fn decode_failure_log_names_subscription_and_type() {
        use std::collections::HashMap;
        use std::sync::Mutex;

        use tracing::field::{Field, Visit};
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};

        #[derive(Default)]
        struct FieldGrab(HashMap<String, String>);

        impl Visit for FieldGrab {
            fn record_str(&mut self, field: &Field, value: &str) {
                self.0.insert(field.name().to_owned(), value.to_owned());
            }

            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                self.0
                    .entry(field.name().to_owned())
                    .or_insert_with(|| format!("{value:?}"));
            }
        }

        struct Capture(Arc<Mutex<Vec<HashMap<String, String>>>>);

        impl<S: tracing::Subscriber> Layer<S> for Capture {
            fn on_event(&self, event: &tracing::Event<'_>, _ctx: LayerContext<'_, S>) {
                let mut grab = FieldGrab::default();
                event.record(&mut grab);
                self.0.lock().unwrap().push(grab.0);
            }
        }

        let events = Arc::new(Mutex::new(Vec::new()));
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&events))),
        );

        let seen = Arc::new(AtomicU32::new(0));
        let handler = typed(JsonCodec, counting_inner(&seen));
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("orders.inbound", &headers, &state, &delivery);
        let msg = StubMsg(b"not json".to_vec(), Headers::new());
        assert_eq!(handler.handle(&msg, &mut ctx).await, HandlerResult::drop());
        drop(guard);

        let decode_event = {
            let captured = events.lock().unwrap();
            captured
                .iter()
                .find(|f| f.get("message").is_some_and(|m| m == "codec decode failed"))
                .cloned()
                .expect("a codec-decode-failed event must be emitted")
        };
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
