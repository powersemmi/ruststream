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
use super::handler::{Handler, HandlerResult};

/// Behaviour when [`Codec`] fails to decode a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum DecodeFailure {
    /// Drop the message: nack with `requeue = false`.
    #[default]
    Drop,
    /// Requeue the message: nack with `requeue = true`. Useful when the failure is transient
    /// (e.g. schema not yet propagated to consumers).
    Requeue,
}

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
        on_decode_failure: DecodeFailure::default(),
        _phantom: PhantomData,
    }
}

/// Handler produced by [`typed`]. Override decode-failure behaviour with
/// [`Typed::on_decode_failure`].
pub struct Typed<M, T, C, H> {
    codec: C,
    inner: H,
    on_decode_failure: DecodeFailure,
    _phantom: PhantomData<fn(M, T)>,
}

impl<M, T, C, H> Typed<M, T, C, H> {
    /// Override the behaviour when the codec fails to decode an incoming payload.
    #[must_use]
    pub fn on_decode_failure(mut self, mode: DecodeFailure) -> Self {
        self.on_decode_failure = mode;
        self
    }
}

impl<M, T, C, H> fmt::Debug for Typed<M, T, C, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Typed")
            .field("on_decode_failure", &self.on_decode_failure)
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
                    error = %err,
                    "codec decode failed",
                );
                match self.on_decode_failure {
                    DecodeFailure::Drop => HandlerResult::drop(),
                    DecodeFailure::Requeue => HandlerResult::retry(),
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

    use super::{DecodeFailure, typed};
    use crate::codec::JsonCodec;
    use crate::runtime::context::{Context, State};
    use crate::runtime::dispatch::Delivery;
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
            typed(JsonCodec, counting_inner(&seen)).on_decode_failure(DecodeFailure::Requeue);
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("typed", &headers, &state, &delivery);

        let msg = StubMsg(b"not json".to_vec(), Headers::new());
        assert_eq!(handler.handle(&msg, &mut ctx).await, HandlerResult::retry());
        assert_eq!(seen.load(Ordering::SeqCst), 0, "inner must not run");
    }
}
