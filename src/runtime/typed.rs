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
/// `inner` is any [`Handler<T>`](Handler) — a closure `Fn(&T) -> _` or a typed middleware stack
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
    async fn handle(&self, msg: &M) -> HandlerResult {
        match self.codec.decode::<T>(msg.payload()) {
            Ok(value) => self.inner.handle(&value).await,
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
