//! Typed handler adapter: turns `Fn(T) -> HandlerResult` into a [`Handler`] by decoding
//! the message payload via a [`Codec`].

use std::{fmt, future::Future, marker::PhantomData};

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

/// Build a handler that decodes the payload with `codec` and forwards the value to `inner`.
pub fn typed<M, T, C, H, Fut>(codec: C, inner: H) -> Typed<M, T, C, H>
where
    M: IncomingMessage,
    T: DeserializeOwned + Send,
    C: Codec,
    H: Fn(T) -> Fut + Send + Sync,
    Fut: Future<Output = HandlerResult> + Send,
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

impl<M, T, C, H, Fut> Handler<M> for Typed<M, T, C, H>
where
    M: IncomingMessage,
    T: DeserializeOwned + Send,
    C: Codec,
    H: Fn(T) -> Fut + Send + Sync,
    Fut: Future<Output = HandlerResult> + Send,
{
    async fn handle(&self, msg: &M) -> HandlerResult {
        match self.codec.decode::<T>(msg.payload()) {
            Ok(value) => (self.inner)(value).await,
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
