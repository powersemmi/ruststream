//! Reply wirings: how a handler's return value reaches a publisher.

use core::error::Error as StdError;
use core::fmt;

use serde::Serialize;
use tracing::warn;

use super::{
    BatchPublishTransform, PublishContext, PublishPipeline, Transactional, TypedPublisher,
};
use crate::codec::Codec;
use crate::lifecycle::BoxError;
use crate::publish::sealed::Sealed;
use crate::{PayloadForm, Publisher, TransactionalPublisher};

/// Why a batch of replies did not leave whole, and how many of them had left before it failed.
///
/// Machinery of the batch dispatch; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct PublishBatchError {
    sent: usize,
    error: BoxError,
}

impl PublishBatchError {
    fn new(sent: usize, error: BoxError) -> Self {
        Self { sent, error }
    }

    /// How many replies, from the front of the batch, were published before the failure.
    #[must_use]
    pub const fn sent(&self) -> usize {
        self.sent
    }

    /// The error of the publish that failed.
    #[must_use]
    pub fn into_error(self) -> BoxError {
        self.error
    }
}

impl fmt::Display for PublishBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl StdError for PublishBatchError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.error)
    }
}

/// The live reply sink a batch's replies travel through.
///
/// Implemented by a plain [`TypedPublisher`] (each reply published independently) and by a
/// [`Transactional`] one (all replies of a batch inside one transaction). Sealed: implemented by
/// exactly those two types. `Cx` is the originating batch handler's context type, threaded so the
/// static [`PublishTransform`](crate::PublishTransform) reads the delivery while publishing each reply.
pub trait ReplyPublisher<Cx = ()>: Sealed + Send + Sync {
    /// The payload form of the publisher underneath, so a batch's replies encode into what that
    /// form carries. See [`Publisher::Payload`].
    type Payload: PayloadForm;

    /// The codec replies are encoded with (also reused as the decode codec when a batch
    /// publishing handler is mounted without an explicit one).
    type Codec: Codec;

    /// Returns the reply codec.
    #[doc(hidden)]
    fn reply_codec(&self) -> &Self::Codec;

    /// Publishes one batch's replies to `name` through `pipeline`, reading the originating
    /// delivery through `cx`.
    #[doc(hidden)]
    fn publish_batch<'a, T, PP>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a PP,
        cx: &'a PublishContext<'a, Cx>,
        encode: <Self::Payload as PayloadForm>::Encode<'a>,
    ) -> impl Future<Output = Result<(), PublishBatchError>> + Send
    where
        T: Serialize + Sync,
        PP: PublishPipeline;
}

impl<P, C, PL, BL, Cx> ReplyPublisher<Cx> for TypedPublisher<P, C, PL, BL>
where
    P: Publisher,
    C: Codec,
    PL: Send + Sync,
    BL: BatchPublishTransform<Cx, P::Options>,
    Cx: Sync,
{
    type Payload = P::Payload;
    type Codec = C;

    fn reply_codec(&self) -> &C {
        self.codec()
    }

    /// Each reply is published independently: a mid-batch failure leaves the earlier replies
    /// visible, and the retried batch may publish them again (at-least-once).
    async fn publish_batch<'a, T, PP>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a PP,
        cx: &'a PublishContext<'a, Cx>,
        mut encode: <P::Payload as PayloadForm>::Encode<'a>,
    ) -> Result<(), PublishBatchError>
    where
        T: Serialize + Sync,
        PP: PublishPipeline,
    {
        for (sent, reply) in replies.iter().enumerate() {
            let slot = <P::Payload as PayloadForm>::encode_again(&mut encode);
            self.publish_batched(name, reply, pipeline, cx, slot)
                .await
                .map_err(|error| PublishBatchError::new(sent, error))?;
        }
        Ok(())
    }
}

impl<P, C, PL, BL, Cx> ReplyPublisher<Cx> for Transactional<P, C, PL, BL>
where
    P: TransactionalPublisher,
    C: Codec,
    PL: Send + Sync,
    BL: BatchPublishTransform<Cx, P::Options>,
    Cx: Sync,
{
    type Payload = P::Payload;
    type Codec = C;

    fn reply_codec(&self) -> &C {
        self.inner.codec()
    }

    /// All replies publish inside one transaction: begin, publish each, commit. Any failure
    /// aborts the transaction, so none of the batch's replies become visible.
    async fn publish_batch<'a, T, PP>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a PP,
        cx: &'a PublishContext<'a, Cx>,
        mut encode: <P::Payload as PayloadForm>::Encode<'a>,
    ) -> Result<(), PublishBatchError>
    where
        T: Serialize + Sync,
        PP: PublishPipeline,
    {
        // The transaction aborts as a whole, so a failure leaves none of the replies sent.
        let aborted = |error| PublishBatchError::new(0, error);
        let publisher = &self.inner.publisher;
        #[cfg(feature = "std")]
        let _turn = self.turn.lock().await;
        publisher
            .begin_transaction()
            .await
            .map_err(|error| aborted(BoxError::from(error)))?;
        for reply in replies {
            let slot = <P::Payload as PayloadForm>::encode_again(&mut encode);
            if let Err(err) = self
                .inner
                .publish_batched(name, reply, pipeline, cx, slot)
                .await
            {
                abort_quietly(publisher).await;
                return Err(aborted(err));
            }
        }
        // Per the TransactionalPublisher contract a failed commit closes the transaction, so
        // there is nothing left to abort here; the error alone settles the batch as failed.
        publisher
            .commit()
            .await
            .map_err(|error| aborted(BoxError::from(error)))
    }
}

/// Aborts a failed transaction; an abort failure is logged, not propagated, because the
/// original publish / commit error is the one the caller acts on.
async fn abort_quietly<P: TransactionalPublisher>(publisher: &P) {
    if let Err(err) = publisher.abort().await {
        warn!(target: "ruststream::dispatch", error = %err, "transaction abort failed");
    }
}
