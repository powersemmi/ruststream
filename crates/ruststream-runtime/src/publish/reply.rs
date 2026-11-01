//! Reply wirings: how a handler's return value reaches a publisher.

use alloc::vec::Vec;
use bytes::Bytes;
use core::fmt;
use serde::Serialize;
use tracing::warn;

use super::{
    BatchPublishTransform, PublishContext, PublishPipeline, Transactional, TypedPublisher,
};
use crate::codec::Codec;
use crate::lifecycle::BoxError;
use crate::publish::sealed::Sealed;
use crate::publish_failure::{FailedMessage, Refusal};
use crate::{HeaderMap, PayloadForm, Publisher, TransactionalPublisher};

/// A batch whose replies did not all go out: the cause, and which replies were lost.
///
/// A reply the publisher refused comes back with the message that was sent; the replies that
/// were never attempted, or that a transaction aborted, are lost with it. Machinery; never named
/// directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct BatchRefusal {
    /// The first reply that did not go out.
    lost_from: usize,
    /// The refused reply and what came back for it, when a publish was refused.
    refused: Option<(usize, Refusal)>,
    /// The cause when no single reply was refused: the transaction could not begin or commit.
    error: Option<BoxError>,
}

impl BatchRefusal {
    /// The refusal of the reply at `index`, with every reply from `lost_from` on lost.
    fn at(index: usize, lost_from: usize, refusal: Refusal) -> Self {
        Self {
            lost_from,
            refused: Some((index, refusal)),
            error: None,
        }
    }

    /// A failure of the batch as a whole, which loses every reply.
    fn whole(error: BoxError) -> Self {
        Self {
            lost_from: 0,
            refused: None,
            error: Some(error),
        }
    }

    /// The cause alone.
    pub(crate) fn into_error(self) -> BoxError {
        match (self.refused, self.error) {
            (Some((_, refusal)), _) => refusal.into_error(),
            (None, Some(error)) => error,
            (None, None) => BoxError::from("a batch reply was refused"),
        }
    }

    /// The cause and the replies that did not go out, as messages for `name`.
    ///
    /// The refused reply is the message that was sent, with `lent` taking the bytes a lending
    /// publisher was shown; the others are encoded again with `encode`, because nothing of them
    /// was handed to a publisher that kept it.
    pub(crate) fn into_lost<T>(
        self,
        name: &str,
        replies: &[T],
        encode: impl Fn(&T) -> Bytes,
        lent: impl FnOnce(usize, usize) -> Bytes,
    ) -> (BoxError, Vec<FailedMessage>) {
        let Self {
            lost_from,
            refused,
            error,
        } = self;
        let (refused_at, refusal) = match refused {
            Some((index, refusal)) => (Some(index), Some(refusal)),
            None => (None, None),
        };
        let mut sent = refusal.map(|refusal| refusal.into_message(name, lent));
        let mut cause = error;
        let lost = replies
            .iter()
            .enumerate()
            .skip(lost_from)
            .map(|(index, reply)| {
                if Some(index) == refused_at
                    && let Some((error, message)) = sent.take()
                {
                    cause = Some(error);
                    return message;
                }
                FailedMessage::new(name, encode(reply), HeaderMap::new())
            })
            .collect();
        (
            cause.unwrap_or_else(|| BoxError::from("a batch reply was refused")),
            lost,
        )
    }
}

impl fmt::Display for BatchRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.refused, &self.error) {
            (Some((_, refusal)), _) => fmt::Display::fmt(refusal, f),
            (None, Some(error)) => fmt::Display::fmt(error, f),
            (None, None) => f.write_str("a batch reply was refused"),
        }
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
    ) -> impl Future<Output = Result<(), BatchRefusal>> + Send
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
    ) -> Result<(), BatchRefusal>
    where
        T: Serialize + Sync,
        PP: PublishPipeline,
    {
        for (index, reply) in replies.iter().enumerate() {
            let slot = <P::Payload as PayloadForm>::encode_again(&mut encode);
            self.publish_batched(name, reply, pipeline, cx, slot)
                .await
                .map_err(|refusal| BatchRefusal::at(index, index, refusal))?;
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
    ) -> Result<(), BatchRefusal>
    where
        T: Serialize + Sync,
        PP: PublishPipeline,
    {
        let publisher = &self.inner.publisher;
        #[cfg(feature = "std")]
        let _turn = self.turn.lock().await;
        publisher
            .begin_transaction()
            .await
            .map_err(|err| BatchRefusal::whole(BoxError::from(err)))?;
        for (index, reply) in replies.iter().enumerate() {
            let slot = <P::Payload as PayloadForm>::encode_again(&mut encode);
            if let Err(refusal) = self
                .inner
                .publish_batched(name, reply, pipeline, cx, slot)
                .await
            {
                abort_quietly(publisher).await;
                // The transaction is aborted, so every reply of the batch is lost.
                return Err(BatchRefusal::at(index, 0, refusal));
            }
        }
        // Per the TransactionalPublisher contract a failed commit closes the transaction, so
        // there is nothing left to abort here; the error alone settles the batch as failed.
        publisher
            .commit()
            .await
            .map_err(|err| BatchRefusal::whole(BoxError::from(err)))
    }
}

/// Aborts a failed transaction; an abort failure is logged, not propagated, because the
/// original publish / commit error is the one the caller acts on.
async fn abort_quietly<P: TransactionalPublisher>(publisher: &P) {
    if let Err(err) = publisher.abort().await {
        warn!(target: "ruststream::dispatch", error = %err, "transaction abort failed");
    }
}
