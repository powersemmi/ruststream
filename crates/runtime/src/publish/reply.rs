//! Reply wirings: how a handler's return value reaches a publisher.

#[cfg(ruststream_alloc)]
use alloc::vec::Vec;
use core::fmt;
use ruststream_core::diag::warn;
use ruststream_core::{MaybeSend, MaybeSendSync, MaybeSync, Places};
use serde::Serialize;

use super::{
    BatchPublishTransform, PublishContext, PublishPipeline, Transactional, TypedPublisher,
};
use crate::HeaderSet;
use crate::codec::ActiveCodec as Codec;
use crate::copied::CopiedPayload;
use crate::lifecycle::BoxError;
use crate::publish::sealed::Sealed;
#[cfg(not(ruststream_alloc))]
use crate::publish_failure::Failed;
use crate::publish_failure::FailedMessage;
use crate::publish_failure::Refusal;
use crate::{PayloadForm, Publisher, TransactionalPublisher};

/// A batch whose replies did not all go out: the cause, and which replies were lost.
///
/// A reply the publisher refused comes back with the message that was sent; the replies that
/// were never attempted, or that a transaction aborted, are lost with it. Machinery; never named
/// directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct BatchRefusal<'p> {
    /// The first reply that did not go out.
    lost_from: usize,
    /// Why they did not.
    cause: Cause<'p>,
}

/// Why a batch's replies did not all go out.
#[derive(Debug)]
enum Cause<'p> {
    /// The reply at the index was refused, with what came back for it.
    Refused(usize, Refusal<'p>),
    /// No single reply was refused: the transaction could not begin or commit.
    Whole(BoxError),
}

impl<'p> BatchRefusal<'p> {
    /// The refusal of the reply at `index`, with every reply from `lost_from` on lost.
    fn at(index: usize, lost_from: usize, refusal: Refusal<'p>) -> Self {
        Self {
            lost_from,
            cause: Cause::Refused(index, refusal),
        }
    }

    /// A failure of the batch as a whole, which loses every reply.
    fn whole(error: BoxError) -> Self {
        Self {
            lost_from: 0,
            cause: Cause::Whole(error),
        }
    }

    /// The cause alone.
    pub(crate) fn into_error(self) -> BoxError {
        match self.cause {
            Cause::Refused(_, refusal) => refusal.into_error(),
            Cause::Whole(error) => error,
        }
    }

    /// The cause and the replies that did not go out, as messages for `name`.
    ///
    /// The list of lost messages is as long as the batch, so a build without `alloc` reports a
    /// batch's lost replies through the batch's own fixed buffer instead.
    ///
    /// The refused reply is the message that was sent, with `lent` taking the bytes a lending
    /// publisher was shown; the others are encoded again with `encode`, because nothing of them
    /// was handed to a publisher that kept it.
    #[cfg(ruststream_alloc)]
    pub(crate) fn into_lost<T>(
        self,
        name: &str,
        replies: &[T],
        encode: impl Fn(&T) -> CopiedPayload,
        lent: impl FnOnce(usize, usize) -> CopiedPayload,
    ) -> (BoxError, Vec<FailedMessage>) {
        let Self { lost_from, cause } = self;
        let (cause, refused_at, mut sent) = match cause {
            Cause::Refused(index, refusal) => {
                let (error, message) = refusal.into_message(name, lent);
                (error, Some(index), Some(message))
            }
            Cause::Whole(error) => (error, None, None),
        };
        let lost = replies
            .iter()
            .enumerate()
            .skip(lost_from)
            .map(|(index, reply)| {
                if Some(index) == refused_at
                    && let Some(message) = sent.take()
                {
                    return message;
                }
                FailedMessage::new(name, Some(encode(reply)), HeaderSet::new())
            })
            .collect();
        (cause, lost)
    }
}

#[cfg(not(ruststream_alloc))]
impl BatchRefusal<'_> {
    /// The replies a refused batch publish lost, each as a [`Failed`] with the batch's cause, in
    /// the batch's order: the refused reply as it was sent, the others encoded again.
    ///
    /// The list is the call's own fixed buffer of `ELEMENTS`, the most replies a batch holds: the
    /// mount fixes the batch size at compile time, and the buffer is as large.
    ///
    /// # Panics
    ///
    /// When the batch holds more replies than `ELEMENTS`, which the mount's batch size rules out.
    pub(crate) fn into_failed<T, const ELEMENTS: usize>(
        self,
        name: &str,
        replies: &[T],
        encode: impl Fn(&T) -> CopiedPayload,
        lent: impl FnOnce(usize, usize) -> CopiedPayload,
    ) -> heapless::Vec<Failed, ELEMENTS> {
        let Self { lost_from, cause } = self;
        let (cause, refused_at, mut sent) = match cause {
            Cause::Refused(index, refusal) => {
                let (error, message) = refusal.into_message(name, lent);
                (error, Some(index), Some(message))
            }
            Cause::Whole(error) => (error, None, None),
        };
        let mut lost = heapless::Vec::new();
        for (index, reply) in replies.iter().enumerate().skip(lost_from) {
            let message = match sent.take() {
                Some(message) if Some(index) == refused_at => message,
                other => {
                    sent = other;
                    FailedMessage::new(name, Some(encode(reply)), HeaderSet::new())
                }
            };
            // A batch is at most as many replies as the mount's size, which the buffer is made for.
            assert!(
                lost.push(Failed::new(cause, message)).is_ok(),
                "a batch holds at most as many replies as its mount's size"
            );
        }
        lost
    }
}

impl fmt::Display for BatchRefusal<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            Cause::Refused(_, refusal) => fmt::Display::fmt(refusal, f),
            Cause::Whole(error) => fmt::Display::fmt(error, f),
        }
    }
}

/// The live reply sink a batch's replies travel through.
///
/// Implemented by a plain [`TypedPublisher`] (each reply published independently) and by a
/// [`Transactional`] one (all replies of a batch inside one transaction). Sealed: implemented by
/// exactly those two types. `Cx` is the originating batch handler's context type, threaded so the
/// static [`PublishTransform`](crate::PublishTransform) reads the delivery while publishing each reply.
pub trait ReplyPublisher<Cx = ()>: Sealed + MaybeSendSync {
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
    /// delivery through `cx`. A refused reply moves into one of `places`.
    #[doc(hidden)]
    fn publish_batch<'a, 'p, T, PP>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a PP,
        cx: &'a PublishContext<'a, Cx>,
        encode: <Self::Payload as PayloadForm>::Encode<'a>,
        places: Places<'p>,
    ) -> impl Future<Output = Result<(), BatchRefusal<'p>>> + MaybeSend
    where
        T: Serialize + MaybeSync,
        PP: PublishPipeline;
}

impl<P, C, PL, BL, Cx> ReplyPublisher<Cx> for TypedPublisher<P, C, PL, BL>
where
    P: Publisher,
    C: Codec,
    PL: MaybeSendSync,
    BL: BatchPublishTransform<Cx, P::Options>,
    Cx: MaybeSync,
{
    type Payload = P::Payload;
    type Codec = C;

    fn reply_codec(&self) -> &C {
        self.codec()
    }

    /// Each reply is published independently: a mid-batch failure leaves the earlier replies
    /// visible, and the retried batch may publish them again (at-least-once).
    async fn publish_batch<'a, 'p, T, PP>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a PP,
        cx: &'a PublishContext<'a, Cx>,
        mut encode: <P::Payload as PayloadForm>::Encode<'a>,
        places: Places<'p>,
    ) -> Result<(), BatchRefusal<'p>>
    where
        T: Serialize + MaybeSync,
        PP: PublishPipeline,
    {
        for (index, reply) in replies.iter().enumerate() {
            let slot = <P::Payload as PayloadForm>::encode_again(&mut encode);
            self.publish_batched(name, reply, pipeline, cx, slot, places)
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
    PL: MaybeSendSync,
    BL: BatchPublishTransform<Cx, P::Options>,
    Cx: MaybeSync,
{
    type Payload = P::Payload;
    type Codec = C;

    fn reply_codec(&self) -> &C {
        self.inner.codec()
    }

    /// All replies publish inside one transaction: begin, publish each, commit. Any failure
    /// aborts the transaction, so none of the batch's replies become visible.
    async fn publish_batch<'a, 'p, T, PP>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a PP,
        cx: &'a PublishContext<'a, Cx>,
        mut encode: <P::Payload as PayloadForm>::Encode<'a>,
        places: Places<'p>,
    ) -> Result<(), BatchRefusal<'p>>
    where
        T: Serialize + MaybeSync,
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
                .publish_batched(name, reply, pipeline, cx, slot, places)
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
