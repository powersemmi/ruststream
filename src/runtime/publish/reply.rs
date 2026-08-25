//! Reply wirings: how a handler's return value reaches a publisher.

use std::future::Future;

use serde::Serialize;
use tracing::warn;

use super::{
    BatchPublishTransform, PublishContext, PublishPipeline, TransactionScope, Transactional,
    TypedPublisher,
};
use crate::codec::Codec;
use crate::runtime::lifecycle::BoxError;
use crate::runtime::publish::sealed::Sealed;
use crate::{Publisher, TransactionalPublisher};

/// The decode-codec view of a reply wiring, readable before pairing.
///
/// Both wrapper shapes carry their codec as a field, whatever the leaf (a live publisher or a
/// publish policy), so the batch publishing mounts can reuse the reply codec for decoding
/// without requiring a live leaf at include time. Sealed like [`ReplyPublisher`].
pub trait ReplyWiring: Sealed {
    /// The codec replies are encoded with.
    type Codec: Codec + Clone;

    /// Returns the reply codec.
    #[doc(hidden)]
    fn decode_codec(&self) -> &Self::Codec;
}

impl<P, C: Codec + Clone, PL, BL> ReplyWiring for TypedPublisher<P, C, PL, BL> {
    type Codec = C;

    fn decode_codec(&self) -> &C {
        self.codec()
    }
}

impl<P, C: Codec + Clone, PL, BL> ReplyWiring for Transactional<P, C, PL, BL> {
    type Codec = C;

    fn decode_codec(&self) -> &C {
        self.inner.codec()
    }
}

/// The reply wiring accepted by the batch reply-publishing mounts.
///
/// Implemented by a plain [`TypedPublisher`] (each reply published independently) and by a
/// [`Transactional`] one (all replies of a batch inside one transaction). Sealed: implemented by
/// exactly those two types. `Cx` is the originating batch handler's context type, threaded so the
/// static [`PublishTransform`](crate::runtime::PublishTransform) reads the delivery while publishing each reply.
pub trait ReplyPublisher<Cx = ()>: Sealed + Send + Sync {
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
    ) -> impl Future<Output = Result<(), BoxError>> + Send
    where
        T: Serialize + Sync,
        PP: PublishPipeline;
}

impl<P, C, PL, BL, Cx> ReplyPublisher<Cx> for TypedPublisher<P, C, PL, BL>
where
    P: Publisher,
    C: Codec,
    PL: Send + Sync,
    BL: BatchPublishTransform<Cx>,
    Cx: Sync,
{
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
    ) -> Result<(), BoxError>
    where
        T: Serialize + Sync,
        PP: PublishPipeline,
    {
        for reply in replies {
            self.publish_batched(name, reply, pipeline, cx).await?;
        }
        Ok(())
    }
}

impl<P, C, PL, BL, Cx> ReplyPublisher<Cx> for Transactional<P, C, PL, BL>
where
    P: TransactionalPublisher,
    C: Codec,
    PL: Send + Sync,
    BL: BatchPublishTransform<Cx>,
    Cx: Sync,
{
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
    ) -> Result<(), BoxError>
    where
        T: Serialize + Sync,
        PP: PublishPipeline,
    {
        let publisher = &self.inner.publisher;
        publisher
            .begin_transaction()
            .await
            .map_err(|e| Box::new(e) as BoxError)?;
        for reply in replies {
            if let Err(err) = self.inner.publish_batched(name, reply, pipeline, cx).await {
                abort_quietly(publisher).await;
                return Err(err);
            }
        }
        // Per the TransactionalPublisher contract a failed commit closes the transaction, so
        // there is nothing left to abort here; the error alone settles the batch as failed.
        publisher
            .commit()
            .await
            .map_err(|err| Box::new(err) as BoxError)
    }
}

/// Aborts a failed transaction; an abort failure is logged, not propagated, because the
/// original publish / commit error is the one the caller acts on.
async fn abort_quietly<P: TransactionalPublisher>(publisher: &P) {
    if let Err(err) = publisher.abort().await {
        warn!(target: "ruststream::dispatch", error = %err, "transaction abort failed");
    }
}

impl<P, C, PL, BL> Transactional<P, C, PL, BL>
where
    P: TransactionalPublisher,
    C: Sync,
    PL: Sync,
    BL: Sync,
{
    /// Opens a broker transaction and returns the [`TransactionScope`] that owns it.
    ///
    /// The scope is the only handle on the transaction: publishes go through it, and it is
    /// consumed by [`commit`](TransactionScope::commit) or [`abort`](TransactionScope::abort).
    /// A second `begin` on this wrapper, a commit without a begin, or a publish after the commit
    /// are not expressible - the methods do not exist on the types those states leave behind.
    ///
    /// This is the typed sugar over the borrowed transaction kind
    /// ([`TransactionalPublisher`]): the scope claims the handle's single broker-side
    /// transaction, so one scope per wrapper is open at a time. Brokers whose transactions are
    /// client buffers additionally offer the owned kind through
    /// [`TypedPublisher::transaction`], where every call opens an independent buffer-owning
    /// [`TypedTransaction`](crate::runtime::TypedTransaction).
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::TypedPublisher;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MemoryBroker::new();
    /// let publisher = TypedPublisher::new(broker.publisher()).transactional();
    ///
    /// let mut scope = publisher.begin().await?;
    /// scope.publish("orders.settled", &42_u32).await?;
    /// scope.commit().await?;
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to start a transaction.
    pub async fn begin(&self) -> Result<TransactionScope<'_, P, C>, P::Error> {
        self.inner.publisher.begin_transaction().await?;
        Ok(TransactionScope {
            publisher: &self.inner.publisher,
            codec: &self.inner.codec,
            open: true,
        })
    }
}
