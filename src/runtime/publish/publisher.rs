//! The live reply sink: a byte publisher paired with a codec and its transform stacks.

use std::fmt;

use bytes::BytesMut;
use serde::Serialize;

use super::{
    BatchPublishTransform, BatchTransformIdentity, Outgoing, PublishContext, PublishPipeline,
    PublishTransform, PublishTransformIdentity,
};
use crate::Publisher;
use crate::codec::Codec;
use crate::runtime::lifecycle::BoxError;

/// A byte [`Publisher`] paired with a [`Codec`] and a static [`PublishTransform`] stack: what a
/// reply wiring becomes once the broker connects.
///
/// It carries *how* a reply is encoded and the per-publisher transforms, while *where* it goes
/// (the destination name) is supplied per send - so one live stack serves handlers replying to
/// different names. Machinery behind
/// [`ReplyWiring`](super::ReplyWiring): the mount site builds the wiring, the runtime pairs it
/// into this, and the dispatch publishes through it.
pub struct TypedPublisher<P, C, PL = PublishTransformIdentity, BL = BatchTransformIdentity> {
    // The reply wirings and the transaction scopes publish through these directly, so they are
    // visible across the publish modules and nowhere else.
    pub(super) publisher: P,
    pub(super) codec: C,
    layers: PL,
    batch_layers: BL,
}

impl<P, C, PL, BL> TypedPublisher<P, C, PL, BL> {
    /// The live stack a paired wiring produces.
    pub(crate) const fn live(publisher: P, codec: C, layers: PL, batch_layers: BL) -> Self {
        Self {
            publisher,
            codec,
            layers,
            batch_layers,
        }
    }

    /// The codec this publisher encodes replies with.
    pub(crate) const fn codec(&self) -> &C {
        &self.codec
    }
}

#[cfg(test)]
impl<P, C> TypedPublisher<P, C, PublishTransformIdentity, BatchTransformIdentity> {
    /// The live stack of a wiring that named a codec and no transforms. The crate's own dispatch
    /// tests drive this shape directly, without a mount site to build it for them.
    pub(crate) const fn with_codec(publisher: P, codec: C) -> Self {
        Self::live(
            publisher,
            codec,
            PublishTransformIdentity,
            BatchTransformIdentity,
        )
    }
}

impl<P: Publisher, C: Codec, PL, BL> TypedPublisher<P, C, PL, BL> {
    /// The message a reply starts from: `name` and `payload` over the publisher's own base
    /// ([`Publisher::base_headers`]).
    ///
    /// A reply never passes the publish builder, so this is where the base reaches it, in the same
    /// order the builder uses: the handle's headers first, whatever the reply itself names written
    /// over them. A publisher with no base yields the empty map a reply always started from, so
    /// nothing is cloned on the path every broker publisher takes today.
    fn outgoing<'n>(&self, name: &'n str, payload: BytesMut) -> Outgoing<'n> {
        let mut out = Outgoing::new(name, payload);
        if let Some(base) = self.publisher.base_headers() {
            *out.headers_mut() = base.clone();
        }
        out
    }

    /// Encodes `value`, applies the static transforms (reading the originating delivery through
    /// `cx`), then publishes to `name` through `pipeline`.
    pub(crate) async fn publish<T: Serialize + Sync, Cx, PP>(
        &self,
        name: &str,
        value: &T,
        pipeline: &PP,
        cx: &PublishContext<'_, Cx>,
    ) -> Result<(), BoxError>
    where
        PL: PublishTransform<Cx>,
        BL: Sync,
        Cx: Sync,
        PP: PublishPipeline,
    {
        let payload = self
            .codec
            .encode(value)
            .map_err(|e| Box::new(e) as BoxError)?;
        let mut out = self.outgoing(name, payload);
        self.layers.apply(&mut out, cx);
        pipeline.run(&mut out, &self.publisher).await
    }

    /// Like [`publish`](Self::publish), but the reply is a typed-headers pair: the contract
    /// serializes into the outgoing headers before the transforms run, and the body encodes
    /// through the reply codec.
    pub(crate) async fn publish_pair<Hd: Serialize + Sync, T: Serialize + Sync, Cx, PP>(
        &self,
        name: &str,
        headers: &Hd,
        value: &T,
        pipeline: &PP,
        cx: &PublishContext<'_, Cx>,
    ) -> Result<(), BoxError>
    where
        PL: PublishTransform<Cx>,
        BL: Sync,
        Cx: Sync,
        PP: PublishPipeline,
    {
        let payload = self
            .codec
            .encode(value)
            .map_err(|e| Box::new(e) as BoxError)?;
        let mut out = self.outgoing(name, payload);
        out.headers_mut()
            .insert_typed(headers)
            .map_err(|e| Box::new(e) as BoxError)?;
        self.layers.apply(&mut out, cx);
        pipeline.run(&mut out, &self.publisher).await
    }

    /// Like [`publish`](Self::publish), but applies the batch-only [`BatchPublishTransform`] stack
    /// instead of the per-message [`PublishTransform`] one. Used per reply on the batch path: the
    /// per-message transforms do not run for batched replies (a transform wanted on both paths is
    /// added to each, reusing it on the batch side with [`for_batch`]).
    pub(crate) async fn publish_batched<T: Serialize + Sync, Cx, PP>(
        &self,
        name: &str,
        value: &T,
        pipeline: &PP,
        cx: &PublishContext<'_, Cx>,
    ) -> Result<(), BoxError>
    where
        PL: Sync,
        BL: BatchPublishTransform<Cx>,
        Cx: Sync,
        PP: PublishPipeline,
    {
        let payload = self
            .codec
            .encode(value)
            .map_err(|e| Box::new(e) as BoxError)?;
        let mut out = self.outgoing(name, payload);
        self.batch_layers.apply(&mut out, cx);
        pipeline.run(&mut out, &self.publisher).await
    }
}

impl<P, C, PL, BL> fmt::Debug for TypedPublisher<P, C, PL, BL> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedPublisher").finish_non_exhaustive()
    }
}

/// A [`TypedPublisher`] whose batch replies are published inside one broker transaction.
///
/// What a `.transactional()` wiring pairs into. Per batch, the runtime begins a transaction,
/// publishes every reply, then commits before the incoming batch is acked; any failure aborts the
/// transaction and the batch is retried, so replies are never half-visible.
pub struct Transactional<P, C, PL = PublishTransformIdentity, BL = BatchTransformIdentity> {
    pub(super) inner: TypedPublisher<P, C, PL, BL>,
}

impl<P, C, PL, BL> Transactional<P, C, PL, BL> {
    /// The live transactional sink a paired wiring produces.
    pub(crate) const fn live(inner: TypedPublisher<P, C, PL, BL>) -> Self {
        Self { inner }
    }
}

impl<P, C, PL, BL> fmt::Debug for Transactional<P, C, PL, BL> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transactional").finish_non_exhaustive()
    }
}
