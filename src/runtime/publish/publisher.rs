//! The typed publisher: a byte publisher paired with a codec and its transform stacks.

use std::fmt;

use serde::Serialize;

use crate::codec::Codec;
use crate::runtime::lifecycle::BoxError;
// `DefaultCodec` only exists when a codec feature is on; the impl that names it is gated the same
// way, so an ungated import would break `--no-default-features`.
use super::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, Outgoing,
    PublishContext, PublishPipeline, PublishTransform, PublishTransformIdentity,
    PublishTransformStack,
};
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::{ConnectedBroker, PairError, PublishPolicy, Publisher, TransactionalPublisher};

/// A byte [`Publisher`] paired with a [`Codec`] and a static [`PublishTransform`] stack, ready to send
/// typed values.
///
/// The publish-side counterpart to a typed subscriber: it carries *how* a value is encoded and the
/// per-publisher transforms ([`transform`](Self::transform)), while *where* it goes (the destination name)
/// is supplied per send - so one `TypedPublisher` is reused across handlers replying to different
/// names. The `#[subscriber(.., publish("name"))]` reply form supplies the name; the
/// `TypedPublisher` is passed at wiring.
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # {
/// use ruststream::codec::JsonCodec;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::TypedPublisher;
///
/// let broker = MemoryBroker::new();
/// let with_default = TypedPublisher::new(broker.publisher()); // DefaultCodec
/// let explicit = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
/// # let _ = (with_default, explicit);
/// # }
/// ```
///
/// [macro]: crate::subscriber
pub struct TypedPublisher<P, C, PL = PublishTransformIdentity, BL = BatchTransformIdentity> {
    // The reply wirings and the transaction scopes publish through these directly, so they are
    // visible across the publish modules and nowhere else.
    pub(super) publisher: P,
    pub(super) codec: C,
    layers: PL,
    batch_layers: BL,
}

impl<P, C> TypedPublisher<P, C, PublishTransformIdentity, BatchTransformIdentity> {
    /// Pairs `publisher` with an explicit `codec` and no static transforms.
    #[must_use]
    pub fn with_codec(publisher: P, codec: C) -> Self {
        Self {
            publisher,
            codec,
            layers: PublishTransformIdentity,
            batch_layers: BatchTransformIdentity,
        }
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<P> TypedPublisher<P, DefaultCodec, PublishTransformIdentity, BatchTransformIdentity> {
    /// Pairs `publisher` with the [`DefaultCodec`](DefaultCodec) and no static
    /// transforms. Use [`with_codec`](Self::with_codec) to name a codec explicitly.
    #[must_use]
    pub fn new(publisher: P) -> Self {
        Self::with_codec(publisher, DefaultCodec::default())
    }
}

impl<P, C, PL, BL> TypedPublisher<P, C, PL, BL> {
    /// The codec this publisher encodes replies with. Lets the runtime reuse it as the decode
    /// codec when a publishing handler is mounted without an explicit one.
    pub(crate) const fn codec(&self) -> &C {
        &self.codec
    }

    /// Adds a static [`PublishTransform`], applied to every single-message reply from this publisher
    /// (a `#[subscriber(.., publish(..))]` handler). It does not run on the batch path; use
    /// [`batch_transform`](Self::batch_transform) for that. The first one added runs first (closest
    /// to the encoded value).
    #[must_use]
    pub fn transform<N>(
        self,
        transform: N,
    ) -> TypedPublisher<P, C, PublishTransformStack<PL, N>, BL> {
        TypedPublisher {
            publisher: self.publisher,
            codec: self.codec,
            layers: PublishTransformStack {
                inner: self.layers,
                outer: transform,
            },
            batch_layers: self.batch_layers,
        }
    }

    /// Adds a static [`BatchPublishTransform`], applied to every reply of a
    /// `#[subscriber(batch(..), publish(..))]` handler only (after the per-message
    /// [`PublishTransform`] stack), never to a single-message reply. Wrap a per-message
    /// [`PublishTransform`] with [`for_batch`] to reuse it here. The single-message mounts reject a
    /// publisher carrying a non-trivial batch stack, so a batch-only transform cannot leak onto the
    /// single path.
    #[must_use]
    pub fn batch_transform<N>(
        self,
        transform: N,
    ) -> TypedPublisher<P, C, PL, BatchPublishTransformStack<BL, N>> {
        TypedPublisher {
            publisher: self.publisher,
            codec: self.codec,
            layers: self.layers,
            batch_layers: BatchPublishTransformStack {
                inner: self.batch_layers,
                outer: transform,
            },
        }
    }

    /// Switches batch reply publishing to one broker transaction per batch: the replies of a
    /// `#[subscriber(batch(..), publish(..))]` handler all become visible atomically on commit,
    /// or none of them do.
    ///
    /// The leaf may be a live publisher or a publish policy; either way the transactional
    /// requirement is enforced where the wiring is consumed (the batch publishing mounts bound
    /// the live form by [`TransactionalPublisher`](crate::TransactionalPublisher), and pairing a
    /// policy stack requires the same), so a broker without transactions still fails to compile,
    /// at the registration instead of here. The returned wiring is accepted by the batch
    /// publishing mounts only: a one-message transaction adds broker round-trips for no
    /// atomicity gain, so the single-message forms keep taking a plain [`TypedPublisher`].
    #[must_use]
    pub fn transactional(self) -> Transactional<P, C, PL, BL> {
        Transactional { inner: self }
    }
}

impl<P: Publisher, C: Codec, PL, BL> TypedPublisher<P, C, PL, BL> {
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
        let mut out = Outgoing::new(name, payload);
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
        let mut out = Outgoing::new(name, payload);
        self.batch_layers.apply(&mut out, cx);
        pipeline.run(&mut out, &self.publisher).await
    }
}

impl<P, C, PL, BL> fmt::Debug for TypedPublisher<P, C, PL, BL> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedPublisher").finish_non_exhaustive()
    }
}

// Pairing is functorial over the combinator stack: a typed publisher whose leaf is a policy is
// itself a policy, and pairing swaps the leaf for its live form while the codec and transform
// stacks travel unchanged. Fully monomorphized; no erasure anywhere on this path.
impl<CB, P, C, PL, BL> PublishPolicy<CB> for TypedPublisher<P, C, PL, BL>
where
    CB: ConnectedBroker,
    P: PublishPolicy<CB> + Send,
    C: Send,
    PL: Send,
    BL: Send,
{
    type Live = TypedPublisher<P::Live, C, PL, BL>;

    async fn pair(self, connected: &CB) -> Result<Self::Live, PairError> {
        Ok(TypedPublisher {
            publisher: self.publisher.pair(connected).await?,
            codec: self.codec,
            layers: self.layers,
            batch_layers: self.batch_layers,
        })
    }
}

/// A [`TypedPublisher`] whose batch replies are published inside one broker transaction.
///
/// Built with [`TypedPublisher::transactional`]; accepted by the
/// `include_batch_publishing` mounts. Per batch, the runtime begins a transaction, publishes
/// every reply, then commits before the incoming batch is acked; any failure aborts the
/// transaction and the batch is retried, so replies are never half-visible.
pub struct Transactional<P, C, PL = PublishTransformIdentity, BL = BatchTransformIdentity> {
    pub(super) inner: TypedPublisher<P, C, PL, BL>,
}

impl<P, C, PL, BL> fmt::Debug for Transactional<P, C, PL, BL> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transactional").finish_non_exhaustive()
    }
}

// The transactional wiring is a policy over a policy: pairing resolves the inner stack and keeps
// the transactional marker, provided the leaf's live form actually is transactional.
impl<CB, P, C, PL, BL> PublishPolicy<CB> for Transactional<P, C, PL, BL>
where
    CB: ConnectedBroker,
    P: PublishPolicy<CB> + Send,
    P::Live: TransactionalPublisher,
    C: Send,
    PL: Send,
    BL: Send,
{
    type Live = Transactional<P::Live, C, PL, BL>;

    async fn pair(self, connected: &CB) -> Result<Self::Live, PairError> {
        Ok(Transactional {
            inner: self.inner.pair(connected).await?,
        })
    }
}
