//! Outgoing message and the publish middleware pipeline.
//!
//! When a handler's reply is published (via `#[subscriber(.., publish(..))]`), it flows through a
//! chain of [`PublishLayer`] before reaching the broker publisher. Middleware transform the
//! payload (for example, wrap it in a Confluent / Avro envelope) and enrich the headers
//! (content-type, schema id), or observe it (publish metrics). The chain is symmetric to the
//! consume-side static [`Stack`](super::Stack).

use std::{borrow::Cow, fmt, future::Future, pin::Pin, sync::Arc};

use bytes::BytesMut;
use serde::Serialize;
use thiserror::Error;
use tracing::warn;

use super::lifecycle::BoxError;
use crate::codec::{Codec, CodecError};
// `DefaultCodec` only exists when a codec feature is on; the impl that names it is gated the same
// way, so an ungated import would break `--no-default-features`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::runtime::publish::sealed::Sealed;
use crate::{
    ConnectedBroker, Headers, OutgoingMessage, OwnedTransactions, PairError, PublishPolicy,
    Publisher, Transaction, TransactionalPublisher,
};

// The boxed future of the DYNAMIC middleware path only (PublishDynLayer / PublishDynNext).
// The static pipeline returns unboxed RPITIT futures; only the opt-in runtime-composed list
// pays this allocation.
type PublishFut<'a> = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>>;

/// A mutable outgoing message flowing through the publish pipeline.
///
/// The [`name`](Self::name) is a [`Cow`]: the macro reply path borrows a string literal
/// (`reply_name(&self) -> &str`), so the common case carries the destination without an
/// allocation; a computed name moves in owned. The [`payload`](Self::payload_mut) is a
/// [`BytesMut`]: codec output moves in directly (no copy), and middleware can still mutate it in
/// place (for example wrapping it in an envelope). Middleware may change the name, transform the
/// payload, and enrich the [`headers`](Self::headers_mut) before the message is sent.
#[derive(Debug, Clone)]
pub struct Outgoing<'a> {
    name: Cow<'a, str>,
    payload: BytesMut,
    headers: Headers,
}

impl<'a> Outgoing<'a> {
    /// Creates an outgoing message with no headers.
    ///
    /// Pass a `&str` (a borrowed destination, the no-allocation case) or a `String` (a computed
    /// owned one) for `name`; pass a [`BytesMut`] (codec output moves in) or a `&[u8]` for the
    /// payload.
    #[must_use]
    pub fn new(name: impl Into<Cow<'a, str>>, payload: impl Into<BytesMut>) -> Self {
        Self {
            name: name.into(),
            payload: payload.into(),
            headers: Headers::new(),
        }
    }

    /// The destination name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Sets the destination name.
    pub fn set_name(&mut self, name: impl Into<Cow<'a, str>>) {
        self.name = name.into();
    }

    /// The payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// The payload bytes, mutably (for envelope wrapping).
    pub fn payload_mut(&mut self) -> &mut BytesMut {
        &mut self.payload
    }

    /// Replaces the payload.
    pub fn set_payload(&mut self, payload: impl Into<BytesMut>) {
        self.payload = payload.into();
    }

    /// The outgoing headers.
    #[must_use]
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// The outgoing headers, mutably.
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }
}

/// A static, app-wide publish pipeline: an around-style chain of [`PublishLayer`] ending in
/// the broker send.
///
/// The publish-side analog of the consume-side static [`Stack`](super::Stack) /
/// [`Identity`](super::Identity): the
/// app's publish middleware (added with
/// [`RustStream::publish_layer`](super::RustStream::publish_layer)) compose into a concrete type, so
/// the default path ([`PublishIdentity`], no middleware) is a zero-cost direct send with no `dyn`
/// dispatch. You rarely name this trait; it is built for you. (A runtime-composed escape hatch, the
/// publish counterpart of [`DynStack`](super::DynStack), can be layered in later without changing
/// this contract.)
pub trait PublishPipeline: Send + Sync {
    /// Runs `out` through the remaining middleware, then sends it via `send`.
    fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        send: &'a P,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a;
}

/// The terminal [`PublishPipeline`]: no middleware, just the broker send. The default for an app
/// with no [`publish_layer`](super::RustStream::publish_layer).
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishIdentity;

impl PublishPipeline for PublishIdentity {
    async fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        send: &'a P,
    ) -> Result<(), BoxError> {
        let msg =
            OutgoingMessage::new(out.name(), out.payload()).with_headers(out.headers().clone());
        send.publish(msg).await.map_err(|e| Box::new(e) as BoxError)
    }
}

/// Prepends a [`PublishLayer`] `Head` to a [`PublishPipeline`] `Tail`. Built by
/// [`RustStream::publish_layer`](super::RustStream::publish_layer); you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishStack<Head, Tail> {
    head: Head,
    tail: Tail,
}

impl<Head, Tail> PublishStack<Head, Tail> {
    /// Composes `head` in front of `tail`.
    pub(crate) const fn new(head: Head, tail: Tail) -> Self {
        Self { head, tail }
    }
}

impl<Head: PublishLayer, Tail: PublishPipeline> PublishPipeline for PublishStack<Head, Tail> {
    fn run<'a, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        send: &'a P,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        self.head.on_publish(
            out,
            PublishNext {
                tail: &self.tail,
                send,
            },
        )
    }
}

/// Middleware that transforms (or observes) an [`Outgoing`] message before it is published.
///
/// Each middleware inspects / mutates `out`, then calls [`PublishNext::run`] to continue; the chain
/// ends in the actual broker publish. Static (no `dyn` dispatch): a middleware is generic over the
/// rest of the pipeline `N`, so the whole chain monomorphizes. Added app-wide with
/// [`RustStream::publish_layer`](super::RustStream::publish_layer).
pub trait PublishLayer: Send + Sync {
    /// Handle the outgoing message, calling `next` to continue the pipeline.
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a;
}

/// A cursor over the rest of the publish pipeline, ending in the broker send. Handed to a
/// [`PublishLayer`]; call [`run`](Self::run) to continue.
pub struct PublishNext<'a, N, P> {
    tail: &'a N,
    send: &'a P,
}

impl<'a, N: PublishPipeline, P: Publisher> PublishNext<'a, N, P> {
    /// Runs the rest of the pipeline (the remaining middleware, then the send).
    pub fn run(
        self,
        out: &'a mut Outgoing<'a>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        self.tail.run(out, self.send)
    }
}

impl<N, P> fmt::Debug for PublishNext<'_, N, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishNext").finish_non_exhaustive()
    }
}

/// An object-safe publish middleware, for a [`PublishDynStack`].
///
/// The dynamic counterpart of [`PublishLayer`]: it cannot name the rest of the pipeline as a
/// type parameter (that is what keeps it object-safe and lets a heterogeneous, runtime-built list
/// live in one [`PublishDynStack`]), so it continues through the type-erased [`PublishDynNext`]
/// instead. Use it only when the middleware set is decided at runtime; otherwise a static
/// [`PublishLayer`] is zero-cost.
pub trait PublishDynLayer: Send + Sync {
    /// Handle the outgoing message, calling `next` to continue.
    fn on_publish<'a>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishDynNext<'a>,
    ) -> PublishFut<'a>;
}

/// A cursor over the rest of a [`PublishDynStack`], ending in the surrounding static pipeline.
///
/// Mirrors [`PublishNext`] for the dynamic list: [`run`](Self::run) advances to the next
/// [`PublishDynLayer`], or hands control back to the static chain once the list is exhausted.
pub struct PublishDynNext<'a> {
    rest: &'a [Arc<dyn PublishDynLayer>],
    // The surrounding static `PublishNext::run`, erased so this cursor need not carry its type. A
    // one-shot continuation: `run` is called exactly once per published message.
    tail: Box<dyn FnOnce(&'a mut Outgoing<'a>) -> PublishFut<'a> + Send + 'a>,
}

impl<'a> PublishDynNext<'a> {
    /// Runs the next dynamic middleware, or the surrounding static pipeline if the list is done.
    #[must_use]
    pub fn run(self, out: &'a mut Outgoing<'a>) -> PublishFut<'a> {
        match self.rest.split_first() {
            Some((middleware, rest)) => middleware.on_publish(
                out,
                PublishDynNext {
                    rest,
                    tail: self.tail,
                },
            ),
            None => (self.tail)(out),
        }
    }
}

impl fmt::Debug for PublishDynNext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishDynNext")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// A single static [`PublishLayer`] wrapping a runtime-built, frozen list of
/// [`PublishDynLayer`].
///
/// The publish-side counterpart of the consume-side [`DynStack`](super::DynStack): the opt-in
/// escape hatch for a middleware set decided at runtime (from config, a loop, feature flags) that
/// therefore cannot be a compile-time [`publish_layer`](super::RustStream::publish_layer) chain.
/// Add it like any other middleware; only the middleware inside it pay one boxed future per layer.
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # {
/// use std::sync::Arc;
/// use ruststream::runtime::{PublishDynLayer, PublishDynStack};
///
/// fn stack(
///     middleware: Vec<Arc<dyn PublishDynLayer>>,
/// ) -> PublishDynStack {
///     PublishDynStack::new(middleware)
/// }
/// # }
/// ```
pub struct PublishDynStack(Arc<[Arc<dyn PublishDynLayer>]>);

// Manual `Clone`: the field is an `Arc`, so a clone is a refcount bump regardless of the contents.
impl Clone for PublishDynStack {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl PublishDynStack {
    /// Builds a stack from a list of middleware, applied in iteration order (first runs outermost).
    #[must_use]
    pub fn new(middleware: impl IntoIterator<Item = Arc<dyn PublishDynLayer>>) -> Self {
        Self(middleware.into_iter().collect())
    }
}

impl fmt::Debug for PublishDynStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishDynStack")
            .field("middleware", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl PublishLayer for PublishDynStack {
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        // Erase the static continuation into a one-shot closure so the object-safe walker can end
        // by handing control back to the surrounding static pipeline. Boxing starts here: only
        // the dynamic list pays it.
        PublishDynNext {
            rest: &self.0,
            tail: Box::new(move |out| Box::pin(next.run(out)) as PublishFut<'a>),
        }
        .run(out)
    }
}

/// A read-only view of the originating delivery, handed to a [`PublishTransform`].
///
/// A reply is published from inside a handler, so the static publish transform can read the
/// delivery that produced it: its channel [`name`](Self::name), the incoming
/// [`headers`](Self::headers) (a W3C `traceparent`, a correlation id), and the broker's typed
/// per-delivery [`context`](Self::context) by [`Field`](crate::Field) key. This is how a trace / correlation id
/// propagates from the incoming message onto the reply (the static, zero-cost path; the app-wide
/// [`PublishLayer`] stays context-agnostic). `C` is the
/// handler's context type (`()` when it names none).
pub struct PublishContext<'a, C = ()> {
    name: &'a str,
    headers: &'a Headers,
    cx: &'a C,
}

impl<'a, C> PublishContext<'a, C> {
    /// Builds the view from the parts the runtime already holds at publish time.
    pub(crate) fn new(name: &'a str, headers: &'a Headers, cx: &'a C) -> Self {
        Self { name, headers, cx }
    }

    /// The channel the originating message was delivered on.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name
    }

    /// The originating message's headers (the working copy the handler saw).
    #[must_use]
    pub fn headers(&self) -> &Headers {
        self.headers
    }

    /// Reads a broker-supplied per-delivery field off the typed context by compile-time `key`,
    /// mirroring [`Context::context`](super::Context::context).
    pub fn context<K: crate::Field<C>>(&self, key: K) -> K::Value<'_> {
        key.get(self.cx)
    }
}

impl<C> fmt::Debug for PublishContext<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishContext")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// A static, compile-time publish transform: mutates an [`Outgoing`] before it is sent, with
/// read access to the originating delivery through [`PublishContext`].
///
/// The publish-side counterpart to the consume-side [`Layer`](super::Layer): zero-cost composition,
/// no `dyn` dispatch. Baked onto a [`TypedPublisher`] with [`TypedPublisher::transform`]. Use for
/// per-destination transforms that belong to the publisher itself - a Confluent / Avro envelope, a
/// fixed content-type header, or stamping the delivery's trace / correlation id onto the reply
/// (read it from `cx`). The `C` parameter is the originating handler's context type; a transform
/// that ignores the context is generic over it (mounts on any handler). For cross-cutting
/// *observation* across every publish (metrics), use the app-wide [`PublishLayer`] via
/// [`RustStream::publish_layer`](super::RustStream::publish_layer) instead; the per-publisher
/// transforms run first (closest to the value), then the app-wide publish pipeline, then the send.
pub trait PublishTransform<C = ()>: Send + Sync {
    /// Transforms `out` in place before it is sent, reading the delivery through `cx`.
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>);
}

/// The no-op [`PublishTransform`]: the default for a [`TypedPublisher`] with no static transforms.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishTransformIdentity;

impl<C> PublishTransform<C> for PublishTransformIdentity {
    fn apply(&self, _out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {}
}

/// Composes two [`PublishTransform`]s: `inner` runs first, then `outer`. Built by
/// [`TypedPublisher::transform`]; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishTransformStack<Inner, Outer> {
    inner: Inner,
    outer: Outer,
}

impl<C, Inner: PublishTransform<C>, Outer: PublishTransform<C>> PublishTransform<C>
    for PublishTransformStack<Inner, Outer>
{
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        self.inner.apply(out, cx);
        self.outer.apply(out, cx);
    }
}

/// A static publish transform that runs only on a `#[subscriber(batch(..), publish(..))]` handler's
/// replies, not on single-message replies.
///
/// The batch counterpart of [`PublishTransform`], kept a distinct trait so a transform that belongs to
/// the batch path only (a header marking a reply as batched, a per-batch sampling decision) cannot
/// be added with [`TypedPublisher::transform`] by mistake; it is added with
/// [`TypedPublisher::batch_transform`], which the single-message mounts reject at compile time. The
/// per-message [`PublishTransform`] stack does not run for batched replies and this one does not run for
/// single-message replies - the two paths are independent. To use the same transform on both, add
/// it to each, reusing it on the batch side with [`for_batch`] (no second implementation). Each
/// reply in the batch is passed through it individually, reading the delivery through
/// [`PublishContext`].
pub trait BatchPublishTransform<C = ()>: Send + Sync {
    /// Transforms one of the batch's outgoing replies before it is sent.
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>);
}

/// The no-op [`BatchPublishTransform`]: the default for a [`TypedPublisher`] with no batch transforms.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchTransformIdentity;

impl<C> BatchPublishTransform<C> for BatchTransformIdentity {
    fn apply(&self, _out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {}
}

/// Composes two [`BatchPublishTransform`]s: `inner` runs first, then `outer`. Built by
/// [`TypedPublisher::batch_transform`]; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchPublishTransformStack<Inner, Outer> {
    inner: Inner,
    outer: Outer,
}

impl<C, Inner: BatchPublishTransform<C>, Outer: BatchPublishTransform<C>> BatchPublishTransform<C>
    for BatchPublishTransformStack<Inner, Outer>
{
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        self.inner.apply(out, cx);
        self.outer.apply(out, cx);
    }
}

/// Adapts a per-message [`PublishTransform`] into a [`BatchPublishTransform`], applying it to each reply of
/// a batch. Built by [`for_batch`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ForBatch<L>(L);

impl<C, L: PublishTransform<C>> BatchPublishTransform<C> for ForBatch<L> {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        self.0.apply(out, cx);
    }
}

/// Lifts a per-message [`PublishTransform`] onto the batch path so the same transform can be added with
/// [`TypedPublisher::batch_transform`] without a second implementation.
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{for_batch, Outgoing, PublishContext, PublishTransform, TypedPublisher};
///
/// struct Stamp;
/// impl<C> PublishTransform<C> for Stamp {
///     fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
///         out.headers_mut().insert("x-stamp", b"1".to_vec());
///     }
/// }
///
/// let broker = MemoryBroker::new();
/// // The same `Stamp` on both paths: per message, and batched.
/// let publisher = TypedPublisher::new(broker.publisher())
///     .transform(Stamp)
///     .batch_transform(for_batch(Stamp));
/// # let _ = publisher;
/// # }
/// ```
#[must_use]
pub fn for_batch<L>(transform: L) -> ForBatch<L> {
    ForBatch(transform)
}

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
    publisher: P,
    codec: C,
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
    inner: TypedPublisher<P, C, PL, BL>,
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

mod sealed {
    /// Seals [`ReplyPublisher`](super::ReplyPublisher): the reply-publishing strategies are the
    /// two wirings above, not an extension point.
    pub trait Sealed {}

    impl<P, C, PL, BL> Sealed for super::TypedPublisher<P, C, PL, BL> {}
    impl<P, C, PL, BL> Sealed for super::Transactional<P, C, PL, BL> {}
}

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

/// The reply wiring accepted by the `include_batch_publishing` mounts.
///
/// Implemented by a plain [`TypedPublisher`] (each reply published independently) and by a
/// [`Transactional`] one (all replies of a batch inside one transaction). Sealed: implemented by
/// exactly those two types. `Cx` is the originating batch handler's context type, threaded so the
/// static [`PublishTransform`] reads the delivery while publishing each reply.
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
    /// [`TypedTransaction`].
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

/// A live broker transaction, opened by [`Transactional::begin`].
///
/// Publishes issued through the scope become visible together on [`commit`](Self::commit), or
/// not at all after [`abort`](Self::abort); both consume the scope, so a double commit or a
/// publish after settling is a compile error. This is the manual counterpart of the per-batch
/// transaction the runtime drives for `include_batch_publishing` mounts.
///
/// The scope encodes values with the wrapper's codec and sends them directly: the reply
/// [`PublishTransform`] stack and the app-wide [`publish_layer`](super::RustStream::publish_layer)
/// middleware do not run here - both belong to the dispatch path, where a delivery context
/// exists.
///
/// Dropping an unsettled scope logs a warning and leaves the broker transaction open (destructors
/// cannot run async work); always settle explicitly.
#[must_use = "a transaction scope must be settled with commit() or abort()"]
pub struct TransactionScope<'a, P, C> {
    publisher: &'a P,
    codec: &'a C,
    open: bool,
}

impl<P, C> TransactionScope<'_, P, C>
where
    P: TransactionalPublisher,
    C: Codec,
{
    /// Encodes `value` with the wrapper's codec and publishes it to `name` inside the
    /// transaction.
    ///
    /// A failed publish does not settle the scope: the caller decides between retrying and
    /// [`abort`](Self::abort). Aborting is the safe default - after an error the broker-side
    /// transaction state is implementation-defined.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionPublishError::Encode`] when the codec rejects the value, and
    /// [`TransactionPublishError::Publish`] when the broker rejects the message.
    ///
    /// # Cancel safety
    ///
    /// As with [`Publisher::publish`], cancel safety is broker-defined: dropping the future
    /// mid-flight may leave the message in an indeterminate state inside the transaction.
    pub async fn publish<T: Serialize + Sync>(
        &mut self,
        name: &str,
        value: &T,
    ) -> Result<(), TransactionPublishError<P::Error>> {
        let payload = self
            .codec
            .encode(value)
            .map_err(TransactionPublishError::Encode)?;
        self.publisher
            .publish(OutgoingMessage::new(name, &payload))
            .await
            .map_err(TransactionPublishError::Publish)
    }

    /// Commits the transaction: every publish issued through the scope becomes visible at once.
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker rejects the commit. Per the
    /// [`TransactionalPublisher`] contract a failed commit closes the transaction, so the spent
    /// scope leaves the handle free for a fresh [`begin`](Transactional::begin).
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe: dropping the future mid-commit leaves the broker transaction in an
    /// implementation-defined state. The scope treats itself as unsettled then - its drop
    /// warning fires - which is why `open` clears only after the broker call completes.
    pub async fn commit(mut self) -> Result<(), P::Error> {
        let result = self.publisher.commit().await;
        self.open = false;
        result
    }

    /// Aborts the transaction: nothing published through the scope becomes visible.
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker fails to abort.
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe, exactly like [`commit`](Self::commit): a future dropped mid-abort
    /// leaves the scope unsettled and its drop warning fires.
    pub async fn abort(mut self) -> Result<(), P::Error> {
        let result = self.publisher.abort().await;
        self.open = false;
        result
    }
}

impl<P, C> Drop for TransactionScope<'_, P, C> {
    fn drop(&mut self) {
        if self.open {
            warn!(
                target: "ruststream::dispatch",
                "transaction scope dropped without commit or abort; the broker transaction stays \
                 open on this publisher handle until it is settled or the handle is dropped"
            );
        }
    }
}

impl<P, C> fmt::Debug for TransactionScope<'_, P, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionScope")
            .field("open", &self.open)
            .finish_non_exhaustive()
    }
}

impl<P, C, PL, BL> TypedPublisher<P, C, PL, BL>
where
    P: OwnedTransactions,
    C: Sync,
    PL: Sync,
    BL: Sync,
{
    /// Opens an owned broker transaction and returns the [`TypedTransaction`] that owns it.
    ///
    /// This is the typed sugar over the owned transaction kind ([`OwnedTransactions`]), the
    /// counterpart of the borrowed [`Transactional::begin`]: every call opens its own
    /// independent transaction and the returned value owns its buffer, so any number can be
    /// open on one publisher at a time - settling one never touches another. Kafka-like
    /// brokers, whose client holds exactly one transaction per producer, offer only the
    /// borrowed kind.
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::TypedPublisher;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MemoryBroker::new();
    /// let publisher = TypedPublisher::new(broker.publisher());
    ///
    /// let mut orders = publisher.transaction().await?;
    /// let mut audit = publisher.transaction().await?; // concurrent with `orders`
    /// orders.publish("orders.settled", &42_u32).await?;
    /// audit.publish("audit.trail", &7_u32).await?;
    /// orders.commit().await?;
    /// audit.commit().await?;
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to open a transaction; pure
    /// client-buffer implementations are infallible in practice.
    pub async fn transaction(&self) -> Result<TypedTransaction<'_, P::Transaction, C>, P::Error> {
        Ok(TypedTransaction {
            txn: self.publisher.transaction().await?,
            codec: &self.codec,
        })
    }
}

/// An owned broker transaction carrying the publisher's codec, opened by
/// [`TypedPublisher::transaction`].
///
/// The owned counterpart of [`TransactionScope`]: the scope borrows the handle's single
/// broker-side transaction, while this value owns an independent one
/// ([`OwnedTransactions::Transaction`]), so any number can be open on one publisher and driven
/// concurrently. Publishes encode with the publisher's codec and buffer into the transaction;
/// the whole buffer becomes visible atomically on [`commit`](Self::commit) and is discarded by
/// [`abort`](Self::abort), both of which consume the value, so a double commit or a publish
/// after settling is a compile error.
///
/// Like the scope, it encodes values and sends them directly: the reply [`PublishTransform`]
/// stack and the app-wide [`publish_layer`](super::RustStream::publish_layer) middleware belong
/// to the dispatch path, where a delivery context exists, and do not run here.
///
/// Dropping an unsettled value discards the client buffer like an abort - unlike the scope, no
/// broker-side transaction is left open on the handle. The missed-settle warning comes from the
/// underlying [`Transaction`] value's own drop (per its contract), so this wrapper does not add
/// a second one.
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct TypedTransaction<'a, Txn, C> {
    txn: Txn,
    codec: &'a C,
}

impl<Txn, C> TypedTransaction<'_, Txn, C>
where
    Txn: Transaction,
    C: Codec,
{
    /// Encodes `value` with the publisher's codec and publishes it into the transaction:
    /// buffered, not visible before [`commit`](Self::commit).
    ///
    /// A failed publish does not settle the transaction; the caller decides between retrying
    /// and [`abort`](Self::abort).
    ///
    /// # Errors
    ///
    /// Returns [`TransactionPublishError::Encode`] when the codec rejects the value, and
    /// [`TransactionPublishError::Publish`] when the message cannot be buffered (a pure client
    /// buffer is infallible in practice).
    ///
    /// # Cancel safety
    ///
    /// As with [`Transaction::publish`], cancel safety is implementation-defined: dropping the
    /// future mid-flight may leave the message in an indeterminate state inside the
    /// transaction.
    pub async fn publish<T>(
        &mut self,
        name: &str,
        value: &T,
    ) -> Result<(), TransactionPublishError<Txn::Error>>
    where
        T: Serialize + Sync,
    {
        let payload = self
            .codec
            .encode(value)
            .map_err(TransactionPublishError::Encode)?;
        self.txn
            .publish(OutgoingMessage::new(name, &payload))
            .await
            .map_err(TransactionPublishError::Publish)
    }

    /// Commits the transaction: the whole buffer becomes visible atomically, in publish order.
    ///
    /// # Errors
    ///
    /// Returns the transaction's error when the flush fails. A failed commit has still consumed
    /// the transaction and its buffer is lost; redelivery of the inputs, not resubmission of the
    /// buffer, is the recovery path (the [`Transaction::commit`] contract).
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe: the future owns the transaction, so dropping it mid-commit discards
    /// what is left of the buffer with the flush incomplete - which messages became visible is
    /// implementation-defined, as for a [`TransactionScope::commit`] dropped mid-flight.
    pub async fn commit(self) -> Result<(), Txn::Error> {
        self.txn.commit().await
    }

    /// Aborts the transaction, discarding the buffer.
    ///
    /// # Errors
    ///
    /// Returns the transaction's error when staged broker-side state cannot be discarded; for a
    /// pure client buffer this is infallible in practice.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future mid-abort still discards the buffer - the future owns the
    /// transaction, and an unsettled drop is itself the discard (the underlying value may log
    /// its missed-settle warning).
    pub async fn abort(self) -> Result<(), Txn::Error> {
        self.txn.abort().await
    }
}

impl<Txn, C> fmt::Debug for TypedTransaction<'_, Txn, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedTransaction").finish_non_exhaustive()
    }
}

/// Error returned by [`TransactionScope::publish`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransactionPublishError<E> {
    /// The codec rejected the value.
    #[error("failed to encode the value for a transactional publish")]
    Encode(#[source] CodecError),
    /// The broker rejected the message.
    #[error("failed to publish inside the transaction")]
    Publish(#[source] E),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixtures the in-memory broker cannot express: a value the codec cannot encode, a
    /// transactional publisher rigged to fail one step of the protocol, and a policy that
    /// refuses to pair.
    #[cfg(feature = "json")]
    mod fixtures {
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use thiserror::Error;

        #[cfg(feature = "memory")]
        use crate::memory::{ConnectedMemoryBroker, MemoryTransaction};
        use crate::{OutgoingMessage, Publisher, TransactionalPublisher};
        #[cfg(feature = "memory")]
        use crate::{OwnedTransactions, PairError, PublishPolicy};

        /// A value JSON cannot encode: an object key must be a string, and this map's are
        /// tuples. Encoding one is a real codec failure, not a stubbed one.
        pub(super) fn unencodable() -> HashMap<(u8, u8), u8> {
            HashMap::from([((1, 2), 3)])
        }

        /// The failure the rigged publisher reports.
        #[derive(Debug, Error)]
        #[error("the rigged publisher refused")]
        pub(super) struct RiggedError;

        /// A transactional publisher whose protocol steps fail on demand: the memory broker's
        /// transactions always succeed, so the batch path's error handling needs this.
        #[derive(Debug, Default)]
        pub(super) struct Rigged {
            pub(super) fail_begin: bool,
            pub(super) fail_commit: bool,
            pub(super) fail_abort: bool,
            pub(super) published: AtomicUsize,
            pub(super) aborted: AtomicUsize,
        }

        impl Rigged {
            /// One protocol step, rigged or not.
            fn step(fail: bool) -> Result<(), RiggedError> {
                if fail { Err(RiggedError) } else { Ok(()) }
            }
        }

        impl Publisher for Rigged {
            type Error = RiggedError;

            async fn publish(&self, _msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
                self.published.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        impl TransactionalPublisher for Rigged {
            async fn begin_transaction(&self) -> Result<(), Self::Error> {
                Self::step(self.fail_begin)
            }

            async fn commit(&self) -> Result<(), Self::Error> {
                Self::step(self.fail_commit)
            }

            async fn abort(&self) -> Result<(), Self::Error> {
                self.aborted.fetch_add(1, Ordering::SeqCst);
                Self::step(self.fail_abort)
            }
        }

        // The publisher a broker hands out is opened per call; refusing to open one is how a
        // broker reports that the handle is unusable.
        #[cfg(feature = "memory")]
        impl OwnedTransactions for Rigged {
            type Transaction = MemoryTransaction;

            async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
                Err(RiggedError)
            }
        }

        /// A policy that fails to pair, standing in for a broker whose publisher needs real work
        /// to come alive (a transactional producer initializing).
        #[cfg(feature = "memory")]
        pub(super) struct RefusePairing;

        #[cfg(feature = "memory")]
        impl PublishPolicy<ConnectedMemoryBroker> for RefusePairing {
            type Live = Rigged;

            async fn pair(
                self,
                _connected: &ConnectedMemoryBroker,
            ) -> Result<Self::Live, PairError> {
                Err(PairError::from_boxed(Box::from(
                    "the policy refused to pair",
                )))
            }
        }
    }

    /// Collects the `tracing` messages emitted on this thread: a warning's field values are only
    /// evaluated while a subscriber listens, so asserting on one needs a capture in place.
    #[cfg(feature = "logging")]
    fn capture_events() -> (
        Arc<std::sync::Mutex<Vec<String>>>,
        tracing::subscriber::DefaultGuard,
    ) {
        use std::sync::Mutex;

        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};

        struct Capture(Arc<Mutex<Vec<String>>>);

        impl<S: tracing::Subscriber> Layer<S> for Capture {
            fn on_event(&self, event: &tracing::Event<'_>, _ctx: LayerContext<'_, S>) {
                struct Grab(Vec<String>);
                impl tracing::field::Visit for Grab {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn fmt::Debug,
                    ) {
                        self.0.push(format!("{}={value:?}", field.name()));
                    }
                }
                let mut grab = Grab(Vec::new());
                event.record(&mut grab);
                self.0.lock().unwrap().push(grab.0.join(" "));
            }
        }

        let events = Arc::new(Mutex::new(Vec::new()));
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&events))),
        );
        (events, guard)
    }

    // A cancelled commit leaves the broker transaction genuinely unsettled, so the scope's
    // drop warning must still fire (needs a tracing subscriber, hence the `logging` gate; the
    // stub's pending commit is the cancellation window the single-poll memory broker lacks).
    #[cfg(all(feature = "json", feature = "logging"))]
    #[tokio::test]
    async fn cancelled_commit_keeps_the_unsettled_drop_warning() {
        use crate::{OutgoingMessage, Publisher, TransactionalPublisher};

        struct PendingCommit;

        impl Publisher for PendingCommit {
            type Error = std::convert::Infallible;

            async fn publish(&self, _msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
                Ok(())
            }
        }

        impl TransactionalPublisher for PendingCommit {
            async fn begin_transaction(&self) -> Result<(), Self::Error> {
                Ok(())
            }

            async fn commit(&self) -> Result<(), Self::Error> {
                std::future::pending().await
            }

            async fn abort(&self) -> Result<(), Self::Error> {
                Ok(())
            }
        }

        let (events, guard) = capture_events();

        let wrapper = TypedPublisher::new(PendingCommit).transactional();
        let scope = wrapper.begin().await.expect("begin failed");
        {
            let mut commit = std::pin::pin!(scope.commit());
            assert!(
                futures::poll!(commit.as_mut()).is_pending(),
                "the stub commit must hold the cancellation window open",
            );
        }
        drop(guard);

        let warned = {
            let captured = events.lock().unwrap();
            captured
                .iter()
                .any(|message| message.contains("transaction scope dropped without commit"))
        };
        assert!(
            warned,
            "a commit cancelled mid-flight leaves the broker transaction unsettled and must \
             keep the drop warning",
        );
    }

    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dyn_stack_walks_its_layers_then_the_static_tail() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures::StreamExt;

        use crate::codec::JsonCodec;
        use crate::memory::MemoryBroker;
        use crate::{IncomingMessage, Subscriber};

        /// Adds its weight on every pass, proving order and that each layer ran exactly once.
        struct Mark(Arc<AtomicUsize>, usize);
        impl PublishDynLayer for Mark {
            fn on_publish<'a>(
                &'a self,
                out: &'a mut Outgoing<'a>,
                next: PublishDynNext<'a>,
            ) -> PublishFut<'a> {
                self.0.fetch_add(self.1, Ordering::SeqCst);
                next.run(out)
            }
        }

        let hits = Arc::new(AtomicUsize::new(0));
        let stack = PublishDynStack::new([
            Arc::new(Mark(Arc::clone(&hits), 1)) as Arc<dyn PublishDynLayer>,
            Arc::new(Mark(Arc::clone(&hits), 10)),
        ]);
        assert!(format!("{stack:?}").contains("middleware"));
        let pipeline = PublishStack::new(stack.clone(), PublishIdentity);

        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("dyn");
        let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
        let headers = Headers::new();
        let cx = PublishContext::new("dyn", &headers, &());
        publisher
            .publish("dyn", &5_u32, &pipeline, &cx)
            .await
            .expect("publish through the dynamic stack failed");

        assert_eq!(hits.load(Ordering::SeqCst), 11, "each layer must run once");
        let mut stream = std::pin::pin!(subscriber.stream());
        let msg = stream
            .next()
            .await
            .expect("delivery missing")
            .expect("memory subscriber never errors");
        assert_eq!(msg.payload(), b"5", "the static tail must still send");
        msg.ack().await.expect("ack failed");
    }

    #[test]
    fn borrowed_name_is_not_owned() {
        // The macro-reply hot path passes a string literal: it must stay borrowed (no alloc),
        // which is the whole point of the Cow.
        let out = Outgoing::new("orders.created", b"payload".as_slice());
        assert!(matches!(out.name, Cow::Borrowed(_)));
        assert_eq!(out.name(), "orders.created");
        assert_eq!(out.payload(), b"payload");
    }

    #[test]
    fn owned_name_moves_in() {
        let computed = format!("orders.{}", 42);
        let out = Outgoing::new(computed, BytesMut::from(&b"x"[..]));
        assert!(matches!(out.name, Cow::Owned(_)));
        assert_eq!(out.name(), "orders.42");
    }

    #[test]
    fn payload_mutates_in_place() {
        let mut out = Outgoing::new("t", BytesMut::from(&b"body"[..]));
        out.payload_mut().extend_from_slice(b"!");
        assert_eq!(out.payload(), b"body!");

        out.set_payload(b"fresh".as_slice());
        assert_eq!(out.payload(), b"fresh");
    }

    #[test]
    fn set_name_and_headers() {
        let mut out = Outgoing::new("a", b"".as_slice());
        out.set_name("b");
        out.headers_mut().insert("k", "v");
        assert_eq!(out.name(), "b");
        assert_eq!(out.headers().get_str("k"), Some("v"));
    }

    /// Both cursors report where in the chain a middleware sits: the static one is opaque, the
    /// dynamic one counts the middleware still ahead of it.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test]
    async fn the_pipeline_cursors_render_their_position() {
        use std::sync::Mutex;

        use crate::codec::JsonCodec;
        use crate::memory::MemoryBroker;

        /// Records how the cursor it is handed renders, then continues the chain.
        struct Record(Arc<Mutex<Vec<String>>>);

        impl PublishLayer for Record {
            fn on_publish<'a, N: PublishPipeline, P: Publisher>(
                &'a self,
                out: &'a mut Outgoing<'a>,
                next: PublishNext<'a, N, P>,
            ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
                self.0.lock().unwrap().push(format!("{next:?}"));
                next.run(out)
            }
        }

        impl PublishDynLayer for Record {
            fn on_publish<'a>(
                &'a self,
                out: &'a mut Outgoing<'a>,
                next: PublishDynNext<'a>,
            ) -> PublishFut<'a> {
                self.0.lock().unwrap().push(format!("{next:?}"));
                next.run(out)
            }
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let dynamic = PublishDynStack::new([
            Arc::new(Record(Arc::clone(&seen))) as Arc<dyn PublishDynLayer>,
            Arc::new(Record(Arc::clone(&seen))),
        ]);
        let pipeline = PublishStack::new(
            Record(Arc::clone(&seen)),
            PublishStack::new(dynamic, PublishIdentity),
        );

        let broker = MemoryBroker::new();
        let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
        let headers = Headers::new();
        let cx = PublishContext::new("cursors", &headers, &());
        publisher
            .publish("cursors", &1_u32, &pipeline, &cx)
            .await
            .expect("publish through the recorded pipeline failed");

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen.len(),
            3,
            "every middleware must see a cursor: {seen:?}"
        );
        assert!(
            seen[0].starts_with("PublishNext"),
            "the static cursor names itself: {}",
            seen[0],
        );
        assert!(
            seen[1].contains("remaining: 1"),
            "the first dynamic middleware still has one ahead of it: {}",
            seen[1],
        );
        assert!(
            seen[2].contains("remaining: 0"),
            "the last dynamic middleware hands back to the static tail: {}",
            seen[2],
        );
    }

    /// The view a static transform gets: the delivery's channel and headers, plus a Debug form
    /// naming the channel (what a user sees when they log the context).
    #[test]
    fn publish_context_reads_the_originating_delivery() {
        let mut headers = Headers::new();
        headers.insert("correlation-id", "abc");
        let cx = PublishContext::new("orders.created", &headers, &());

        assert_eq!(cx.name(), "orders.created");
        assert_eq!(cx.headers().get_str("correlation-id"), Some("abc"));
        assert!(
            format!("{cx:?}").contains("orders.created"),
            "the delivery channel is what identifies the context in a log line",
        );
    }

    /// A codec failure is reported by the publish paths instead of reaching the broker, on both
    /// the single-message and the batch reply route.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test]
    async fn an_unencodable_reply_stops_both_reply_paths() {
        use fixtures::unencodable;
        use futures::StreamExt;

        use crate::Subscriber;
        use crate::codec::JsonCodec;
        use crate::memory::MemoryBroker;

        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("out");
        let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
        let headers = Headers::new();
        let cx = PublishContext::new("in", &headers, &());

        let single = publisher
            .publish("out", &unencodable(), &PublishIdentity, &cx)
            .await
            .expect_err("the codec cannot encode this reply");
        assert!(
            single.to_string().contains("encode failed"),
            "the codec error must reach the caller: {single}",
        );

        let batched = publisher
            .publish_batch(
                "out",
                &[unencodable(), unencodable()],
                &PublishIdentity,
                &cx,
            )
            .await
            .expect_err("the batch path encodes per reply");
        assert!(
            batched.to_string().contains("encode failed"),
            "the codec error must reach the caller: {batched}",
        );

        let mut stream = std::pin::pin!(subscriber.stream());
        assert!(
            futures::poll!(stream.next()).is_pending(),
            "nothing may reach the broker when encoding fails",
        );
    }

    /// A plain typed publisher sends the batch's replies one by one, and exposes the codec the
    /// batch mounts reuse for decoding.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test]
    async fn a_plain_wiring_publishes_each_reply_and_names_its_codec() {
        use futures::StreamExt;

        use crate::codec::JsonCodec;
        use crate::memory::MemoryBroker;
        use crate::{IncomingMessage, Subscriber};

        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("out");
        let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
        let headers = Headers::new();
        let cx = PublishContext::new("in", &headers, &());

        publisher
            .publish_batch("out", &[1_u32, 2, 3], &PublishIdentity, &cx)
            .await
            .expect("publishing a batch of replies failed");

        let mut stream = std::pin::pin!(subscriber.stream());
        let mut sent = Vec::new();
        for _ in 0..3 {
            let msg = stream
                .next()
                .await
                .expect("delivery missing")
                .expect("memory subscriber never errors");
            sent.push(msg.payload().to_vec());
            msg.ack().await.expect("ack failed");
        }
        assert_eq!(
            sent,
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
            "each reply is published independently, in order",
        );

        let decoded: u32 = ReplyPublisher::<()>::reply_codec(&publisher)
            .decode(b"7")
            .expect("the reply codec decodes what it encodes");
        assert_eq!(decoded, 7);
    }

    /// The transactional wiring reports the same reply codec as the stack it wraps: the batch
    /// mounts read it off either shape to decode the incoming batch.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[test]
    fn the_transactional_wiring_reports_the_reply_codec() {
        use crate::codec::{Codec, JsonCodec};
        use crate::memory::MemoryBroker;

        let broker = MemoryBroker::new();
        let wiring = TypedPublisher::with_codec(broker.publisher(), JsonCodec).transactional();

        let from_wiring: u32 = ReplyWiring::decode_codec(&wiring)
            .decode(b"7")
            .expect("decode through the wiring codec");
        let from_reply: u32 = ReplyPublisher::<()>::reply_codec(&wiring)
            .decode(b"7")
            .expect("decode through the reply codec");
        assert_eq!((from_wiring, from_reply), (7, 7));
    }

    /// The wiring stacks hide their leaf and codec from Debug: a publisher is not a data type,
    /// and its connection must not print.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[test]
    fn the_wiring_stacks_render_without_their_leaf() {
        use crate::codec::JsonCodec;
        use crate::memory::MemoryPublish;

        let typed = TypedPublisher::with_codec(MemoryPublish, JsonCodec);
        assert_eq!(format!("{typed:?}"), "TypedPublisher { .. }");
        assert_eq!(
            format!("{:?}", typed.transactional()),
            "Transactional { .. }",
        );
    }

    /// A typed publisher over a policy leaf is itself a policy: pairing swaps the leaf for its
    /// live form and the codec travels along, so the paired stack publishes.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test]
    async fn a_typed_publisher_over_a_policy_pairs_into_its_live_form() {
        use futures::StreamExt;

        use crate::codec::JsonCodec;
        use crate::memory::{MemoryBroker, MemoryPublish};
        use crate::{Broker, IncomingMessage, Subscriber};

        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("paired");
        let connected = broker.clone().connect().await.expect("connect failed");

        let live = TypedPublisher::with_codec(MemoryPublish, JsonCodec)
            .pair(&connected)
            .await
            .expect("pairing a typed publisher over a policy failed");

        let headers = Headers::new();
        let cx = PublishContext::new("in", &headers, &());
        live.publish("paired", &9_u32, &PublishIdentity, &cx)
            .await
            .expect("the paired stack must publish");

        let mut stream = std::pin::pin!(subscriber.stream());
        let msg = stream
            .next()
            .await
            .expect("delivery missing")
            .expect("memory subscriber never errors");
        assert_eq!(msg.payload(), b"9");
        msg.ack().await.expect("ack failed");
    }

    /// The transactional wiring pairs like the plain one, and its scope holds the publishes back
    /// until the commit.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test]
    async fn a_paired_transactional_wiring_scopes_its_publishes() {
        use futures::StreamExt;

        use crate::codec::JsonCodec;
        use crate::memory::{MemoryBroker, MemoryPublish};
        use crate::{Broker, IncomingMessage, Subscriber};

        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("scoped");
        let connected = broker.clone().connect().await.expect("connect failed");

        let live = TypedPublisher::with_codec(MemoryPublish, JsonCodec)
            .transactional()
            .pair(&connected)
            .await
            .expect("pairing a transactional wiring failed");

        let mut scope = live.begin().await.expect("begin failed");
        scope
            .publish("scoped", &4_u32)
            .await
            .expect("publishing inside the scope failed");
        assert!(
            format!("{scope:?}").contains("open: true"),
            "an unsettled scope reports itself open",
        );

        let mut stream = std::pin::pin!(subscriber.stream());
        assert!(
            futures::poll!(stream.next()).is_pending(),
            "a scoped publish stays invisible until the commit",
        );
        scope.commit().await.expect("commit failed");

        let msg = stream
            .next()
            .await
            .expect("delivery missing")
            .expect("memory subscriber never errors");
        assert_eq!(msg.payload(), b"4");
        msg.ack().await.expect("ack failed");
    }

    /// A broker that refuses to open the transaction fails the batch before any reply is sent.
    #[cfg(feature = "json")]
    #[tokio::test]
    async fn a_refused_begin_fails_the_batch_before_the_first_reply() {
        use std::sync::atomic::Ordering;

        use fixtures::Rigged;

        use crate::codec::JsonCodec;

        let wiring = TypedPublisher::with_codec(
            Rigged {
                fail_begin: true,
                ..Rigged::default()
            },
            JsonCodec,
        )
        .transactional();
        let headers = Headers::new();
        let cx = PublishContext::new("in", &headers, &());

        let err = wiring
            .publish_batch("out", &[1_u32, 2], &PublishIdentity, &cx)
            .await
            .expect_err("a refused begin fails the batch");
        assert!(err.to_string().contains("rigged"), "reported: {err}");
        assert_eq!(
            wiring.inner.publisher.published.load(Ordering::SeqCst),
            0,
            "no reply may be sent without an open transaction",
        );
    }

    /// A reply that cannot be produced aborts the whole batch, so no half-published batch stays
    /// visible.
    #[cfg(all(feature = "json", feature = "memory"))]
    #[tokio::test]
    async fn a_failed_reply_aborts_the_whole_batch() {
        use std::sync::atomic::Ordering;

        use fixtures::{Rigged, unencodable};

        use crate::codec::JsonCodec;

        let wiring = TypedPublisher::with_codec(Rigged::default(), JsonCodec).transactional();
        let headers = Headers::new();
        let cx = PublishContext::new("in", &headers, &());

        let err = wiring
            .publish_batch(
                "out",
                &[unencodable(), unencodable()],
                &PublishIdentity,
                &cx,
            )
            .await
            .expect_err("a reply that cannot be encoded fails the batch");

        assert!(
            err.to_string().contains("encode failed"),
            "the caller acts on the failure that broke the batch: {err}",
        );
        assert_eq!(
            wiring.inner.publisher.aborted.load(Ordering::SeqCst),
            1,
            "the open transaction must be aborted exactly once",
        );
    }

    /// When the abort itself fails, the original error still travels and the abort failure is
    /// only logged: the caller acts on the failure that broke the batch.
    #[cfg(all(feature = "json", feature = "memory", feature = "logging"))]
    #[tokio::test]
    async fn a_failed_abort_is_logged_rather_than_propagated() {
        use std::sync::atomic::Ordering;

        use fixtures::{Rigged, unencodable};

        use crate::codec::JsonCodec;

        let (events, guard) = capture_events();

        let wiring = TypedPublisher::with_codec(
            Rigged {
                fail_abort: true,
                ..Rigged::default()
            },
            JsonCodec,
        )
        .transactional();
        let headers = Headers::new();
        let cx = PublishContext::new("in", &headers, &());

        let err = wiring
            .publish_batch("out", &[unencodable()], &PublishIdentity, &cx)
            .await
            .expect_err("a reply that cannot be encoded fails the batch");
        drop(guard);

        assert!(
            err.to_string().contains("encode failed"),
            "the caller acts on the original failure, not on the abort: {err}",
        );
        assert_eq!(
            wiring.inner.publisher.aborted.load(Ordering::SeqCst),
            1,
            "the open transaction must be aborted exactly once",
        );
        let warned = {
            let captured = events.lock().unwrap();
            captured
                .iter()
                .any(|event| event.contains("transaction abort failed"))
        };
        assert!(warned, "a failed abort is logged, never propagated");
    }

    /// A commit the broker rejects fails the batch, so the incoming batch is retried rather
    /// than acked over half-published replies.
    #[cfg(feature = "json")]
    #[tokio::test]
    async fn a_refused_commit_fails_the_batch() {
        use std::sync::atomic::Ordering;

        use fixtures::Rigged;

        use crate::codec::JsonCodec;

        let wiring = TypedPublisher::with_codec(
            Rigged {
                fail_commit: true,
                ..Rigged::default()
            },
            JsonCodec,
        )
        .transactional();
        let headers = Headers::new();
        let cx = PublishContext::new("in", &headers, &());

        let err = wiring
            .publish_batch("out", &[1_u32, 2], &PublishIdentity, &cx)
            .await
            .expect_err("a refused commit fails the batch");
        assert!(err.to_string().contains("rigged"), "reported: {err}");
        assert_eq!(
            wiring.inner.publisher.published.load(Ordering::SeqCst),
            2,
            "the replies were sent; only the commit failed",
        );
        assert_eq!(
            wiring.inner.publisher.aborted.load(Ordering::SeqCst),
            0,
            "a failed commit closes the transaction, so there is nothing to abort",
        );
    }

    /// The scope's publish surfaces the encode failure as its own variant, and does not settle:
    /// the caller still chooses between a retry and an abort.
    #[cfg(feature = "json")]
    #[tokio::test]
    async fn a_scoped_publish_reports_an_encode_failure_without_settling() {
        use fixtures::{Rigged, unencodable};

        use crate::codec::JsonCodec;

        let wiring = TypedPublisher::with_codec(Rigged::default(), JsonCodec).transactional();
        let mut scope = wiring.begin().await.expect("begin failed");

        let err = scope
            .publish("out", &unencodable())
            .await
            .expect_err("the codec cannot encode this value");
        assert!(
            matches!(err, TransactionPublishError::Encode(_)),
            "the encode arm must be distinguishable from a broker rejection: {err:?}",
        );
        assert_eq!(
            err.to_string(),
            "failed to encode the value for a transactional publish",
        );
        scope.abort().await.expect("abort failed");
    }

    /// An owned transaction encodes with the publisher's codec, keeps its buffer private, and
    /// discards it on abort.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test]
    async fn an_owned_transaction_encodes_and_discards_on_abort() {
        use fixtures::unencodable;
        use futures::StreamExt;

        use crate::Subscriber;
        use crate::codec::JsonCodec;
        use crate::memory::MemoryBroker;

        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("owned");
        let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);

        let mut txn = publisher.transaction().await.expect("open failed");
        assert_eq!(format!("{txn:?}"), "TypedTransaction { .. }");
        txn.publish("owned", &3_u32).await.expect("publish failed");
        txn.abort().await.expect("abort failed");

        let mut stream = std::pin::pin!(subscriber.stream());
        assert!(
            futures::poll!(stream.next()).is_pending(),
            "an aborted buffer never reaches the bus",
        );

        let mut txn = publisher.transaction().await.expect("open failed");
        let err = txn
            .publish("owned", &unencodable())
            .await
            .expect_err("the codec cannot encode this value");
        assert!(
            matches!(err, TransactionPublishError::Encode(_)),
            "the encode arm must be distinguishable from a broker rejection: {err:?}",
        );
        txn.abort().await.expect("abort failed");
    }

    /// A policy that fails to pair fails the whole wiring stack: neither shape hands out a
    /// half-built publisher, and the error travels to the startup that asked for it.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test]
    async fn a_refused_pairing_fails_the_whole_wiring() {
        use fixtures::RefusePairing;

        use crate::Broker;
        use crate::codec::JsonCodec;
        use crate::memory::MemoryBroker;

        let connected = MemoryBroker::new().connect().await.expect("connect failed");

        let plain = TypedPublisher::with_codec(RefusePairing, JsonCodec)
            .pair(&connected)
            .await
            .expect_err("the policy refuses to pair");
        assert!(
            plain.to_string().contains("refused to pair"),
            "the policy's reason must survive the stack: {plain}",
        );

        let transactional = TypedPublisher::with_codec(RefusePairing, JsonCodec)
            .transactional()
            .pair(&connected)
            .await
            .expect_err("the policy refuses to pair");
        assert!(
            transactional.to_string().contains("refused to pair"),
            "the transactional wrapper must not swallow it: {transactional}",
        );
    }

    /// A broker that refuses to open the handle's transaction surfaces its error from `begin`,
    /// so no scope exists over a transaction the broker does not have.
    #[cfg(feature = "json")]
    #[tokio::test]
    async fn a_refused_begin_reports_the_publisher_error() {
        use fixtures::Rigged;

        use crate::codec::JsonCodec;

        let wiring = TypedPublisher::with_codec(
            Rigged {
                fail_begin: true,
                ..Rigged::default()
            },
            JsonCodec,
        )
        .transactional();

        let err = wiring
            .begin()
            .await
            .expect_err("the publisher refuses to begin");
        assert_eq!(err.to_string(), "the rigged publisher refused");
    }

    /// Likewise for the owned kind: a refused open is reported, not papered over with an empty
    /// buffer that would silently drop everything published into it.
    #[cfg(all(feature = "memory", feature = "json"))]
    #[tokio::test]
    async fn a_refused_owned_transaction_reports_the_publisher_error() {
        use fixtures::Rigged;

        use crate::codec::JsonCodec;

        let publisher = TypedPublisher::with_codec(Rigged::default(), JsonCodec);

        let err = publisher
            .transaction()
            .await
            .expect_err("the publisher refuses to open a transaction");
        assert_eq!(err.to_string(), "the rigged publisher refused");
    }
}
