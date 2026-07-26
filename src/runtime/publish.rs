//! Outgoing message and the publish middleware pipeline.
//!
//! When a handler's reply is published (via `#[subscriber(.., publish(..))]`), it flows through a
//! chain of [`PublishLayer`] before reaching the broker publisher. Middleware transform the
//! payload (for example, wrap it in a Confluent / Avro envelope) and enrich the headers
//! (content-type, schema id), or observe it (publish metrics). The chain is symmetric to the
//! consume-side static [`Stack`](super::Stack).

use std::{borrow::Cow, future::Future, pin::Pin, sync::Arc};

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
use crate::{Headers, OutgoingMessage, Publisher, TransactionalPublisher};

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

impl<N, P> std::fmt::Debug for PublishNext<'_, N, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Debug for PublishDynNext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Debug for PublishDynStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl<C> std::fmt::Debug for PublishContext<'_, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    /// Exists only when the underlying publisher implements
    /// [`TransactionalPublisher`](crate::TransactionalPublisher); for brokers without
    /// transactions, the method does not exist and the compiler enforces it. The returned wiring
    /// is accepted by the batch publishing mounts only: a one-message transaction adds broker
    /// round-trips for no atomicity gain, so the single-message `include_publishing` forms keep
    /// taking a plain [`TypedPublisher`].
    #[must_use]
    pub fn transactional(self) -> Transactional<P, C, PL, BL>
    where
        P: TransactionalPublisher,
    {
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

impl<P, C, PL, BL> std::fmt::Debug for TypedPublisher<P, C, PL, BL> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedPublisher").finish_non_exhaustive()
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

impl<P, C, PL, BL> std::fmt::Debug for Transactional<P, C, PL, BL> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transactional").finish_non_exhaustive()
    }
}

mod sealed {
    /// Seals [`ReplyPublisher`](super::ReplyPublisher): the reply-publishing strategies are the
    /// two wirings above, not an extension point.
    pub trait Sealed {}

    impl<P, C, PL, BL> Sealed for super::TypedPublisher<P, C, PL, BL> {}
    impl<P, C, PL, BL> Sealed for super::Transactional<P, C, PL, BL> {}
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
    pub async fn commit(mut self) -> Result<(), P::Error> {
        self.open = false;
        self.publisher.commit().await
    }

    /// Aborts the transaction: nothing published through the scope becomes visible.
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker fails to abort.
    pub async fn abort(mut self) -> Result<(), P::Error> {
        self.open = false;
        self.publisher.abort().await
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

impl<P, C> std::fmt::Debug for TransactionScope<'_, P, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionScope")
            .field("open", &self.open)
            .finish_non_exhaustive()
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
}
