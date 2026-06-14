//! Outgoing message and the publish middleware pipeline.
//!
//! When a handler's reply is published (via `#[subscriber(.., publish(..))]`), it flows through a
//! chain of [`PublishMiddleware`] before reaching the broker publisher. Middleware transform the
//! payload (for example, wrap it in a Confluent / Avro envelope) and enrich the headers
//! (content-type, schema id), or observe it (publish metrics). The chain is symmetric to the
//! consume-side [`DynStack`](super::DynStack).

use std::{borrow::Cow, future::Future, pin::Pin, sync::Arc};

use bytes::BytesMut;
use serde::Serialize;
use tracing::warn;

use super::lifecycle::BoxError;
use super::publisher_registry::ErasedPublisher;
use crate::codec::Codec;
// `DefaultCodec` only exists when a codec feature is on; the impl that names it is gated the same
// way, so an ungated import would break `--no-default-features`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::runtime::publish::sealed::Sealed;
use crate::{Extensions, Headers, Publisher, TransactionalPublisher};

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

/// Middleware that transforms (or observes) an [`Outgoing`] message before it is published.
///
/// Each middleware inspects / mutates `out`, then calls [`PublishNext::run`] to continue; the chain
/// ends in the actual broker publish.
pub trait PublishMiddleware: Send + Sync {
    /// Handle the outgoing message, calling `next` to continue the pipeline.
    fn on_publish<'a>(&'a self, out: &'a mut Outgoing<'a>, next: PublishNext<'a>)
    -> PublishFut<'a>;
}

/// A cursor over the remaining publish middleware, ending in the broker publisher.
///
/// Carries the originating delivery's per-delivery [`Extensions`] (via
/// [`extensions`](Self::extensions)), so a transactional middleware or publisher can read a
/// broker-supplied commit token at commit time. A publish with no originating delivery (a fresh
/// startup publish) sees an empty map.
pub struct PublishNext<'a> {
    rest: &'a [Arc<dyn PublishMiddleware>],
    publisher: &'a dyn ErasedPublisher,
    extensions: &'a Extensions,
}

impl<'a> PublishNext<'a> {
    /// Runs the next middleware, or sends the message if the pipeline is exhausted.
    #[must_use]
    pub fn run(self, out: &'a mut Outgoing<'a>) -> PublishFut<'a> {
        match self.rest.split_first() {
            Some((middleware, rest)) => middleware.on_publish(
                out,
                PublishNext {
                    rest,
                    publisher: self.publisher,
                    extensions: self.extensions,
                },
            ),
            None => self
                .publisher
                .publish_message(out.name(), out.payload(), out.headers()),
        }
    }

    /// The originating delivery's per-delivery [`Extensions`], for a transactional middleware to
    /// read a broker-supplied commit token. Empty for a publish with no originating delivery.
    #[must_use]
    pub fn extensions(&self) -> &Extensions {
        self.extensions
    }
}

impl std::fmt::Debug for PublishNext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishNext")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// Runs `out` through `pipeline`, then publishes it via `publisher`, carrying the originating
/// delivery's `extensions` for any transactional middleware to read.
pub(crate) fn run_publish<'a>(
    pipeline: &'a [Arc<dyn PublishMiddleware>],
    publisher: &'a dyn ErasedPublisher,
    out: &'a mut Outgoing<'a>,
    extensions: &'a Extensions,
) -> PublishFut<'a> {
    PublishNext {
        rest: pipeline,
        publisher,
        extensions,
    }
    .run(out)
}

/// A named publisher resolved from a [`Context`](super::Context), sending through the scope's
/// publish pipeline.
///
/// Returned by [`Context::publisher`](super::Context::publisher). Publishing through it runs the
/// same [`PublishMiddleware`] chain (envelope, metrics) as a macro reply, so a manual publish from a
/// handler is not a hole in the pipeline.
pub struct ScopedPublisher<'a> {
    publisher: &'a dyn ErasedPublisher,
    pipeline: &'a [Arc<dyn PublishMiddleware>],
    extensions: &'a Extensions,
}

impl<'a> ScopedPublisher<'a> {
    pub(crate) fn new(
        publisher: &'a dyn ErasedPublisher,
        pipeline: &'a [Arc<dyn PublishMiddleware>],
        extensions: &'a Extensions,
    ) -> Self {
        Self {
            publisher,
            pipeline,
            extensions,
        }
    }

    /// Sends `out` through the publish pipeline to the broker, carrying the originating delivery's
    /// per-delivery extensions for any transactional middleware to read.
    ///
    /// # Errors
    ///
    /// Returns the boxed error from a middleware or the broker publish if either fails.
    pub async fn publish(&self, mut out: Outgoing<'_>) -> Result<(), BoxError> {
        run_publish(self.pipeline, self.publisher, &mut out, self.extensions).await
    }
}

impl std::fmt::Debug for ScopedPublisher<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedPublisher")
            .field("layers", &self.pipeline.len())
            .finish_non_exhaustive()
    }
}

/// A static, compile-time publish transform: mutates an [`Outgoing`] before it is sent.
///
/// The publish-side counterpart to the consume-side [`Layer`](super::Layer): zero-cost composition,
/// no `dyn` dispatch. Baked onto a [`TypedPublisher`] with [`TypedPublisher::layer`]. Use for
/// per-destination transforms that belong to the publisher itself - a Confluent / Avro envelope, a
/// fixed content-type header. For cross-cutting *observation* across every publish (metrics), use
/// the dynamic [`PublishMiddleware`] via
/// [`RustStream::publish_layer`](super::RustStream::publish_layer) instead; the static transforms
/// run first (closest to the value), then the dynamic pipeline, then the send.
pub trait PublishLayer: Send + Sync {
    /// Transforms `out` in place before it is sent.
    fn apply(&self, out: &mut Outgoing<'_>);
}

/// The no-op [`PublishLayer`]: the default for a [`TypedPublisher`] with no static transforms.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishIdentity;

impl PublishLayer for PublishIdentity {
    fn apply(&self, _out: &mut Outgoing<'_>) {}
}

/// Composes two [`PublishLayer`]s: `inner` runs first, then `outer`. Built by
/// [`TypedPublisher::layer`]; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishStack<Inner, Outer> {
    inner: Inner,
    outer: Outer,
}

impl<Inner: PublishLayer, Outer: PublishLayer> PublishLayer for PublishStack<Inner, Outer> {
    fn apply(&self, out: &mut Outgoing<'_>) {
        self.inner.apply(out);
        self.outer.apply(out);
    }
}

/// A byte [`Publisher`] paired with a [`Codec`] and a static [`PublishLayer`] stack, ready to send
/// typed values.
///
/// The publish-side counterpart to a typed subscriber: it carries *how* a value is encoded and the
/// per-publisher transforms ([`layer`](Self::layer)), while *where* it goes (the destination name)
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
pub struct TypedPublisher<P, C, PL = PublishIdentity> {
    publisher: P,
    codec: C,
    layers: PL,
}

impl<P, C> TypedPublisher<P, C, PublishIdentity> {
    /// Pairs `publisher` with an explicit `codec` and no static transforms.
    #[must_use]
    pub fn with_codec(publisher: P, codec: C) -> Self {
        Self {
            publisher,
            codec,
            layers: PublishIdentity,
        }
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<P> TypedPublisher<P, DefaultCodec, PublishIdentity> {
    /// Pairs `publisher` with the [`DefaultCodec`](DefaultCodec) and no static
    /// transforms. Use [`with_codec`](Self::with_codec) to name a codec explicitly.
    #[must_use]
    pub fn new(publisher: P) -> Self {
        Self::with_codec(publisher, DefaultCodec::default())
    }
}

impl<P, C, PL> TypedPublisher<P, C, PL> {
    /// The codec this publisher encodes replies with. Lets the runtime reuse it as the decode
    /// codec when a publishing handler is mounted without an explicit one.
    pub(crate) const fn codec(&self) -> &C {
        &self.codec
    }

    /// Adds a static [`PublishLayer`], applied to every outgoing message from this publisher. The
    /// first one added runs first (closest to the encoded value).
    #[must_use]
    pub fn layer<N>(self, layer: N) -> TypedPublisher<P, C, PublishStack<PL, N>> {
        TypedPublisher {
            publisher: self.publisher,
            codec: self.codec,
            layers: PublishStack {
                inner: self.layers,
                outer: layer,
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
    pub fn transactional(self) -> Transactional<P, C, PL>
    where
        P: TransactionalPublisher,
    {
        Transactional { inner: self }
    }
}

impl<P: Publisher, C: Codec, PL: PublishLayer> TypedPublisher<P, C, PL> {
    /// Encodes `value`, applies the static transforms, then publishes to `name` through `pipeline`,
    /// carrying the originating delivery's `extensions` for any transactional middleware to read.
    pub(crate) async fn publish<T: Serialize + Sync>(
        &self,
        name: &str,
        value: &T,
        pipeline: &[Arc<dyn PublishMiddleware>],
        extensions: &Extensions,
    ) -> Result<(), BoxError> {
        let payload = self
            .codec
            .encode(value)
            .map_err(|e| Box::new(e) as BoxError)?;
        let mut out = Outgoing::new(name, payload);
        self.layers.apply(&mut out);
        run_publish(pipeline, &self.publisher, &mut out, extensions).await
    }
}

impl<P, C, PL> std::fmt::Debug for TypedPublisher<P, C, PL> {
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
pub struct Transactional<P, C, PL = PublishIdentity> {
    inner: TypedPublisher<P, C, PL>,
}

impl<P, C, PL> std::fmt::Debug for Transactional<P, C, PL> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transactional").finish_non_exhaustive()
    }
}

mod sealed {
    /// Seals [`ReplyPublisher`](super::ReplyPublisher): the reply-publishing strategies are the
    /// two wirings above, not an extension point.
    pub trait Sealed {}

    impl<P, C, PL> Sealed for super::TypedPublisher<P, C, PL> {}
    impl<P, C, PL> Sealed for super::Transactional<P, C, PL> {}
}

/// The reply wiring accepted by the `include_batch_publishing` mounts.
///
/// Implemented by a plain [`TypedPublisher`] (each reply published independently) and by a
/// [`Transactional`] one (all replies of a batch inside one transaction). Sealed: implemented by
/// exactly those two types.
pub trait ReplyPublisher: Sealed + Send + Sync {
    /// The codec replies are encoded with (also reused as the decode codec when a batch
    /// publishing handler is mounted without an explicit one).
    type Codec: Codec;

    /// Returns the reply codec.
    #[doc(hidden)]
    fn reply_codec(&self) -> &Self::Codec;

    /// Publishes one batch's replies to `name` through `pipeline`, carrying the originating
    /// delivery's `extensions` for a transactional publisher to read at commit time.
    #[doc(hidden)]
    fn publish_batch<'a, T>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a [Arc<dyn PublishMiddleware>],
        extensions: &'a Extensions,
    ) -> impl Future<Output = Result<(), BoxError>> + Send
    where
        T: Serialize + Sync;
}

impl<P, C, PL> ReplyPublisher for TypedPublisher<P, C, PL>
where
    P: Publisher,
    C: Codec,
    PL: PublishLayer,
{
    type Codec = C;

    fn reply_codec(&self) -> &C {
        self.codec()
    }

    /// Each reply is published independently: a mid-batch failure leaves the earlier replies
    /// visible, and the retried batch may publish them again (at-least-once).
    async fn publish_batch<'a, T>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a [Arc<dyn PublishMiddleware>],
        extensions: &'a Extensions,
    ) -> Result<(), BoxError>
    where
        T: Serialize + Sync,
    {
        for reply in replies {
            self.publish(name, reply, pipeline, extensions).await?;
        }
        Ok(())
    }
}

impl<P, C, PL> ReplyPublisher for Transactional<P, C, PL>
where
    P: TransactionalPublisher,
    C: Codec,
    PL: PublishLayer,
{
    type Codec = C;

    fn reply_codec(&self) -> &C {
        self.inner.codec()
    }

    /// All replies publish inside one transaction: begin, publish each, commit. Any failure
    /// aborts the transaction, so none of the batch's replies become visible. The originating
    /// delivery's `extensions` (for example a broker-supplied commit token) are carried through the
    /// publish pipeline so a transactional middleware can read them at commit time.
    async fn publish_batch<'a, T>(
        &'a self,
        name: &'a str,
        replies: &'a [T],
        pipeline: &'a [Arc<dyn PublishMiddleware>],
        extensions: &'a Extensions,
    ) -> Result<(), BoxError>
    where
        T: Serialize + Sync,
    {
        let publisher = &self.inner.publisher;
        publisher
            .begin_transaction()
            .await
            .map_err(|e| Box::new(e) as BoxError)?;
        for reply in replies {
            if let Err(err) = self.inner.publish(name, reply, pipeline, extensions).await {
                abort_quietly(publisher).await;
                return Err(err);
            }
        }
        if let Err(err) = publisher.commit().await {
            abort_quietly(publisher).await;
            return Err(Box::new(err) as BoxError);
        }
        Ok(())
    }
}

/// Aborts a failed transaction; an abort failure is logged, not propagated, because the
/// original publish / commit error is the one the caller acts on.
async fn abort_quietly<P: TransactionalPublisher>(publisher: &P) {
    if let Err(err) = publisher.abort().await {
        warn!(target: "ruststream::dispatch", error = %err, "transaction abort failed");
    }
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
