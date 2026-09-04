//! Per-delivery publish context and the transform stacks layered over a publisher.

use std::fmt;

use super::Outgoing;
use crate::HeaderMap;

/// A read-only view of the originating delivery, handed to a [`PublishTransform`].
///
/// A reply is published from inside a handler, so the static publish transform can read the
/// delivery that produced it: its channel [`name`](Self::name), the incoming
/// [`headers`](Self::headers) (a W3C `traceparent`, a correlation id), and the broker's typed
/// per-delivery [`context`](Self::context) by [`Field`](crate::Field) key. This is how a trace / correlation id
/// propagates from the incoming message onto the reply (the static, zero-cost path; the app-wide
/// [`PublishLayer`](crate::runtime::PublishLayer) stays context-agnostic). `C` is the
/// handler's context type (`()` when it names none).
pub struct PublishContext<'a, C = ()> {
    name: &'a str,
    headers: &'a HeaderMap,
    cx: &'a C,
}

impl<'a, C> PublishContext<'a, C> {
    /// Builds the view from the parts the runtime already holds at publish time.
    pub(crate) fn new(name: &'a str, headers: &'a HeaderMap, cx: &'a C) -> Self {
        Self { name, headers, cx }
    }

    /// The channel the originating message was delivered on.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name
    }

    /// The originating message's headers (the working copy the handler saw).
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        self.headers
    }

    /// Reads a broker-supplied per-delivery field off the typed context by compile-time `key`,
    /// mirroring [`Context::context`](crate::runtime::Context::context).
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
/// The publish-side counterpart to the consume-side [`Layer`](crate::runtime::Layer): zero-cost composition,
/// no `dyn` dispatch. Composed onto a reply's wiring by the `.transform(..)` step of a mount
/// site's chain. Use for
/// per-destination transforms that belong to the publisher itself - a Confluent / Avro envelope, a
/// fixed content-type header, or stamping the delivery's trace / correlation id onto the reply
/// (read it from `cx`). The `C` parameter is the originating handler's context type; a transform
/// that ignores the context is generic over it (mounts on any handler). For cross-cutting
/// *observation* across every publish (metrics), use the app-wide [`PublishLayer`](crate::runtime::PublishLayer) via
/// [`RustStream::publish_layer`](crate::runtime::RustStream::publish_layer) instead; the per-publisher
/// transforms run first (closest to the value), then the app-wide publish pipeline, then the send.
pub trait PublishTransform<C = ()>: Send + Sync {
    /// Transforms `out` in place before it is sent, reading the delivery through `cx`.
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>);
}

/// The no-op [`PublishTransform`]: the default for a reply wiring with no static transforms.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishTransformIdentity;

impl<C> PublishTransform<C> for PublishTransformIdentity {
    fn apply(&self, _out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {}
}

/// Composes two [`PublishTransform`]s: `inner` runs first, then `outer`. Built by a chain's
/// `.transform(..)` step; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishTransformStack<Inner, Outer> {
    // The reply wiring builds the stack when a transform is layered on.
    pub(super) inner: Inner,
    pub(super) outer: Outer,
}

impl<C, Inner: PublishTransform<C>, Outer: PublishTransform<C>> PublishTransform<C>
    for PublishTransformStack<Inner, Outer>
{
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        self.inner.apply(out, cx);
        self.outer.apply(out, cx);
    }
}

/// A static publish transform that runs only on a batch publishing (`&[T]` + `publish(..)`)
/// handler's replies, not on single-message replies.
///
/// The batch counterpart of [`PublishTransform`], kept a distinct trait so a transform that belongs to
/// the batch path only (a header marking a reply as batched, a per-batch sampling decision) cannot
/// be added with a chain's `.transform(..)` step by mistake; it is added with
/// `.batch_transform(..)`, which the single-message mounts reject at compile time. The
/// per-message [`PublishTransform`] stack does not run for batched replies and this one does not run for
/// single-message replies - the two paths are independent. To use the same transform on both, add
/// it to each, reusing it on the batch side with [`for_batch`] (no second implementation). Each
/// reply in the batch is passed through it individually, reading the delivery through
/// [`PublishContext`].
pub trait BatchPublishTransform<C = ()>: Send + Sync {
    /// Transforms one of the batch's outgoing replies before it is sent.
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>);
}

/// The no-op [`BatchPublishTransform`]: the default for a reply wiring with no batch transforms.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchTransformIdentity;

impl<C> BatchPublishTransform<C> for BatchTransformIdentity {
    fn apply(&self, _out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {}
}

/// Composes two [`BatchPublishTransform`]s: `inner` runs first, then `outer`. Built by a chain's
/// `.batch_transform(..)` step; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchPublishTransformStack<Inner, Outer> {
    // The reply wiring builds the stack when a batch transform is layered on.
    pub(super) inner: Inner,
    pub(super) outer: Outer,
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

/// Lifts a per-message [`PublishTransform`] onto the batch path.
///
/// The same transform then goes on a reply chain's `.batch_transform(..)` step, with no second
/// implementation to write.
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{for_batch, Outgoing, PublishContext, PublishTransform};
/// # use ruststream::subscriber;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: u64 }
/// # #[derive(serde::Serialize, schemars::JsonSchema)]
/// # struct Confirmation { id: u64 }
/// # #[subscriber("orders", publish("confirmations"))]
/// # async fn confirm(orders: &[Order]) -> Vec<Confirmation> {
/// #     orders.iter().map(|o| Confirmation { id: o.id }).collect()
/// # }
///
/// struct Stamp;
/// impl<C> PublishTransform<C> for Stamp {
///     fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
///         out.headers_mut().insert("x-stamp", b"1".to_vec());
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         // `Stamp` on the page path, without a second implementation.
///         b.include(
///             confirm
///                 .batch(nonzero!(8))
///                 .publisher(Publish)
///                 .batch_transform(for_batch(Stamp))
///                 .build(),
///         );
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn for_batch<L>(transform: L) -> ForBatch<L> {
    ForBatch(transform)
}
