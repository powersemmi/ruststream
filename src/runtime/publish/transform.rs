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
///
/// A batch's replies are the exception, because a batch spans many deliveries and has no single
/// one to read: a [`BatchPublishTransform`] sees one view for the whole batch, whose
/// [`name`](Self::name) is the subscription, whose [`headers`](Self::headers) are empty, and
/// whose [`context`](Self::context) is the broker's batch context.
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
///
/// # Contract
///
/// A publish transform owns the headers and the payload, and it must leave the destination alone:
/// [`Outgoing::set_name`] is not its call to make. Where a reply goes is declared - by the reply
/// type or by the mount site - and the generated `AsyncAPI` document reports that declaration, so
/// a transform that rewrote the name would publish somewhere the document never names. A transform
/// that decides the destination per delivery is a [`RedirectTransform`] instead, and it rides the
/// mount chain's `.redirect(..)` step.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a publish transform",
    note = "a `PublishTransform` rewrites a reply's headers and payload and leaves its \
            destination alone; one that names the destination per delivery implements \
            `RedirectTransform` and rides the chain's `.redirect(..)` step instead"
)]
pub trait PublishTransform<C = ()>: Send + Sync {
    /// Transforms `out` in place before it is sent, reading the delivery through `cx`.
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>);
}

/// A static, compile-time redirect: names the destination of one reply, per delivery.
///
/// The one transform allowed to call [`Outgoing::set_name`]. It answers the case a declaration
/// cannot: a broker whose answer goes where the request says (an AMQP `reply-to` header, a
/// `ZeroMQ` `ROUTER` peer identity) rather than to a channel a service names up front. Read the
/// delivery through `cx` and set the name; the headers and the payload belong to the
/// [`PublishTransform`] stack, which runs after this.
///
/// A chain takes at most one redirect, and only where the reply type leaves the destination open
/// (`#[derive(Outgoing)]` with no `#[outgoing(name = "..")]`): a reply that names its own channel
/// is published there, and `.redirect(..)` on it does not compile. The mount site's own name stays
/// as the reply's declared destination - it is what the document reports and what a delivery falls
/// back to when the redirect leaves the name alone.
///
/// `C` is the originating handler's context type, exactly as on [`PublishTransform`]; a redirect
/// that reads only the incoming headers is generic over it and mounts on any handler.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{Outgoing, PublishContext, RedirectTransform};
/// # use ruststream::subscriber;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: u64 }
/// # #[derive(serde::Serialize, schemars::JsonSchema, ruststream::Outgoing)]
/// # struct Confirmation { id: u64 }
/// # #[subscriber("orders", publish("confirmations"))]
/// # async fn confirm(order: &Order) -> Confirmation {
/// #     Confirmation { id: order.id }
/// # }
///
/// /// Answers where the request asked to be answered.
/// struct ReplyTo;
///
/// impl<C> RedirectTransform<C> for ReplyTo {
///     fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
///         if let Some(to) = cx.headers().get("reply-to")
///             && let Ok(to) = std::str::from_utf8(to)
///         {
///             out.set_name(to.to_owned());
///         }
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(confirm).out(Reply, Publish).redirect(ReplyTo);
///     })
/// }
/// # }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a redirect",
    note = "`.redirect(..)` takes a `RedirectTransform`, the one transform that names a reply's \
            destination; a transform that only rewrites headers and payload implements \
            `PublishTransform` and rides `.transform(..)` instead"
)]
pub trait RedirectTransform<C = ()>: Send + Sync {
    /// Names the destination of `out`, reading the delivery through `cx`.
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>);
}

/// The empty redirect position of a reply wiring: the reply goes where its declaration says.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRedirect;

/// The redirect position of a reply wiring, filled by a chain's `.redirect(..)` step.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Redirected<T>(pub(super) T);

/// A redirect position still empty: what a `.redirect(..)` step fills.
///
/// Stated about the position ([`AddReplyRedirect::Slot`](super::AddReplyRedirect::Slot)) rather
/// than about the wiring, so a second `.redirect(..)` reports the redirect the first one already
/// named.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this reply is already redirected",
    label = "`.redirect(..)` names the reply's destination, and it is named",
    note = "a reply has one destination: drop one of the `.redirect(..)` calls, or fold the two \
            decisions into a single `RedirectTransform`"
)]
pub trait RedirectSlotOpen {}

impl RedirectSlotOpen for NoRedirect {}

/// A [`RedirectTransform`] seen as the first element of a reply's [`PublishTransform`] stack:
/// how a named redirect reaches the publish path. Built when the wiring pairs.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Redirecting<T>(T);

impl<C, T: RedirectTransform<C>> PublishTransform<C> for Redirecting<T> {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        self.0.apply(out, cx);
    }
}

/// Lowers a reply wiring's redirect position onto its [`PublishTransform`] stack, producing the
/// stack the live publisher runs. Machinery; never named in user code.
///
/// The empty position lowers to the stack unchanged, so a reply with no redirect pays nothing. A
/// named one becomes the stack's first element: the redirect decides where the reply goes, then
/// the ordinary transforms rewrite its headers and payload, whatever order the chain named them
/// in.
#[doc(hidden)]
pub trait LowerRedirect<PL> {
    /// The composed transform stack.
    type Out;

    /// Composes it.
    fn lower(self, layers: PL) -> Self::Out;
}

impl<PL> LowerRedirect<PL> for NoRedirect {
    type Out = PL;

    fn lower(self, layers: PL) -> PL {
        layers
    }
}

impl<T, PL> LowerRedirect<PL> for Redirected<T> {
    type Out = PublishTransformStack<Redirecting<T>, PL>;

    fn lower(self, layers: PL) -> Self::Out {
        PublishTransformStack {
            inner: Redirecting(self.0),
            outer: layers,
        }
    }
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
/// it to each, reusing it on the batch side with [`for_batch`] (no second implementation).
///
/// Each reply in the batch runs through it individually, but they all see one
/// [`PublishContext`], and it is the batch's rather than a delivery's: a batch spans many
/// deliveries, so it has no single one to read. [`name`](PublishContext::name) is the
/// subscription, [`headers`](PublishContext::headers) is empty, and
/// [`context`](PublishContext::context) reads the broker's batch context (built from the batch's
/// first delivery). A transform that has to read the message it answers for belongs on the
/// per-message path, where the reply and its delivery are one.
pub trait BatchPublishTransform<C = ()>: Send + Sync {
    /// Transforms one of the batch's outgoing replies before it is sent.
    ///
    /// `cx` is the batch's context, not the reply's own delivery: see the trait's own docs for
    /// what it carries.
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
/// # #[derive(serde::Serialize, schemars::JsonSchema, ruststream::Outgoing)]
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
///         // `Stamp` on the batch path, without a second implementation.
///         b.include(confirm.batch(nonzero!(8)))
///             .out(Reply, Publish)
///             .batch_transform(for_batch(Stamp));
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn for_batch<L>(transform: L) -> ForBatch<L> {
    ForBatch(transform)
}
