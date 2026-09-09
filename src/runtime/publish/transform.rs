//! Per-delivery publish context and the transform stacks layered over a publisher.

use std::fmt;
use std::marker::PhantomData;

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

/// What one publish position hands its transforms.
///
/// A reply is published by the runtime, which still holds the delivery being answered, so a
/// transform there can read it. A message leaving an [`Out`](crate::runtime::Out) slot is
/// published by the handler body, which has already read whatever it wanted from its own
/// [`Context`](crate::runtime::Context), so there is no delivery left to hand on. The difference
/// between the two positions is exactly this view, not two interfaces: [`PublishTransform`] takes
/// the kind as a parameter and every position picks its own at compile time.
///
/// The set is closed - [`ForReply`] and [`ForSlot`] - and nothing below the mount site asks which
/// one it got.
pub trait ContextKind {
    /// The view a transform of this kind reads, borrowed for the length of one publish.
    type View<'a>
    where
        Self: 'a;
}

/// The kind of a reply position: the transform reads the delivery it answers, as a
/// [`PublishContext`] over the handler's own context type `C`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ForReply<C = ()>(PhantomData<fn() -> C>);

impl<C> ContextKind for ForReply<C> {
    type View<'a>
        = PublishContext<'a, C>
    where
        Self: 'a;
}

/// The kind of an [`Out`](crate::runtime::Out) slot position: the transform reads a
/// [`SlotContext`], which names the slot and nothing else.
#[derive(Debug, Clone, Copy, Default)]
pub struct ForSlot;

impl ContextKind for ForSlot {
    type View<'a>
        = SlotContext<'a>
    where
        Self: 'a;
}

/// What a transform on an [`Out`](crate::runtime::Out) slot gets to read: the slot's own name.
///
/// A slot publish is issued by the handler body, so the delivery that prompted it is the body's to
/// read and to put on the message; by the time a transform runs there is no delivery left. What
/// remains is which slot the message is leaving through, which is worth having when one transform
/// value is named on several slots. `#[non_exhaustive]`, because a position that later carries
/// more can carry it here without a new interface.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct SlotContext<'a> {
    slot: &'a str,
}

impl<'a> SlotContext<'a> {
    /// Builds the view from the marker the mount site bound.
    pub(crate) const fn new(slot: &'a str) -> Self {
        Self { slot }
    }

    /// The slot's [`OutSlot::NAME`](crate::runtime::OutSlot::NAME).
    #[must_use]
    pub const fn slot(&self) -> &'a str {
        self.slot
    }
}

/// A static, compile-time publish transform: mutates an [`Outgoing`] before it is sent, with read
/// access to whatever its position hands it.
///
/// The publish-side counterpart to the consume-side [`Layer`](crate::runtime::Layer): zero-cost composition,
/// no `dyn` dispatch. Composed onto a position by the `.transform(..)` step of a mount site's
/// chain. Use for
/// per-destination transforms that belong to the publisher itself - a Confluent / Avro envelope, a
/// fixed content-type header, or stamping the delivery's trace / correlation id onto the reply
/// (read it from `cx`). For cross-cutting
/// *observation* across every publish (metrics), use the app-wide [`PublishLayer`](crate::runtime::PublishLayer) via
/// [`RustStream::publish_layer`](crate::runtime::RustStream::publish_layer) instead; the per-publisher
/// transforms run first (closest to the value), then the app-wide publish pipeline, then the send.
///
/// # Which positions a transform mounts on
///
/// `K` is the position's [`ContextKind`], and writing the impl is how a transform says where it
/// belongs. A transform that reads nothing is generic over the kind and mounts anywhere:
///
/// ```
/// use ruststream::runtime::{ContextKind, Outgoing, PublishTransform};
///
/// struct Envelope;
///
/// impl<K: ContextKind> PublishTransform<K> for Envelope {
///     fn apply(&self, out: &mut Outgoing<'_>, _cx: &K::View<'_>) {
///         out.headers_mut().insert("x-envelope", b"1".to_vec());
///     }
/// }
/// ```
///
/// One that reads the delivery names [`ForReply`], and naming it on a slot is then a compile error
/// at the mount site:
///
/// ```
/// use ruststream::runtime::{ForReply, Outgoing, PublishContext, PublishTransform};
///
/// struct StampSource;
///
/// impl<C> PublishTransform<ForReply<C>> for StampSource {
///     fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
///         out.headers_mut().insert("x-source", cx.name().as_bytes().to_vec());
///     }
/// }
/// ```
///
/// # Where the destination comes from
///
/// A transform named with `.transform(..)` runs over a message whose destination is already
/// resolved, from the message type's declaration, the mount site or the call site. A transform
/// that decides that destination itself is the same trait named on a different step:
/// `.redirect(..)` runs it first and exists only where the destination is left open, so a
/// declaration and the wire cannot disagree. [`Outgoing::set_name`] is callable from either step -
/// what the two steps differ in is which one the mount site has declared to own the destination.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a publish transform for `{K}`",
    note = "a transform states its position by the kind it implements: `PublishTransform<K>` for \
            every `K: ContextKind` mounts anywhere, `PublishTransform<ForReply<C>>` reads the \
            delivery and mounts on a reply, `PublishTransform<ForSlot>` mounts on an `Out` slot. A \
            slot publish is issued by the handler body, so it has no delivery to hand on"
)]
pub trait PublishTransform<K: ContextKind>: Send + Sync {
    /// Transforms `out` in place before it is sent, reading the position's view through `cx`.
    fn apply(&self, out: &mut Outgoing<'_>, cx: &K::View<'_>);
}

/// The empty redirect position of a reply wiring: the reply goes where its declaration says.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRedirect;

/// The redirect position of a reply wiring, filled by a chain's `.redirect(..)` step: the
/// [`PublishTransform`] that step named, held until the wiring pairs.
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
            decisions into the single transform the step takes"
)]
pub trait RedirectSlotOpen {}

impl RedirectSlotOpen for NoRedirect {}

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
    type Out = PublishTransformStack<T, PL>;

    fn lower(self, layers: PL) -> Self::Out {
        PublishTransformStack {
            inner: self.0,
            outer: layers,
        }
    }
}

/// The no-op [`PublishTransform`]: the default for a reply wiring with no static transforms.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishTransformIdentity;

impl<K: ContextKind> PublishTransform<K> for PublishTransformIdentity {
    fn apply(&self, _out: &mut Outgoing<'_>, _cx: &K::View<'_>) {}
}

/// Composes two [`PublishTransform`]s: `inner` runs first, then `outer`. Built by a chain's
/// `.transform(..)` step; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishTransformStack<Inner, Outer> {
    // A reply wiring and a slot attachment both build the stack when a transform is layered on.
    pub(crate) inner: Inner,
    pub(crate) outer: Outer,
}

impl<K: ContextKind, Inner: PublishTransform<K>, Outer: PublishTransform<K>> PublishTransform<K>
    for PublishTransformStack<Inner, Outer>
{
    fn apply(&self, out: &mut Outgoing<'_>, cx: &K::View<'_>) {
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

impl<C, L: PublishTransform<ForReply<C>>> BatchPublishTransform<C> for ForBatch<L> {
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
/// use ruststream::runtime::{for_batch, ContextKind, Outgoing, PublishTransform};
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
/// impl<K: ContextKind> PublishTransform<K> for Stamp {
///     fn apply(&self, out: &mut Outgoing<'_>, _cx: &K::View<'_>) {
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
