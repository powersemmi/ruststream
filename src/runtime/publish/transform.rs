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

/// What a transform does to the message's destination: the second half of what it declares,
/// beside the view it reads.
///
/// A position offers one of these as the most a transform mounted there may do, and a transform
/// projects one as what it needs. They meet at the mount site: [`Names`] on a transform requires
/// [`Names`] on the position, and [`Reads`] fits anywhere.
#[doc(hidden)]
pub trait DestinationUse {}

/// The transform reads the destination and leaves it alone. What almost every transform declares,
/// and what every position offers.
#[derive(Debug, Clone, Copy, Default)]
pub struct Reads;

impl DestinationUse for Reads {}

/// The transform names the destination. A position offers this only where the destination is not
/// already declared, so a declaration and the wire cannot disagree.
#[derive(Debug, Clone, Copy, Default)]
pub struct Names;

impl DestinationUse for Names {}

/// Whether what a transform declares fits the position it is being mounted on. Implemented on the
/// transform's own [`PublishTransform::Destination`], so what fails is a bound naming the reason
/// rather than a mismatch on the projection that produced it.
///
/// [`Reads`] fits any position at any time. [`Names`] fits a position that offers the right
/// ([`NamingOffered`]) and has not given it away ([`NamingUntaken`]), and those are two bounds so
/// a refusal says which of the two it was. `By` is what declares the position's offer - the reply
/// type, or the slot's marker - carried so the error names it.
#[doc(hidden)]
pub trait FitsOffer<Offer, Taken, By> {}

// A reading transform asks for nothing, so it fits every offer whatever came before.
impl<Offer, Taken, By> FitsOffer<Offer, Taken, By> for Reads {}

// A naming one asks for both halves, as bounds rather than as parameter matches.
impl<Offer: NamingOffered<By>, Taken: NamingUntaken, By> FitsOffer<Offer, Taken, By> for Names {}

/// A position that offers a transform the right to name the destination, as declared by `By`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{By}` does not let a transform name the destination",
    label = "the transform on this step declares `Destination = Names`",
    note = "the destination is already declared and the generated document reports it: a reply \
            type carrying `#[outgoing(name = \"..\")]` is published there, a batch's replies \
            answer many deliveries and carry none of their headers, a slot marker offers the \
            right only when every type in its `#[publishes(..)]` dictionary leaves its destination \
            open - one with no dictionary, `DefaultSlot` among them, never offers it - and the \
            `Retry` slot publishes to the subscription's own redelivery address, which no mount \
            site chooses"
)]
pub trait NamingOffered<By> {}

impl<By> NamingOffered<By> for Names {}

/// A position whose naming right no transform has taken yet.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this position has already given the naming right away",
    label = "an earlier transform on this position declares `Destination = Names`",
    note = "one destination, one transform that names it: fold the two decisions into a single \
            transform, or drop one of them"
)]
pub trait NamingUntaken {}

impl NamingUntaken for Reads {}

/// The destination use of a whole transform stack: [`Names`] as soon as one element names.
/// Machinery behind the `.transform(..)` step's own check.
#[doc(hidden)]
pub trait Either<Rhs> {
    /// The combined use.
    type Out;
}

impl<Rhs: DestinationUse> Either<Rhs> for Reads {
    type Out = Rhs;
}

impl<Rhs: DestinationUse> Either<Rhs> for Names {
    type Out = Self;
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
/// belongs. A transform that reads nothing is generic over the kind and over the options, and
/// mounts anywhere:
///
/// ```
/// use ruststream::runtime::{ContextKind, Outgoing, PublishTransform, Reads};
///
/// struct Envelope;
///
/// impl<K: ContextKind, Options> PublishTransform<K, Options> for Envelope {
///     type Destination = Reads;
///
///     fn apply(
///         &self,
///         out: &mut Outgoing<'_>,
///         _options: &mut Option<Options>,
///         _cx: &K::View<'_>,
///     ) {
///         out.headers_mut().insert("x-envelope", b"1".to_vec());
///     }
/// }
/// ```
///
/// One that reads the delivery names [`ForReply`], and naming it on a slot is then a compile error
/// at the mount site:
///
/// ```
/// use ruststream::runtime::{ForReply, Outgoing, PublishContext, PublishTransform, Reads};
///
/// struct StampSource;
///
/// impl<C, Options> PublishTransform<ForReply<C>, Options> for StampSource {
///     type Destination = Reads;
///
///     fn apply(
///         &self,
///         out: &mut Outgoing<'_>,
///         _options: &mut Option<Options>,
///         cx: &PublishContext<'_, C>,
///     ) {
///         out.headers_mut().insert("x-source", cx.name().as_bytes().to_vec());
///     }
/// }
/// ```
///
/// # What a transform writes
///
/// `Options` is the broker's per-message settings type ([`Publisher::Options`](crate::Publisher::Options)),
/// and the position is `None` until something fills it: the publish policy's own settings are
/// what apply. A transform that sets one writes the broker's type into its impl, so it mounts
/// over that broker's publisher and nowhere else:
///
/// ```
/// use ruststream::runtime::{ForSlot, Outgoing, PublishTransform, Reads, SlotContext};
///
/// /// What one broker lets a single message differ in.
/// #[derive(Clone, Default)]
/// struct Priority {
///     level: Option<u8>,
/// }
///
/// struct Urgent;
///
/// impl PublishTransform<ForSlot, Priority> for Urgent {
///     type Destination = Reads;
///
///     fn apply(
///         &self,
///         _out: &mut Outgoing<'_>,
///         options: &mut Option<Priority>,
///         _cx: &SlotContext<'_>,
///     ) {
///         options.get_or_insert_with(Priority::default).level = Some(9);
///     }
/// }
/// ```
///
/// On a slot the position starts as a copy of what the call site's own builder steps set, so a
/// transform completes the call rather than replacing it: it reads a field the call left alone
/// and overrides one it filled. A reply has no call site, so there the position starts at `None`.
///
/// # What a transform may do to the destination
///
/// [`Destination`](Self::Destination) is the third half of the declaration, beside the view and
/// the options: a
/// transform that leaves the destination alone declares [`Reads`], one that names it declares
/// [`Names`]. Stable Rust has no default for an associated type, so every impl writes the line -
/// including the three above, which declare `Reads`.
///
/// A position offers the right to name only where nothing has declared the destination already:
/// a reply type that leaves it open, a slot whose whole `#[publishes(..)]` dictionary leaves it
/// open. Where it is not on offer, mounting a naming transform is a compile error at the mount
/// site, which is what keeps a declaration and the wire in step. [`Outgoing::set_name`] stays a
/// plain method; what is checked is the declaration, not the call.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a publish transform for `{K}` writing `{Options}`",
    note = "a transform states its position by the kind it implements: `PublishTransform<K>` for \
            every `K: ContextKind` mounts anywhere, `PublishTransform<ForReply<C>>` reads the \
            delivery and mounts on a reply, `PublishTransform<ForSlot>` mounts on an `Out` slot. A \
            slot publish is issued by the handler body, so it has no delivery to hand on",
    note = "a transform also states the broker's per-message options it writes, and mounts only \
            over a publisher whose `Publisher::Options` is that type: `{Options}` here. One that \
            writes none is generic over them - `impl<K: ContextKind, Options> \
            PublishTransform<K, Options> for ..` - and mounts anywhere"
)]
pub trait PublishTransform<K: ContextKind, Options = ()>: Send + Sync {
    /// What this transform does to the destination: [`Reads`] or [`Names`].
    type Destination: DestinationUse;

    /// Transforms `out` in place before it is sent, reading the position's view through `cx` and
    /// resolving the broker's per-message settings through `options`.
    ///
    /// `options` is `None` while nothing has set a field: the publish policy's own settings
    /// apply. Set one with `options.get_or_insert_with(Default::default).field = Some(..)`.
    fn apply(&self, out: &mut Outgoing<'_>, options: &mut Option<Options>, cx: &K::View<'_>);
}

/// The no-op [`PublishTransform`]: the default for a reply wiring with no static transforms.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishTransformIdentity;

impl<K: ContextKind, Options> PublishTransform<K, Options> for PublishTransformIdentity {
    type Destination = Reads;

    fn apply(&self, _out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {}
}

/// Composes two [`PublishTransform`]s: `inner` runs first, then `outer`. Built by a chain's
/// `.transform(..)` step; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishTransformStack<Inner, Outer> {
    // A reply wiring and a slot attachment both build the stack when a transform is layered on.
    pub(crate) inner: Inner,
    pub(crate) outer: Outer,
}

impl<K: ContextKind, Options, Inner, Outer> PublishTransform<K, Options>
    for PublishTransformStack<Inner, Outer>
where
    Inner:
        PublishTransform<K, Options, Destination: Either<Outer::Destination, Out: DestinationUse>>,
    Outer: PublishTransform<K, Options>,
{
    type Destination = <Inner::Destination as Either<Outer::Destination>>::Out;

    fn apply(&self, out: &mut Outgoing<'_>, options: &mut Option<Options>, cx: &K::View<'_>) {
        self.inner.apply(out, options, cx);
        self.outer.apply(out, options, cx);
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
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a batch publish transform writing `{Options}`",
    note = "a batch transform states the broker's per-message options it writes, and mounts only \
            over a publisher whose `Publisher::Options` is that type: `{Options}` here. One that \
            writes none is generic over them - `impl<C, Options> BatchPublishTransform<C, \
            Options> for ..` - and mounts anywhere. `for_batch(..)` lifts a per-message \
            `PublishTransform` onto this path"
)]
pub trait BatchPublishTransform<C = (), Options = ()>: Send + Sync {
    /// Transforms one of the batch's outgoing replies before it is sent.
    ///
    /// `cx` is the batch's context, not the reply's own delivery: see the trait's own docs for
    /// what it carries. `options` starts at `None` - a reply has no call site - and what the
    /// stack leaves there is what the publish carries.
    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    );
}

/// The no-op [`BatchPublishTransform`]: the default for a reply wiring with no batch transforms.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchTransformIdentity;

impl<C, Options> BatchPublishTransform<C, Options> for BatchTransformIdentity {
    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        _cx: &PublishContext<'_, C>,
    ) {
    }
}

/// Composes two [`BatchPublishTransform`]s: `inner` runs first, then `outer`. Built by a chain's
/// `.batch_transform(..)` step; you rarely name it directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchPublishTransformStack<Inner, Outer> {
    // The reply wiring builds the stack when a batch transform is layered on.
    pub(super) inner: Inner,
    pub(super) outer: Outer,
}

impl<C, Options, Inner, Outer> BatchPublishTransform<C, Options>
    for BatchPublishTransformStack<Inner, Outer>
where
    Inner: BatchPublishTransform<C, Options>,
    Outer: BatchPublishTransform<C, Options>,
{
    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        self.inner.apply(out, options, cx);
        self.outer.apply(out, options, cx);
    }
}

/// Adapts a per-message [`PublishTransform`] into a [`BatchPublishTransform`], applying it to each reply of
/// a batch. Built by [`for_batch`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ForBatch<L>(L);

impl<C, Options, L: PublishTransform<ForReply<C>, Options>> BatchPublishTransform<C, Options>
    for ForBatch<L>
{
    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        self.0.apply(out, options, cx);
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
/// use ruststream::runtime::{for_batch, ContextKind, Outgoing, PublishTransform, Reads};
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
/// impl<K: ContextKind, Options> PublishTransform<K, Options> for Stamp {
///     type Destination = Reads;
///
///     fn apply(
///         &self,
///         out: &mut Outgoing<'_>,
///         _options: &mut Option<Options>,
///         _cx: &K::View<'_>,
///     ) {
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
