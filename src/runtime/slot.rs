//! Marker-identified `Out` slots: the type-level machinery pairing each
//! [`Out`](super::Out) parameter with the publish policy attached at the include site.
//!
//! A handler declares `Out(out): Out<impl Publisher, MySlot>`; the marker (`MySlot`, or the
//! implicit [`DefaultSlot`]) identifies the slot, and the concrete publisher type is inferred
//! from the attachment's [`PublishPolicy::Live`](crate::PublishPolicy::Live) - fully monomorphized, no erasure. The pieces:
//!
//! - [`OutSlot`] / [`DefaultSlot`]: the marker vocabulary (`#[derive(OutSlot)]` for named ones).
//! - [`SlotPublisher`]: the transparent wrapper the handler actually receives; it delegates the
//!   publisher capabilities and, under the `testing` feature, attributes publishes to the slot.
//! - [`HasSlots`] / [`BindSlots`]: the macro-implemented contract on a definition (the marker
//!   list, and the instantiation of the publisher-generic definition from the bound sources).
//! - [`BindSlot`] / [`InitSlots`] / [`MissingSlot`]: the include-site builder machinery that
//!   places each `.out(marker, policy)` attachment into its marker's position, in any order;
//!   a registration commits only when no `MissingSlot` remains, so a forgotten binding is a
//!   compile error naming the slot.

use std::marker::PhantomData;
use std::time::Duration;

#[cfg(feature = "testing")]
use crate::testing::coordinator::record_slot_publish;
use crate::{
    ConnectedBroker, OutgoingMessage, OwnedTransactions, Publisher, RequestReply,
    TransactionalPublisher,
};

/// A slot marker: the identity of one [`Out`](super::Out) injection.
///
/// Markers decouple a handler signature from the concrete publisher type: the handler names the
/// slot, the include site names the policy, and the runtime pairs them. Derive it on a unit
/// struct with `#[derive(OutSlot)]` (`macros` feature); [`NAME`](Self::NAME) labels the slot in
/// startup errors and test assertions.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::OutSlot;
///
/// #[derive(Clone, Copy, Debug)]
/// struct Encoded;
///
/// impl OutSlot for Encoded {
///     const NAME: &'static str = "Encoded";
/// }
///
/// assert_eq!(<Encoded as OutSlot>::NAME, "Encoded");
/// ```
pub trait OutSlot: 'static {
    /// The human-readable slot name, used by diagnostics and test assertions.
    const NAME: &'static str;
}

/// The implicit marker of a single unnamed `Out<impl Publisher>` parameter.
///
/// A handler with one `Out` parameter needs no marker: the parameter binds to this slot, and
/// the include site attaches its policy with the plain
/// [`publisher`](super::IncludeWith::publisher) call.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{DefaultSlot, OutSlot};
///
/// assert_eq!(<DefaultSlot as OutSlot>::NAME, "default");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DefaultSlot;

impl OutSlot for DefaultSlot {
    const NAME: &'static str = "default";
}

/// The live publisher an [`Out`](super::Out) slot injects: the attachment's paired publisher,
/// wrapped with the slot identity.
///
/// The wrapper is a zero-cost newtype (static dispatch, no boxing): every capability of the
/// underlying publisher ([`Publisher`], [`TransactionalPublisher`], [`OwnedTransactions`],
/// [`RequestReply`]) is delegated, so an `Out<impl OwnedTransactions, M>` bound holds exactly
/// when the attached policy's live form supports it. Under the `testing` feature, publishes made
/// through the wrapper inside a `TestApp`-driven handler are also recorded against the slot,
/// which is what backs the harness's per-slot assertions.
///
/// You never name this type: the handler sees it as `impl Publisher` (or a capability
/// refinement), and the include machinery constructs it.
#[derive(Debug)]
pub struct SlotPublisher<P, M> {
    inner: P,
    _slot: PhantomData<fn() -> M>,
}

impl<P, M> SlotPublisher<P, M> {
    pub(crate) fn new(inner: P) -> Self {
        Self {
            inner,
            _slot: PhantomData,
        }
    }

    /// The paired value the slot wraps: the extension point for broker-defined capabilities.
    ///
    /// The core capability vocabulary ([`Publisher`], [`TransactionalPublisher`],
    /// [`OwnedTransactions`], [`RequestReply`]) is delegated by this wrapper directly. A broker
    /// whose paired value offers more than that - or is not a publisher at all (a per-partition
    /// producer cache, a shard router) - declares its own capability trait and grafts it onto
    /// the wrapper with a blanket impl delegating through this accessor; handlers then bound
    /// their slot with that trait (`Out<impl PartitionLanes>`). The wrapper type itself never
    /// appears in handler code, so this accessor is reachable only from such generic impls.
    ///
    /// Publishes made through values obtained this way bypass the slot's test-capture
    /// attribution, like a settled owned transaction's buffer: assert on the broker's publish
    /// log for those.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{OutSlot, SlotPublisher};
    ///
    /// /// A broker-defined capability the core knows nothing about.
    /// trait PartitionLanes {
    ///     fn lane_id(&self, partition: i32) -> String;
    /// }
    ///
    /// // The broker crate grafts it onto the slot wrapper once, for every marker:
    /// impl<P: PartitionLanes, M: OutSlot> PartitionLanes for SlotPublisher<P, M> {
    ///     fn lane_id(&self, partition: i32) -> String {
    ///         self.inner().lane_id(partition)
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn inner(&self) -> &P {
        &self.inner
    }
}

impl<P: Publisher, M: OutSlot> Publisher for SlotPublisher<P, M> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        #[cfg(feature = "testing")]
        record_slot_publish(M::NAME, &msg);
        self.inner.publish(msg).await
    }
}

impl<P: TransactionalPublisher, M: OutSlot> TransactionalPublisher for SlotPublisher<P, M> {
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.inner.begin_transaction().await
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.inner.commit().await
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.inner.abort().await
    }
}

// Transaction values own their buffer and leave the slot's scope, so publishes made through
// them are visible in the broker's publish log but are not attributed to the slot.
impl<P: OwnedTransactions, M: OutSlot> OwnedTransactions for SlotPublisher<P, M> {
    type Transaction = P::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.inner.transaction().await
    }
}

impl<P: RequestReply, M: OutSlot> RequestReply for SlotPublisher<P, M> {
    type Reply = P::Reply;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        #[cfg(feature = "testing")]
        record_slot_publish(M::NAME, &msg);
        self.inner.request(msg, timeout).await
    }
}

/// The "not bound yet" placeholder of the `Out` slot marked `M` at the include site.
///
/// The marker rides in the type so the compile error of an incomplete registration (a
/// `.mount()` whose attachment tuple still contains a `MissingSlot<..>`) names the slot that
/// was forgotten. A value of this type never reaches the runtime: committing requires every
/// position bound.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct MissingSlot<M>(PhantomData<fn() -> M>);

impl<M> MissingSlot<M> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

/// A user-attached publisher source, wrapped so the bound and unbound attachment states live on
/// different type constructors (disjoint impls, no negative reasoning needed).
#[doc(hidden)]
#[derive(Debug)]
pub struct WithSource<Source>(Source);

impl<Source> WithSource<Source> {
    pub(crate) fn new(source: Source) -> Self {
        Self(source)
    }
}

impl<Source> IntoSlotSource for WithSource<Source> {
    type Source = Source;

    fn into_source(self) -> Self::Source {
        self.0
    }
}

/// A definition whose handler carries marker-identified `Out` slots.
///
/// Implemented by `#[subscriber]` on the value you pass to `include`; [`Markers`](Self::Markers)
/// lists the slot markers in signature order, which is the order the positional machinery
/// ([`InitSlots`], [`BindSlot`], [`BindSlots`]) works in. You never implement this by hand
/// unless you hand-write a definition.
pub trait HasSlots {
    /// The slot markers, as a tuple in the handler signature's parameter order.
    type Markers;
}

/// Builds the initial all-unbound attachment tuple for a definition's markers. Machinery behind
/// `include`; never named directly.
#[doc(hidden)]
pub trait InitSlots {
    /// One [`MissingSlot`] per marker, carrying it.
    type Init;

    fn init() -> Self::Init;
}

/// Unwraps one include-site attachment into the policy the runtime pairs: a bound
/// [`WithSource`] yields its policy. Machinery; never named directly.
#[doc(hidden)]
pub trait IntoSlotSource {
    /// The publish policy this attachment resolves to.
    type Source;

    fn into_source(self) -> Self::Source;
}

/// Positional placement of one `.out(marker, policy)` attachment: finds the (still unbound)
/// element carrying the marker and replaces it. The `Index` parameter is inferred per call,
/// which is what makes the calls order-independent; binding the same slot twice finds no
/// still-unbound element and fails to compile. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this handler has no unbound Out slot marked `{M}`",
    note = "each .out(marker, policy) call binds one still-unbound slot the handler declares; \
            check the marker, and that the slot was not bound twice"
)]
pub trait BindSlot<M, Src, Index> {
    /// The attachment tuple with the marker's position bound.
    type Out;

    fn bind(self, src: Src) -> Self::Out;
}

/// The position tokens inferred by [`BindSlot`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct SlotPos<const N: usize>;

/// Instantiates a slot-carrying definition from the bound sources.
///
/// Implemented by `#[subscriber]` next to [`HasSlots`]: given the source tuple (in marker
/// order), it names the fully-applied definition type - each slot's publisher is
/// [`SlotPublisher`] over the source's [`PublishPolicy::Live`](crate::PublishPolicy::Live) - and pads the sources into the
/// definition's injection-extra tuple (a unit for a trailing `Seek` parameter). The connected
/// broker type `C` is the pairing target the publisher types are computed against.
pub trait BindSlots<C: ConnectedBroker, Sources>: Sized {
    /// The publisher-applied definition type.
    type Bound;

    /// The extra tuple [`FromStartup`](super::FromStartup) resolves the injections against:
    /// the sources, padded to the injection tuple's arity.
    type Extra;

    /// Instantiates the definition and arranges the sources for resolution.
    fn bind(self, sources: Sources) -> (Self::Bound, Self::Extra);
}

/// Implements [`InitSlots`] for each marker-tuple arity: every position starts as its marker's
/// [`MissingSlot`].
macro_rules! impl_init_slots {
    ($(($($marker:ident),+))+) => {$(
        impl<$($marker),+> InitSlots for ($($marker,)+) {
            type Init = ($(MissingSlot<$marker>,)+);

            fn init() -> Self::Init {
                ($(MissingSlot::<$marker>::new(),)+)
            }
        }
    )+};
}

impl_init_slots! {
    (M0)
    (M0, M1)
    (M0, M1, M2)
}

/// Implements [`BindSlot`] for each (arity, position) pair: the element carrying the marker
/// (`@ <position>`) becomes [`WithSource`], the surrounding elements pass through unchanged.
macro_rules! impl_bind_slot {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<M, Src $(, $before)* $(, $after)*> BindSlot<M, Src, SlotPos<$pos>>
            for ($($before,)* MissingSlot<M>, $($after,)*)
        {
            type Out = ($($before,)* WithSource<Src>, $($after,)*);

            fn bind(self, src: Src) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($before,)* _missing, $($after,)*) = self;
                ($($before,)* WithSource::new(src), $($after,)*)
            }
        }
    )+};
}

impl_bind_slot! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct A;
    impl OutSlot for A {
        const NAME: &'static str = "A";
    }

    #[derive(Debug)]
    struct B;
    impl OutSlot for B {
        const NAME: &'static str = "B";
    }

    #[test]
    fn binds_slots_in_any_order() {
        let init = <(A, B) as InitSlots>::init();
        // Bind the second marker first, then the first: positions are found by marker.
        let step = BindSlot::<B, &str, SlotPos<1>>::bind(init, "b");
        let done = BindSlot::<A, &str, SlotPos<0>>::bind(step, "a");
        let (a, b) = done;
        assert_eq!(a.into_source(), "a");
        assert_eq!(b.into_source(), "b");
    }
}
