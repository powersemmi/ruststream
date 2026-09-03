//! The typed publishing capabilities a body names in its bounds: [`Publish`] for the builder,
//! and its refinements [`TransactionalPublish`], [`OwnedTransactionalPublish`] and
//! [`RequestReplyPublish`] for the broker capabilities a body drives.
//!
//! Each refinement is the typed twin of one broker capability. The broker trait is what a broker
//! crate implements on its live publisher and what the include site's policy is checked against;
//! the twin is what a body states on the surface it holds - a wired slot entry
//! (`Slot<Marker, W, E>: TransactionalPublish`) or a typed wiring it is handed
//! (`Transactional<P, C>: TransactionalPublish`) - and it offers that capability's typed
//! operations over the surface's codec and dictionary, the same ones the typed wirings offer
//! inherently. A body never names the broker trait, and the check against the policy happens
//! once, at compile time, where the slot is bound.
//!
//! | broker capability | typed twin | what the bound gives |
//! |---|---|---|
//! | [`Publisher`] | [`Publish`] | `message(..)`, the publish builder |
//! | [`TransactionalPublisher`] | [`TransactionalPublish`] | `begin()` -> [`TransactionScope`] |
//! | [`OwnedTransactions`] | [`OwnedTransactionalPublish`] | `transaction()` -> [`TypedTransaction`] |
//! | [`RequestReply`] | [`RequestReplyPublish`] | `request(msg, timeout)` -> the reply |
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "json"))]
//! # mod demo {
//! use ruststream::prelude::*;
//! # #[derive(serde::Deserialize, schemars::JsonSchema)]
//! # struct Order { id: u64 }
//! # #[derive(serde::Serialize, schemars::JsonSchema)]
//! # struct Event { id: u64 }
//! # impl ruststream::OutgoingDestination for Event { type Form = ruststream::CallerName; }
//! # impl ruststream::MessageHeaders for Event { type Contract = ruststream::NoHeaders; }
//! # struct Journal;
//! # impl OutSlot for Journal { const NAME: &'static str = "Journal"; }
//! # impl ruststream::runtime::PublishedThrough<Journal> for Event {}
//!
//! struct Record;
//!
//! // The bound names the capability, not the broker: the include site's policy has to pair a
//! // transactional publisher, and the body drives the transaction through the entry.
//! impl<W, E> Handle<Order, (), Outs<(Slot<Journal, W, E>,)>> for Record
//! where
//!     Slot<Journal, W, E>: TransactionalPublish,
//! {
//!     async fn handle(
//!         &self,
//!         order: &Order,
//!         outs: &Outs<(Slot<Journal, W, E>,)>,
//!         _ctx: &mut Context<'_>,
//!     ) -> Result<(), HandlerOutcome> {
//!         let Ok(scope) = outs.get(Journal).begin().await else {
//!             return Err(HandlerOutcome::retry());
//!         };
//!         let event = Event { id: order.id };
//!         if scope.message(&event).to("journal").publish().await.is_err()
//!             || scope.message(&event).to("journal.mirror").publish().await.is_err()
//!             || scope.commit().await.is_err()
//!         {
//!             return Err(HandlerOutcome::retry());
//!         }
//!         Ok(())
//!     }
//! }
//! # }
//! ```

use std::future::Future;
use std::time::Duration;

use crate::codec::Codec;
use crate::runtime::publish::{
    Admits, TransactionScope, Transactional, TypedPublisher, TypedTransaction,
};
use crate::runtime::slot::{ContainsMessage, OutSlot, PublishedThrough, SlotPublisher};
use crate::{OutgoingMessage, OwnedTransactions, Publisher, RequestReply, TransactionalPublisher};

use super::outs::Slot;

/// The publish capability of a typed surface: what a body's mandatory bound names to start
/// typed publishes through a wired slot entry, and what the typed wirings carry inherently.
///
/// On a slot the bound is stated on the whole entry (`Slot<Marker, W, E>: Publish`), and holds
/// exactly when the bound policy's live form is a [`Publisher`] and the include site's codec
/// encodes. The broker capabilities a body drives through the entry have typed twins refining
/// this one - [`TransactionalPublish`], [`OwnedTransactionalPublish`], [`RequestReplyPublish`] -
/// stated the same way; a broker-defined capability still pins the entry to the broker's live
/// type or bounds `W` with the broker's own trait.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a typed publishing surface",
    note = "a slot body's publish bound is stated on the whole entry: `impl<W, E> Handle<T, (), \
            Outs<(Slot<Marker, W, E>,)>> for Body where Slot<Marker, W, E>: Publish`"
)]
// `Sized`: a transaction opened on the surface carries the surface's type as its dictionary gate
// (see `Admits`), so the surface has to be nameable as a type argument.
pub trait Publish: Sized + Send + Sync {
    /// The live publisher under the surface (on a slot, the attributed one). The bound is stated
    /// here so a body generic over the surface can drive the whole publish builder off the one
    /// `Publish` bound.
    #[doc(hidden)]
    type Leaf: Publisher;

    /// The codec typed publishes encode with: the include site's on a slot, the wiring's own on
    /// a typed publisher.
    #[doc(hidden)]
    type EncodeCodec: Codec + Send + Sync;

    #[doc(hidden)]
    fn leaf(&self) -> &Self::Leaf;

    #[doc(hidden)]
    fn encode_codec(&self) -> &Self::EncodeCodec;
}

/// The error of the publisher under a typed surface: what its publishes and transactions fail
/// with.
pub type ErrorOf<S> = <<S as Publish>::Leaf as Publisher>::Error;

/// The transaction scope a typed surface opens: a [`TransactionScope`] over the surface's
/// publisher and codec, gated by the surface itself.
pub type ScopeOf<'a, S> =
    TransactionScope<'a, <S as Publish>::Leaf, <S as Publish>::EncodeCodec, S>;

/// The owned transaction a typed surface opens: a [`TypedTransaction`] over the publisher's
/// transaction value and the surface's codec, gated by the surface itself.
pub type OwnedTransactionOf<'a, S> = TypedTransaction<
    'a,
    <<S as Publish>::Leaf as OwnedTransactions>::Transaction,
    <S as Publish>::EncodeCodec,
    S,
>;

/// The reply a typed surface's request resolves to: the broker's own delivered message.
pub type ReplyOf<S> = <<S as Publish>::Leaf as RequestReply>::Reply;

/// The typed twin of [`TransactionalPublisher`]: the borrowed transaction kind, driven through
/// a [`TransactionScope`] exactly as on a [`Transactional`] wiring.
///
/// On a slot the bound is stated on the whole entry (`Slot<Marker, W, E>: TransactionalPublish`)
/// and holds when the marker's policy pairs a transactional live publisher; the include site is
/// where a policy without transactions fails to compile. [`begin`](Self::begin) claims the
/// handle's single broker-side transaction, so one scope per entry is open at a time, and the
/// scope's typed entry admits what the slot's own does (the scope carries the surface it was
/// opened on as its last type argument, which is what gates it). Publishes issued through the
/// scope keep the slot's test-capture attribution.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no broker-side transaction to begin",
    note = "the slot's marker is bound to a policy without transactions: attach one whose live \
            publisher is transactional (a transactional producer configuration), and state the \
            bound on the whole entry, `where Slot<Marker, W, E>: TransactionalPublish`"
)]
pub trait TransactionalPublish: Publish<Leaf: TransactionalPublisher> {
    /// Opens a broker transaction and returns the [`TransactionScope`] that owns it: publishes
    /// go through the scope, and [`commit`](TransactionScope::commit) or
    /// [`abort`](TransactionScope::abort) consume it, so a commit without a begin, a second
    /// commit, or a publish after settling do not compile.
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to start a transaction, or when
    /// one is already open on this handle.
    fn begin(&self) -> impl Future<Output = Result<ScopeOf<'_, Self>, ErrorOf<Self>>> + Send;
}

/// The typed twin of [`OwnedTransactions`]: the owned transaction kind, driven through a
/// [`TypedTransaction`] exactly as on a [`TypedPublisher`].
///
/// On a slot the bound is stated on the whole entry
/// (`Slot<Marker, W, E>: OwnedTransactionalPublish`) and holds when the marker's policy pairs a
/// live publisher whose transactions are client buffers. Every
/// [`transaction`](Self::transaction) call opens its own independent transaction, so any number
/// can be open on one entry at a time; the transaction's typed entry admits what the slot's own
/// does. The buffer settles outside the slot, so its publishes land in the broker's publish log
/// and are not attributed to the slot (the documented capture boundary).
#[diagnostic::on_unimplemented(
    message = "`{Self}` opens no caller-owned transactions",
    note = "the slot's marker is bound to a policy without owned transactions: attach one whose \
            live publisher buffers client-side transactions (Kafka-like brokers offer only the \
            borrowed `TransactionalPublish` kind), and state the bound on the whole entry, \
            `where Slot<Marker, W, E>: OwnedTransactionalPublish`"
)]
pub trait OwnedTransactionalPublish: Publish<Leaf: OwnedTransactions> {
    /// Opens an owned broker transaction and returns the [`TypedTransaction`] that owns it:
    /// publishes buffer into the value, and [`commit`](TypedTransaction::commit) or
    /// [`abort`](TypedTransaction::abort) consume it.
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to open a transaction; pure
    /// client-buffer implementations are infallible in practice.
    fn transaction(
        &self,
    ) -> impl Future<Output = Result<OwnedTransactionOf<'_, Self>, ErrorOf<Self>>> + Send;
}

/// The typed twin of [`RequestReply`]: request / reply through the entry, with the slot's
/// test-capture attribution.
///
/// On a slot the bound is stated on the whole entry (`Slot<Marker, W, E>: RequestReplyPublish`)
/// and holds when the marker's policy pairs a live publisher that correlates replies natively.
/// The request is the capability's own operation - an assembled [`OutgoingMessage`] - since the
/// reply is broker-shaped and carries no declaration to resolve a destination or a codec from.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not correlate replies",
    note = "the slot's marker is bound to a policy without request / reply: attach one whose \
            live publisher correlates replies natively (NATS-style; Kafka and classic queues do \
            not), and state the bound on the whole entry, `where Slot<Marker, W, E>: \
            RequestReplyPublish`"
)]
pub trait RequestReplyPublish: Publish<Leaf: RequestReply> {
    /// Publishes `msg` and awaits a single correlated reply, or fails after `timeout`.
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker rejects the publish, the reply times out,
    /// or the transport fails before a reply arrives.
    fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> impl Future<Output = Result<ReplyOf<Self>, ErrorOf<Self>>> + Send;
}

// ------------------------------------------------------------------------------ slot entries

impl<M: OutSlot, W: Publisher, E: Codec + Send + Sync, Body> Publish for Slot<M, W, E, Body> {
    type Leaf = SlotPublisher<W, M>;
    type EncodeCodec = E;

    fn leaf(&self) -> &SlotPublisher<W, M> {
        &self.wired
    }

    fn encode_codec(&self) -> &E {
        &self.codec
    }
}

// A transaction opened on an entry admits exactly what the entry's own typed publish admits:
// the marker's dictionary, narrowed by the parameter's declared set. No bound on `W` or `E`:
// a body generic over the entry knows neither, and the gate must resolve for it.
impl<M, W, E, Body, T, Index> Admits<T, Index> for Slot<M, W, E, Body>
where
    T: PublishedThrough<M>,
    Body: ContainsMessage<T, Index>,
{
}

impl<M: OutSlot, W: TransactionalPublisher, E: Codec + Send + Sync, Body> TransactionalPublish
    for Slot<M, W, E, Body>
{
    async fn begin(&self) -> Result<TransactionScope<'_, SlotPublisher<W, M>, E, Self>, W::Error> {
        TransactionScope::open(&self.wired, &self.codec).await
    }
}

impl<M: OutSlot, W: OwnedTransactions, E: Codec + Send + Sync, Body> OwnedTransactionalPublish
    for Slot<M, W, E, Body>
{
    async fn transaction(&self) -> Result<TypedTransaction<'_, W::Transaction, E, Self>, W::Error> {
        TypedTransaction::open(&self.wired, &self.codec).await
    }
}

impl<M: OutSlot, W: RequestReply, E: Codec + Send + Sync, Body> RequestReplyPublish
    for Slot<M, W, E, Body>
{
    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<W::Reply, W::Error> {
        self.wired.request(msg, timeout).await
    }
}

// ------------------------------------------------------------------------------ typed wirings

// The typed wirings carry the same vocabulary, so a function handed one live (the scope's
// `after_startup`) states the capability it needs without naming the broker trait.
impl<P, C, PL, BL> Publish for TypedPublisher<P, C, PL, BL>
where
    P: Publisher,
    C: Codec + Send + Sync,
    PL: Send + Sync,
    BL: Send + Sync,
{
    type Leaf = P;
    type EncodeCodec = C;

    fn leaf(&self) -> &P {
        self.publisher()
    }

    fn encode_codec(&self) -> &C {
        self.codec()
    }
}

impl<P, C, PL, BL> OwnedTransactionalPublish for TypedPublisher<P, C, PL, BL>
where
    P: OwnedTransactions,
    C: Codec + Send + Sync,
    PL: Send + Sync,
    BL: Send + Sync,
{
    async fn transaction(&self) -> Result<TypedTransaction<'_, P::Transaction, C, Self>, P::Error> {
        Self::transaction(self).await
    }
}

impl<P, C, PL, BL> Publish for Transactional<P, C, PL, BL>
where
    P: Publisher,
    C: Codec + Send + Sync,
    PL: Send + Sync,
    BL: Send + Sync,
{
    type Leaf = P;
    type EncodeCodec = C;

    fn leaf(&self) -> &P {
        self.inner().publisher()
    }

    fn encode_codec(&self) -> &C {
        self.inner().codec()
    }
}

impl<P, C, PL, BL> TransactionalPublish for Transactional<P, C, PL, BL>
where
    P: TransactionalPublisher,
    C: Codec + Send + Sync,
    PL: Send + Sync,
    BL: Send + Sync,
{
    async fn begin(&self) -> Result<TransactionScope<'_, P, C, Self>, P::Error> {
        Self::begin(self).await
    }
}
