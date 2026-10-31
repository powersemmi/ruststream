//! One arena entry: the wired live value of a marker with its publish path, the typed
//! operations each publisher capability offers on it, and the [`OutEntry`] bound a body states.

use alloc::boxed::Box;
use alloc::format;
use core::fmt;
use core::marker::PhantomData;
use core::ops::Deref;
use core::time::Duration;

use crate::codec::Codec;
use crate::inject::FromStartup;
use crate::publish::{
    Admits, HeadersUnset, MessageBody, OutPipeline, PublishBuilder, PublishIdentity,
    TransactionScope, TypedTransaction, message_of,
};
use crate::slot::{ContainsMessage, OutSlot, PublishedThrough, SlotPublisher};
#[cfg(feature = "testing")]
use crate::testing::coordinator::{Origin, paired, publishing_to};
use crate::{
    Connected, HeaderMap, OutgoingDestination, OutgoingFor, OwnedTransactions, PairError,
    PublishPolicy, Publisher, RequestReply, TransactionalPublisher,
};

/// One arena entry: the wired live value of the marker `M`, plus the publish path the include
/// site gave it - its encode codec and the pipeline every message leaving the slot travels.
///
/// The wired value is the bound policy's [`Live`](crate::PublishPolicy::Live) form, and the
/// entry is a transparent window onto it: `Deref` reaches every method the live value offers -
/// the broker capability vocabulary ([`TransactionalPublisher`](crate::TransactionalPublisher),
/// [`OwnedTransactions`](crate::OwnedTransactions), [`RequestReply`](crate::RequestReply)) and
/// any broker-defined capability trait alike, so a body pins the entry to the broker's concrete
/// live type (or bounds [`OutEntry::Wire`] with the broker's trait) and calls it directly. Under
/// each of the core capability bounds the entry also offers that capability's typed form -
/// [`message`](Self::message), [`begin`](Self::begin), [`transaction`](Self::transaction) - over
/// the include site's codec and the marker's dictionary.
///
/// A body does not normally spell this type out: it names one parameter per marker and bounds it
/// with [`OutEntry`], which projects `W`, `E` and `Pipe` so a mount the body never sees cannot
/// change its signature. Spelling the entry is for pinning it to a concrete live value.
///
/// `Pipe` is that publish path: the app's own publish pipeline (the
/// [`publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer) chain) with the slot's
/// `.transform(..)` steps composed on top. It is [`PublishIdentity`] - nothing in the way, the
/// bare leaf call - until a mount site names either.
///
/// Autoderef carries a method call, not a trait bound: a helper written as `fn f<L: Lanes>(l:
/// &L)` rejects the entry a body holds. A broker crate that wants such helpers - or bodies
/// generic over its capability - adds one blanket impl next to its trait,
/// `impl<M, W: Lanes, E, Pipe, Body> Lanes for Slot<M, W, E, Pipe, Body>`, delegating through
/// this `Deref`.
///
/// `Body` is the entry's declared message set, `()` (any dictionary type) unless the
/// `#[subscriber]` parameter's third `Out` position narrows it;
/// [`message`](Slot::message) checks it at compile time (see
/// [`ContainsMessage`](crate::ContainsMessage)).
pub struct Slot<M, W, E, Pipe = PublishIdentity, Body = ()> {
    wired: SlotPublisher<W, M>,
    codec: E,
    pipeline: Pipe,
    _declared: PhantomData<fn() -> Body>,
    /// The broker the slot's publisher was paired against, which its publishes go to, whatever
    /// broker the handler holding it consumes from.
    #[cfg(feature = "testing")]
    origin: Origin,
}

impl<M, W, E, Pipe, Body> fmt::Debug for Slot<M, W, E, Pipe, Body> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot").finish_non_exhaustive()
    }
}

// The transparent window: the wired live value's whole surface (broker-defined capability
// traits included) is reachable without any grafting machinery. Calls that resolve on the
// entry itself (its typed operations, the delegated core capabilities) travel the slot's
// publish path and keep its test-capture attribution; calls reaching the live value through
// this `Deref` leave through the unwrapped value and bypass both, like a settled owned
// transaction's buffer.
impl<M, W, E, Pipe, Body> Deref for Slot<M, W, E, Pipe, Body> {
    type Target = W;

    fn deref(&self) -> &W {
        self.wired.inner()
    }
}

// A transaction opened on an entry admits exactly what the entry's own typed publish admits:
// the marker's dictionary, narrowed by the parameter's declared set. No bound on `W` or `E`:
// a body generic over the entry knows neither, and the gate must resolve for it.
impl<M, W, E, Pipe, Body, T, Index> Admits<T, Index> for Slot<M, W, E, Pipe, Body>
where
    T: PublishedThrough<M>,
    Body: ContainsMessage<T, Index>,
{
}

impl<M: OutSlot, W: Publisher, E: Codec + Send + Sync, Pipe: OutPipeline<W>, Body>
    Slot<M, W, E, Pipe, Body>
{
    /// Starts a typed publish through the slot, on the message type's own wire
    /// ([`MessageWire`](crate::MessageWire)): a `serde::Serialize` value encodes with
    /// the include site's codec, a [`Serialized`](crate::Serialized) one carries its
    /// bytes and they leave as they are. The
    /// message type has to be in the marker's `#[publishes(..)]` dictionary (see
    /// [`PublishedThrough`](crate::PublishedThrough)) and, when the entry carries a
    /// declared message set, in that set (see
    /// [`ContainsMessage`](crate::ContainsMessage)); everything else - the
    /// destination and the header contract - comes from the type's `#[derive(Outgoing)]`
    /// declaration, so the builder demands exactly the positions that declaration leaves open.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::runtime::{HandlerOutcome, Out};
    /// use ruststream::{Outgoing, OutSlot, Publisher, subscriber};
    /// use serde::{Deserialize, Serialize};
    /// # #[derive(serde::Deserialize)]
    /// # struct Event { id: u64 }
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct DoneMeta {
    ///     task_id: u64,
    /// }
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "chunks.progress")]
    /// struct Progress {
    ///     percent: u8,
    /// }
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "chunks.done", headers = DoneMeta)]
    /// struct ChunkDone {
    ///     output_key: String,
    /// }
    ///
    /// #[derive(OutSlot)]
    /// #[publishes(ChunkDone, Progress)]
    /// struct Events;
    ///
    /// #[subscriber("chunks.raw")]
    /// async fn convert(
    ///     event: &Event,
    ///     Out(out): Out<impl Publisher, Events, (ChunkDone, Progress)>,
    /// ) -> HandlerOutcome {
    ///     // No headers contract on Progress: publish straight away.
    ///     if out.message(&Progress { percent: 100 }).publish().await.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     // ChunkDone declares DoneMeta: with_headers is demanded by the contract.
    ///     let done = ChunkDone { output_key: format!("out/{}", event.id) };
    ///     let meta = DoneMeta { task_id: event.id };
    ///     if out.message(&done).with_headers(&meta).publish().await.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// # }
    /// ```
    // The builder rides the entry itself, so a publish issued here travels the slot's publish
    // path - its transforms, then the app-wide pipeline - and is recorded against the slot by
    // the test harness on the way out.
    pub fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a Self, MessageBody<'a, T>, &'a E, HeadersUnset, T::Form>
    where
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination + PublishedThrough<M>,
    {
        message_of(self, value, &self.codec)
    }
}

// The transaction openers do not travel the slot's pipeline, but the entry they borrow crosses
// the await with them, so it still has to be shareable.
impl<M: OutSlot, W: TransactionalPublisher, E: Send + Sync, Pipe: Send + Sync, Body>
    Slot<M, W, E, Pipe, Body>
{
    /// Opens the entry's broker transaction and returns the [`TransactionScope`] that owns it:
    /// publishes go through the scope, and [`commit`](TransactionScope::commit) or
    /// [`abort`](TransactionScope::abort) consume it, so a commit without a begin, a second
    /// commit, or a publish after settling do not compile.
    ///
    /// This is the borrowed kind: the wired publisher carries at most one broker-side
    /// transaction, so one scope per entry is open at a time. The scope's typed entry admits
    /// what the slot's own does (the marker's dictionary, narrowed by the parameter's declared
    /// set), and publishes issued through it keep the slot's test-capture attribution. They go
    /// to the broker as they are: the scope opens on the attributed leaf, so the slot's
    /// transforms and the app-wide pipeline stay on the direct publish path (see
    /// [`TransactionScope`]).
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to start a transaction, or when
    /// one is already open on this handle.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::runtime::{HandlerOutcome, Out};
    /// use ruststream::{Outgoing, OutSlot, TransactionalPublisher, subscriber};
    /// use serde::Serialize;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: u64 }
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "ledger.settled")]
    /// struct Settled {
    ///     id: u64,
    /// }
    ///
    /// #[derive(OutSlot)]
    /// #[publishes(Settled)]
    /// struct Journal;
    ///
    /// #[subscriber("ledger.orders")]
    /// async fn settle(
    ///     order: &Order,
    ///     Out(journal): Out<impl TransactionalPublisher, Journal, Settled>,
    /// ) -> HandlerOutcome {
    ///     let Ok(scope) = journal.begin().await else {
    ///         return HandlerOutcome::retry();
    ///     };
    ///     if scope.message(&Settled { id: order.id }).publish().await.is_err()
    ///         || scope.commit().await.is_err()
    ///     {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// # }
    /// ```
    pub async fn begin(
        &self,
    ) -> Result<TransactionScope<'_, SlotPublisher<W, M>, &'_ E, Self>, W::Error> {
        TransactionScope::open(&self.wired, &self.codec).await
    }
}

impl<M: OutSlot, W: OwnedTransactions, E: Send + Sync, Pipe: Send + Sync, Body>
    Slot<M, W, E, Pipe, Body>
{
    /// Opens an independent, caller-owned broker transaction and returns the
    /// [`TypedTransaction`] that owns it: publishes buffer into the value, and
    /// [`commit`](TypedTransaction::commit) or [`abort`](TypedTransaction::abort) consume it.
    ///
    /// Every call opens its own transaction, so any number can be open on one entry at a time;
    /// the transaction's typed entry admits what the slot's own does. The buffer settles outside
    /// the slot, so its publishes land in the broker's publish log and are not attributed to the
    /// slot (the documented capture boundary).
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to open a transaction; pure
    /// client-buffer implementations are infallible in practice.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::runtime::{HandlerOutcome, Out};
    /// use ruststream::{Outgoing, OutSlot, OwnedTransactions, subscriber};
    /// use serde::Serialize;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: u64 }
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "ledger.settled")]
    /// struct Settled {
    ///     id: u64,
    /// }
    ///
    /// #[derive(OutSlot)]
    /// #[publishes(Settled)]
    /// struct Ledger;
    ///
    /// #[subscriber("ledger.orders")]
    /// async fn settle(
    ///     order: &Order,
    ///     Out(ledger): Out<impl OwnedTransactions, Ledger, Settled>,
    /// ) -> HandlerOutcome {
    ///     let Ok(mut txn) = ledger.transaction().await else {
    ///         return HandlerOutcome::retry();
    ///     };
    ///     if txn.message(&Settled { id: order.id }).publish().await.is_err()
    ///         || txn.commit().await.is_err()
    ///     {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// # }
    /// ```
    // Shares its name with `OwnedTransactions::transaction`, which the entry also implements:
    // the inherent typed form is what a body wants, and the raw one stays reachable through the
    // trait path.
    pub async fn transaction(
        &self,
    ) -> Result<TypedTransaction<W::Transaction, &'_ E, Self>, W::Error> {
        TypedTransaction::open(&self.wired, &self.codec).await
    }
}

// The broker capability vocabulary is also delegated on the entry itself (not only through
// Deref), so an entry passes into generic positions demanding the capability and a direct
// `publish` / `request` keeps the slot's test-capture attribution.
//

// `publish` is the one that travels the slot's publish path: the pipeline runs above the
// attributed leaf, so what the harness records and what the broker receives are the same
// stamped message. The transaction calls and the request round trip reach the leaf directly -
// the pipeline ends in a send, and neither of those is one - and report their errors in the
// entry's own error type.
impl<M: OutSlot, W: Publisher, E: Send + Sync, Pipe: OutPipeline<W>, Body> Publisher
    for Slot<M, W, E, Pipe, Body>
{
    // The slot is attribution, not policy: the payload travels in the wired publisher's own
    // form, so what a transform above it sees is what the broker will be handed.
    type Payload = W::Payload;
    type Error = Pipe::Error<W::Error>;
    // The slot is attribution, not policy: the broker's per-message settings are the wired
    // publisher's, which is what puts that broker's builder steps on this entry's `message(..)`.
    type Options = W::Options;

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let publish = self.pipeline.send(&self.wired, msg, options);
        #[cfg(feature = "testing")]
        let publish = publishing_to(self.origin, publish);
        publish.await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.wired.base_headers()
    }
}

impl<M: OutSlot, W: TransactionalPublisher, E: Send + Sync, Pipe: OutPipeline<W>, Body>
    TransactionalPublisher for Slot<M, W, E, Pipe, Body>
{
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.wired
            .begin_transaction()
            .await
            .map_err(Pipe::from_publish_error)
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.wired.commit().await.map_err(Pipe::from_publish_error)
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.wired.abort().await.map_err(Pipe::from_publish_error)
    }
}

impl<M: OutSlot, W: OwnedTransactions, E: Send + Sync, Pipe: OutPipeline<W>, Body> OwnedTransactions
    for Slot<M, W, E, Pipe, Body>
{
    type Transaction = W::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.wired
            .transaction()
            .await
            .map_err(Pipe::from_publish_error)
    }
}

impl<M: OutSlot, W: RequestReply, E: Send + Sync, Pipe: OutPipeline<W>, Body> RequestReply
    for Slot<M, W, E, Pipe, Body>
{
    type Reply = W::Reply;

    async fn request(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let request = self.wired.request(msg, timeout);
        #[cfg(feature = "testing")]
        let request = publishing_to(self.origin, request);
        request.await.map_err(Pipe::from_publish_error)
    }
}

impl<M, W, E, Pipe, Body> Slot<M, W, E, Pipe, Body> {
    /// Builds an entry over an already-paired live value, for the crate's own unit tests.
    #[cfg(all(test, feature = "json"))]
    pub(crate) fn wired(wired: W, codec: E, pipeline: Pipe) -> Self {
        Self {
            wired: SlotPublisher::new(wired),
            codec,
            pipeline,
            _declared: PhantomData,
            #[cfg(feature = "testing")]
            origin: Origin::default(),
        }
    }
}

/// The injected slot: pairs the marker's attached policy against the connected broker and
/// stores the live value under the include site's publish path (its encode codec and the
/// pipeline the mount composed). A failing pair surfaces at startup with the slot's name; an
/// unbound slot never gets this far (the include site does not compile).
impl<B, Sub, Policy, E, Pipe, M, Body> FromStartup<B, Sub, (Policy, E, Pipe)>
    for Slot<M, <Policy as PublishPolicy<Connected<B>>>::Live, E, Pipe, Body>
where
    B: crate::Broker,
    Sub: Sync,
    Policy: PublishPolicy<Connected<B>> + Send,
    E: Send,
    Pipe: Send,
    M: OutSlot,
{
    async fn resolve(
        (policy, codec, pipeline): (Policy, E, Pipe),
        connected: &Connected<B>,
        _subscriber: &Sub,
    ) -> Result<Self, PairError> {
        let pairing = policy.pair(connected);
        #[cfg(feature = "testing")]
        let (live, origin) = paired(connected, pairing).await;
        #[cfg(not(feature = "testing"))]
        let live = pairing.await;
        let live = live.map_err(|err| {
            PairError::from_boxed(Box::from(format!(
                "pairing the publisher for the `{}` slot failed: {err}",
                M::NAME,
            )))
        })?;
        Ok(Self {
            wired: SlotPublisher::new(live),
            codec,
            pipeline,
            _declared: PhantomData,
            #[cfg(feature = "testing")]
            origin,
        })
    }
}

/// What one arena entry is, stated as the mount's choices rather than spelled out: the bound
/// marker `M`, and the wired value, codec and publish path the include site gave it.
///
/// This is how a body declares its arena. It names one type parameter per marker and bounds it
/// here, with the broker capability it needs on [`Wire`](Self::Wire) and nothing else:
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::prelude::*;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: u64 }
/// # #[derive(serde::Serialize, schemars::JsonSchema)]
/// # struct Event { id: u64 }
/// # impl OutgoingDestination for Event { type Form = CallerName; }
/// # impl MessageHeaders for Event { type Contract = NoHeaders; }
/// # struct Audit;
/// # impl OutSlot for Audit { const NAME: &'static str = "Audit"; type Destination = Reads; }
/// # impl PublishedThrough<Audit> for Event {}
/// # struct Journal;
/// # impl OutSlot for Journal { const NAME: &'static str = "Journal"; type Destination = Reads; }
///
/// struct Route;
///
/// impl<A, J> Handle<Order, (), Outs<(A, J)>> for Route
/// where
///     A: OutEntry<Audit, Wire: Publisher>,
///     J: OutEntry<Journal, Wire: TransactionalPublisher>,
/// {
///     async fn handle(
///         &self,
///         order: &Order,
///         outs: &Outs<(A, J)>,
///         _ctx: &mut Context<'_>,
///     ) -> Result<(), HandlerOutcome> {
///         let scope = outs.get(Journal).begin().await;
///         let _ = scope.is_ok();
///         if outs
///             .get(Audit)
///             .message(&Event { id: order.id })
///             .to("audit")
///             .publish()
///             .await
///             .is_err()
///         {
///             return Err(HandlerOutcome::retry());
///         }
///         Ok(())
///     }
/// }
/// # }
/// ```
///
/// The three projections are the include site's business, so nothing the mount does reaches the
/// body's signature: a `.transform(..)` on the slot, an app-wide
/// [`publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer), a `.codec(..)` override, or a
/// different broker behind the policy all land in [`Pipe`](Self::Pipe) and [`Enc`](Self::Enc)
/// and leave the `where` clause alone.
///
/// `Body` is the body's own declaration and not the mount's, which is why it is a parameter and
/// not a projection: it is the message set the entry's typed publish admits, `()` (whatever the
/// marker's `#[publishes(..)]` dictionary holds) unless a body narrows it, and a body that
/// publishes has to know it to name a type at all.
///
/// [`Outs::get`](super::Outs::get) hands back the entry itself, so
/// [`message`](Slot::message) / [`begin`](Slot::begin) / [`transaction`](Slot::transaction) /
/// [`request`](RequestReply::request) and the live value's own surface behind the entry's
/// `Deref` read exactly as they do on a spelled-out arena.
// `Send + Sync` on the trait and on the wired value, rather than on every body's `where`: the
// arena is built once at startup and shared by reference across the subscription's worker tasks,
// so an entry that is not shareable never reaches a body in the first place.
pub trait OutEntry<M, Body = ()>: Send + Sync {
    /// The wired live value: the bound policy's [`Live`](crate::PublishPolicy::Live) form, which
    /// a body bounds with the broker capability it needs.
    type Wire: Send + Sync;

    /// The codec the entry's typed publishes encode with: the include site's own, or the one a
    /// `.codec(..)` named for this slot.
    type Enc: Codec;

    /// The publish path every message leaving the entry travels: the app's publish pipeline with
    /// the slot's `.transform(..)` steps composed on top. It is stated over the wired value,
    /// because a transform there writes that publisher's own per-message options.
    type Pipe: OutPipeline<Self::Wire>;

    /// The entry behind the bound. Machinery: [`Outs::get`](super::Outs::get) resolves the concrete slot through
    /// it, and only a [`Slot`] can produce one.
    #[doc(hidden)]
    fn entry(&self) -> &Slot<M, Self::Wire, Self::Enc, Self::Pipe, Body>;
}

impl<M, W, E, Pipe, Body> OutEntry<M, Body> for Slot<M, W, E, Pipe, Body>
where
    W: Send + Sync,
    E: Codec,
    Pipe: OutPipeline<W>,
{
    type Wire = W;
    type Enc = E;
    type Pipe = Pipe;

    fn entry(&self) -> &Self {
        self
    }
}
