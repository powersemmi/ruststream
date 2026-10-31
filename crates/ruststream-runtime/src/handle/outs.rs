//! The injections arena: the second body argument, built statically by the include site's
//! `.out(marker, policy)` chain.
//!
//! A body that publishes declares the arena in its `O` position - one entry per marker, each a
//! type parameter bounded with [`OutEntry`] at the marker it stands for, carrying the broker
//! capability the body needs on the wired value and nothing else:
//!
//! ```
//! # #[cfg(feature = "json")]
//! # mod demo {
//! use ruststream::prelude::*;
//! # #[derive(serde::Deserialize, schemars::JsonSchema)]
//! # struct Order { id: u64 }
//! # #[derive(serde::Serialize, schemars::JsonSchema)]
//! # struct Event { id: u64 }
//! # impl OutgoingDestination for Event { type Form = CallerName; }
//! # impl MessageHeaders for Event { type Contract = NoHeaders; }
//! # struct Primary;
//! # impl OutSlot for Primary { const NAME: &'static str = "Primary"; type Destination = Reads; }
//! # impl PublishedThrough<Primary> for Event {}
//!
//! struct Mirror;
//!
//! impl<P> Handle<Order, (), Outs<(P,)>> for Mirror
//! where
//!     P: OutEntry<Primary, Wire: Publisher>,
//! {
//!     async fn handle(
//!         &self,
//!         order: &Order,
//!         outs: &Outs<(P,)>,
//!         _ctx: &mut Context<'_>,
//!     ) -> Result<(), HandlerOutcome> {
//!         if outs
//!             .get(Primary)
//!             .message(&Event { id: order.id })
//!             .to("mirror")
//!             .publish()
//!             .await
//!             .is_err()
//!         {
//!             return Err(HandlerOutcome::retry());
//!         }
//!         Ok(())
//!     }
//! }
//! # }
//! ```
//!
//! The include site binds each marker with `.out(marker, policy)` in any order and seals with
//! `.build()`; a missing, duplicate or extra binding, or a policy whose live form lacks the
//! body's declared capability, fails to compile naming the marker. The capability a body states
//! is the broker vocabulary and never a broker type, so the same body mounts on a production
//! broker and on its in-process test transport unchanged. Under each of the publisher
//! capabilities the entry offers that capability's typed form, over the include site's codec and
//! the marker's `#[publishes(..)]` dictionary:
//!
//! | broker capability | typed operation on the entry | what it gives |
//! |---|---|---|
//! | [`Publisher`] | [`message`](Slot::message) | the publish builder |
//! | [`TransactionalPublisher`] | [`begin`](Slot::begin) | a [`TransactionScope`] |
//! | [`OwnedTransactions`] | [`transaction`](Slot::transaction) | a [`TypedTransaction`] |
//! | [`RequestReply`] | [`request`](RequestReply::request) | the correlated reply |
//!
//! Each capability is also delegated raw on the entry, through the attributed leaf, so a body
//! driving `begin_transaction` / `commit` / `abort` by hand keeps the slot's test-capture
//! attribution. Where a typed operation and a raw one share a name
//! ([`transaction`](Slot::transaction)), the inherent typed one wins, and the raw form stays
//! reachable as `OwnedTransactions::transaction(entry)`.
//!
//! The slot's wired value is the policy's live form itself, so a body needing a broker-defined
//! capability bounds `Wire` with the broker's own trait (or pins the entry to the concrete
//! [`Slot`], `Outs<(Slot<Lanes, LaneRouter, JsonCodec>,)>`) and calls it directly through the
//! entry's transparent `Deref`. Everything is monomorphized: the arena is built once at startup,
//! and a delivery only ever passes a reference to it.
//!
//! A mount site that hands the slot's destination to a transform (a `.transform(..)` step whose
//! transform declares `Destination = Names`) leaves the first row of that table and nothing else: the rows below it reach the broker without the slot's
//! publish path, so their messages would go where that transform never looked, and a body asking
//! for one of them fails to mount.

mod any_entry;
mod arena;
mod cells;
mod entry;
mod local_entry;

pub use any_entry::{AnyOutPos, SelectEntry, SlotEntry, SlotTransactions};
pub use arena::{EntryMarkers, LocalOutPos, OutPos, Outs, SelectSlot, SlotDictionary};
pub use entry::{OutEntry, Slot};
pub use local_entry::{LocalEntryMarkers, LocalOutEntry, LocalSlot};
