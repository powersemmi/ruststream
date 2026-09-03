//! The imports a service mounting on the in-memory broker writes, in one glob.
//!
//! `use ruststream::memory::prelude::*;` is the broker-crate prelude shape, and every broker
//! crate ships the same three layers: the core prelude, re-exported so one glob serves the
//! whole file; the broker's own surface a service names (the broker, its subscription source,
//! its per-delivery context keys); and the broker's publish policies under the uniform names a
//! mount site writes - [`Publish`], [`TransactionalPublish`], [`Request`]. Swapping brokers then
//! swaps the glob, not the mount sites.
//!
//! A handler body keeps `use ruststream::prelude::*;`: it states the broker capability its slot
//! needs (`Out<impl TransactionalPublisher>`) and never a policy, so it does not know which
//! broker runs it. The mount site is where a policy is named, and that is where this glob
//! belongs; a file holding both is served by this one alone.
//!
//! # Examples
//!
//! ```
//! # #[cfg(all(feature = "macros", feature = "json"))]
//! # mod demo {
//! use ruststream::memory::prelude::*;
//! # #[derive(serde::Deserialize)]
//! # struct Order { id: u64 }
//!
//! # #[ruststream::subscriber("orders")]
//! # async fn audit(order: &Order, Out(_out): Out<impl Publisher>) -> HandlerOutcome {
//! #     let _ = order.id;
//! #     HandlerOutcome::ack()
//! # }
//! fn app() -> RustStream {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
//!         b.include(audit).publisher(Publish);
//!     })
//! }
//! # }
//! ```

pub use crate::prelude::*;

// The broker's own surface: what a service names to build the app and to read a delivery's
// position. The publisher, requester and message types stay explicit imports - a service that
// names them has left the broker-agnostic path.
pub use super::{
    MemoryBatchContext, MemoryBroker, MemoryContext, MemoryError, MemoryPosition, MemorySource,
    Position, SeekHandle,
};

// The policies under the names every broker's prelude uses, so a mount site reads the same
// whichever broker it is on. `TransactionalPublish` is the same policy as `Publish` here: the
// in-memory publisher carries both transaction kinds, so there is no separate transactional
// configuration to pair. All three are unit structs, so a mount site writes the bare name; a
// broker whose policy carries options names it the same way and constructs it its own way.
pub use super::{
    MemoryPublish as Publish, MemoryPublish as TransactionalPublish, MemoryRequest as Request,
};

// The capability manifest: the core traits this broker implements on its live values, so the
// glob that names the policies also puts their operations in scope. `Partitioned` is absent on
// purpose - in scope it makes `msg.partition_key()` ambiguous with the defaulted
// `IncomingMessage` method, so a service reading partition keys imports it explicitly.
pub use crate::{
    OwnedTransactions, Positioned, RequestReply, Seeker, Transaction, TransactionalPublisher,
};
