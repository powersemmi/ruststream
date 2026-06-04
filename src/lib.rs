//! Rust core of the [`RustStream`](https://github.com/ruststream/ruststream-rs) messaging
//! framework: broker-agnostic traits, message types, codecs, router runtime, and a
//! conformance harness for broker authors.
//!
//! # Cargo features
//!
//! Every feature is additive. Default features cover the common path: typed messages, a
//! router, and JSON codec. Broker authors enable `conformance` to pick up the in-memory
//! reference broker and the contract harness. Codec features are mutually compatible and
//! enable only the deserializers you need.
//!
//! * `runtime` (default): [`runtime::RustStream`] plus middleware, lifecycle, and dispatch.
//! * `json` (default): [`codec::JsonCodec`].
//! * `msgpack`: [`codec::MsgpackCodec`].
//! * `cbor`: [`codec::CborCodec`].
//! * `memory`: [`memory::MemoryBroker`], an in-process broker usable in applications,
//!   prototypes and tests.
//! * `conformance`: the [`conformance::harness`] contract suite and broker-agnostic
//!   [`conformance::helpers`] for application tests. Generic over any broker's `TestClient`,
//!   so it pulls in no concrete broker (enable `memory` too to run it against
//!   [`memory::MemoryBroker`]).
//!
//! Disable defaults (`default-features = false`) to depend only on the core traits, with no
//! runtime, no codecs, and no Tokio. Useful for crates that only consume the trait surface
//! (broker authors implementing their own [`Broker`] from scratch).

#![forbid(unsafe_code)]

mod broker;
mod capability;
mod error;
mod headers;
mod message;
mod publisher;
mod schema;
mod subscriber;
mod subscription;
pub mod testing;

pub use broker::Broker;
pub use capability::{
    BatchPublisher, BatchSubscriber, Partitioned, RequestReply, Subscribe, TransactionalPublisher,
};
pub use error::AckError;
pub use headers::Headers;
pub use message::{IncomingMessage, OutgoingMessage, RawMessage};
pub use publisher::Publisher;
pub use schema::Message;
pub use subscriber::Subscriber;
pub use subscription::{SubscriptionSource, Topic};

pub mod codec;

#[cfg(feature = "memory")]
pub mod memory;

#[cfg(feature = "runtime")]
pub mod runtime;

#[cfg(feature = "runtime")]
pub use runtime::RustStream;

/// Attribute macro that turns an `async fn` into a mountable subscriber definition.
///
/// Available with the `macros` feature. See [`ruststream_macros::subscriber`].
#[cfg(feature = "macros")]
pub use ruststream_macros::subscriber;

/// Derive macro for [`Message`] metadata (type name + doc description).
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::Message;

#[cfg(feature = "conformance")]
pub mod conformance;
