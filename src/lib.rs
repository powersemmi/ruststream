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
//! * `runtime` (default): [`runtime::Router`] plus middleware, lifecycle, and dispatch.
//! * `json` (default): [`codec::JsonCodec`].
//! * `msgpack`: [`codec::MsgpackCodec`].
//! * `cbor`: [`codec::CborCodec`].
//! * `conformance`: [`conformance::MemoryBroker`] reference implementation, the
//!   [`conformance::harness`] contract suite, and broker-agnostic
//!   [`conformance::helpers`] for application tests.
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
mod subscriber;
pub mod testing;

pub use broker::Broker;
pub use capability::{BatchSubscriber, Partitioned, RequestReply, TransactionalPublisher};
pub use error::AckError;
pub use headers::Headers;
pub use message::{IncomingMessage, OutgoingMessage, RawMessage};
pub use publisher::Publisher;
pub use subscriber::Subscriber;

pub mod codec;

#[cfg(feature = "runtime")]
pub mod runtime;

#[cfg(feature = "conformance")]
pub mod conformance;
