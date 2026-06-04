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
    BatchPublisher, BatchSubscriber, DescribeServer, Partitioned, RequestReply, ServerSpec,
    Subscribe, TransactionalPublisher,
};
pub use error::AckError;
pub use headers::Headers;
pub use message::{IncomingMessage, OutgoingMessage, RawMessage};
pub use publisher::Publisher;
pub use schema::Message;
pub use subscriber::Subscriber;
pub use subscription::{Name, SubscriptionSource};

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

#[cfg(feature = "asyncapi")]
pub mod asyncapi;

/// Re-export of [`schemars`] so message types can derive `JsonSchema` without a direct dependency.
///
/// Derive it on a message type (`#[derive(ruststream::schemars::JsonSchema)]`) and its payload
/// schema is emitted into the generated [`AsyncAPI`](asyncapi) document. Available with the
/// `asyncapi` feature.
#[cfg(feature = "asyncapi")]
pub use schemars;

#[cfg(feature = "metrics")]
pub mod metrics;

/// Implementation detail used by the `#[subscriber]` macro to capture a payload's JSON Schema.
///
/// Not part of the public API; no stability guarantees.
#[doc(hidden)]
pub mod __private {
    use core::marker::PhantomData;

    /// A type-carrying probe the macro reads a payload schema off.
    ///
    /// Schema selection uses inherent-vs-trait specialization (a stable-Rust trick): the schema
    /// path is an inherent method on `Probe<T>` bounded by `T: JsonSchema`, and
    /// [`NoSchemaProbe::schema_json`] is the trait fallback. Inherent methods win when present, so
    /// `Probe::<T>::new().schema_json()` returns the schema for a concrete `T: JsonSchema` and
    /// `None` otherwise — without forcing the bound onto every message type. The inherent method
    /// exists only with the `asyncapi` feature.
    #[derive(Debug)]
    pub struct Probe<T>(pub PhantomData<T>);

    impl<T> Probe<T> {
        /// Constructs a probe for `T`.
        #[must_use]
        pub const fn new() -> Self {
            Self(PhantomData)
        }
    }

    impl<T> Default for Probe<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    /// The trait fallback: chosen for any `T` the inherent schema method does not cover.
    pub trait NoSchemaProbe {
        /// Returns `None` (no schema available for the probed type).
        fn schema_json(&self) -> Option<String>;
    }

    impl<T> NoSchemaProbe for Probe<T> {
        fn schema_json(&self) -> Option<String> {
            None
        }
    }

    #[cfg(feature = "asyncapi")]
    impl<T: schemars::JsonSchema> Probe<T> {
        /// Returns the serialized JSON Schema for `T` (inherent; preferred over the trait fallback).
        #[must_use]
        pub fn schema_json(&self) -> Option<String> {
            serde_json::to_string(&schemars::schema_for!(T)).ok()
        }
    }
}
