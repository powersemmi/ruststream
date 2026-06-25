//! Rust core of the [`RustStream`](https://github.com/powersemmi/ruststream) messaging
//! framework: broker-agnostic traits, message types, codecs, router runtime, and a
//! conformance harness for broker authors.
//!
//! # Cargo features
//!
//! The core traits, the [`runtime::RustStream`] application object, middleware, and dispatch are
//! always present. The rest is additive and opt-in. Codec features are mutually compatible and
//! enable only the deserializers you need.
//!
//! * `json` (default): [`codec::JsonCodec`].
//! * `msgpack`: [`codec::MsgpackCodec`].
//! * `cbor`: [`codec::CborCodec`].
//! * `memory`: [`memory::MemoryBroker`], an in-process broker usable in applications, prototypes
//!   and tests.
//! * `macros`: the `#[subscriber]`, [`#[ruststream::app]`](macro@app), and
//!   [`#[derive(Message)]`](macro@Message) macros.
//! * `asyncapi`: `AsyncAPI` document generation and the HTML viewer.
//! * `metrics`: Prometheus metrics middleware and exporter.
//! * `logging`: colored, `RUST_LOG`-driven console logging via `tracing-subscriber`
//!   ([`logging::init`]). The generated `cli` `run` command installs it automatically.
//! * `conformance`: the [`conformance::harness`] contract suite, per-capability suites in
//!   [`conformance::capabilities`], and broker-agnostic [`conformance::helpers`] for application
//!   tests. Generic over any broker's [`testing::TestableBroker`], so it pulls in no concrete broker
//!   (enable `memory` too to run it against [`memory::MemoryBroker`]).
//! * `testing`: the [`testing::TestApp`] in-process harness for application unit tests.
//! * `cli`: the `ruststream` binary (`run`, `asyncapi gen`, `new`).
//!
//! Disable defaults (`default-features = false`) to drop the bundled JSON codec; the core traits,
//! runtime, and dispatch remain. Add back only what you need.

#![forbid(unsafe_code)]

mod broker;
mod buffered;
mod capability;
mod error;
mod field;
mod headers;
mod message;
mod publisher;
mod schema;
mod subscriber;
mod subscription;
pub mod testing;

/// Re-exported for the [`register_testable_broker!`] macro's expansion; not a stable API.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use inventory;

pub use broker::Broker;
pub use buffered::{Buffered, BufferedSubscriber};
pub use capability::{
    BatchSubscriber, DescribeServer, Partitioned, RequestReply, ServerSpec, Subscribe,
    TransactionalPublisher,
};
pub use error::AckError;
pub use field::{BuildContext, Field, FieldMut};
pub use headers::Headers;
pub use message::{IncomingMessage, OutgoingMessage, RawMessage};
pub use publisher::Publisher;
pub use schema::Message;
pub use subscriber::Subscriber;
pub use subscription::{Name, SubscriptionSource};

pub mod codec;

#[cfg(feature = "memory")]
pub mod memory;

pub mod runtime;

pub use runtime::RustStream;

/// Attribute macro that turns an `async fn` into a mountable subscriber definition.
///
/// Available with the `macros` feature. See [`ruststream_macros::subscriber`].
#[cfg(feature = "macros")]
pub use ruststream_macros::subscriber;

/// Attribute macro that generates a `main` entry point from a `RustStream` builder function.
///
/// Available with the `macros` feature. See [`ruststream_macros::app`] and
/// [`runtime::cli`].
#[cfg(feature = "macros")]
pub use ruststream_macros::app;

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

#[cfg(feature = "logging")]
pub mod logging;

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
    /// `None` otherwise - without forcing the bound onto every message type. The inherent method
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

    /// The trait fallback for [`Message`](crate::Message) metadata: chosen for any `T` the
    /// inherent methods below do not cover.
    pub trait NoMessageProbe {
        /// Returns `None` (the probed type does not implement `Message`).
        fn message_name(&self) -> Option<&'static str>;
        /// Returns `None` (the probed type does not implement `Message`).
        fn message_description(&self) -> Option<&'static str>;
    }

    impl<T> NoMessageProbe for Probe<T> {
        fn message_name(&self) -> Option<&'static str> {
            None
        }

        fn message_description(&self) -> Option<&'static str> {
            None
        }
    }

    impl<T: crate::Message> Probe<T> {
        /// Returns [`Message::NAME`](crate::Message::NAME) for `T` (inherent; preferred over the
        /// trait fallback).
        #[must_use]
        pub fn message_name(&self) -> Option<&'static str> {
            Some(T::NAME)
        }

        /// Returns [`Message::DESCRIPTION`](crate::Message::DESCRIPTION) for `T` (inherent;
        /// preferred over the trait fallback).
        #[must_use]
        pub fn message_description(&self) -> Option<&'static str> {
            T::DESCRIPTION
        }
    }
}
