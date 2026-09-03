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
//! * `macros`: the `#[subscriber]`, [`#[ruststream::app]`](macro@app),
//!   [`#[derive(Outgoing)]`](macro@Outgoing) and [`#[derive(MessageInfo)]`](macro@MessageInfo)
//!   macros.
//! * `asyncapi`: `AsyncAPI` document generation and the HTML viewer.
//! * `metrics`: Prometheus metrics middleware and exporter.
//! * `logging`: colored, `RUST_LOG`-driven console logging via `tracing-subscriber`
//!   ([`logging::init`]). The generated `cli` `run` command installs it automatically.
//! * `otel`: OpenTelemetry SDK integration: OTLP export for traces and metrics via
//!   [`otel::OtelBuilder::init`], plus per-handler dispatch metrics middleware and W3C
//!   trace-context propagation ([`otel::propagation`]).
//! * `conformance`: the [`conformance::harness`] contract suite, per-capability suites in
//!   [`conformance::capabilities`], and broker-agnostic [`conformance::helpers`] for application
//!   tests. Generic over any broker's [`testing::TestableBroker`], so it pulls in no concrete broker
//!   (enable `memory` too to run it against [`memory::MemoryBroker`]).
//! * `testing`: the [`testing::TestApp`] in-process harness for application unit tests.
//! * `cli`: the `ruststream` binary (`run`, `asyncapi gen`, `new`).
//!
//! Disable defaults (`default-features = false`) to drop the bundled JSON codec; the core traits,
//! runtime, and dispatch remain. Add back only what you need.
//!
//! # Getting the imports
//!
//! [`prelude`] carries what a service writes every time (the application object, the handler
//! surface, the extractor parameters, the macros): `use ruststream::prelude::*;`. The broker and
//! the codec stay explicit, since those are the choices a service makes for itself.

#![forbid(unsafe_code)]

mod broker;
mod buffered;
mod capability;
mod error;
mod field;
mod headers;
mod message;
pub mod prelude;
mod publisher;
mod schema;
mod subscriber;
mod subscription;
pub mod testing;
#[cfg(test)]
mod testkit;
mod typed_headers;

/// Re-exported for the [`register_testable_broker!`] macro's expansion; not a stable API.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use inventory;

pub use broker::{Broker, Connected, ConnectedBroker};
pub use buffered::{Buffered, BufferedSubscriber};
pub use capability::{
    ApiKeyLocation, BatchSubscriber, DescribeServer, HttpApiKeyLocation, OwnedTransactions,
    Partitioned, Positioned, RequestReply, SecurityScheme, Seekable, Seeker, ServerSpec, Subscribe,
    Transaction, TransactionalPublisher,
};
pub use error::AckError;
pub use field::{BuildBatchContext, BuildContext, ContextField, Field, FieldMut};
pub use headers::HeaderMap;
pub use message::{IncomingMessage, OutgoingMessage, RawMessage};
pub use publisher::{DefaultPublish, PairError, PublishPolicy, Publisher};
pub use schema::{
    CallerName, DestinationForm, FixedName, HeadersContract, MessageHeaders, MessageInfo,
    NameTemplate, NoHeaders, OutgoingDestination, WithHeaders,
};
pub use subscriber::Subscriber;
pub use subscription::{FromName, Name, StartAt, SubscriptionSource, Unnamed};
pub use typed_headers::{DeserializeHeadersError, SerializeHeadersError};

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

/// Derive macro for [`MessageInfo`] metadata (type name + doc description).
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::MessageInfo;

/// Derive macro declaring everything a message type says about being sent: its destination
/// ([`OutgoingDestination`]) and its optional header contract ([`MessageHeaders`]), plus the
/// [`MessageInfo`] metadata the generated document reads.
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::Outgoing;

/// Derive macro that implements [`FromRef`](runtime::FromRef) for each field of an application
/// state, so handlers can inject any field with [`State<T>`](runtime::State).
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::FromRef;

/// Derive macro that makes a unit struct a slot marker ([`OutSlot`]) for
/// `Out<impl Publisher, Marker>` handler parameters.
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::OutSlot;

/// Derive macro for a declared message set: an enum whose variants each wrap one message
/// model, named as the third `Out` argument (`Out<impl Publisher, Marker, SendSet>`). The enum
/// is a type-level declaration and is never constructed.
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::OutMessages;

/// Derive macro for a self-deserializing input type
/// ([`Deserialized`](runtime::Deserialized)): a newtype or single-field struct over `&'a [u8]`
/// gains the construction and the [`Input`](runtime::Input) spelling; the page spelling
/// (`&[Frame<'_>]`) follows from it.
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::Deserialized;

/// Derive macro for a self-serialized outgoing type ([`Serialized`](runtime::Serialized)): a
/// newtype or single-field struct over a byte buffer gains the bytes accessor and the wire
/// spellings that route it onto the serialized wire ([`MessageWire`](runtime::MessageWire) for
/// a typed publish, [`ReplyShape`](runtime::ReplyShape) for the reply position).
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::Serialized;

// The trait shares the derive's name at the root (the serde pattern), so one import serves
// both the `#[derive(OutSlot)]` and a broker's blanket impl bound.
pub use runtime::OutSlot;

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

#[cfg(feature = "otel")]
pub mod otel;

/// Implementation detail used by the `#[subscriber]` macro to capture a payload's JSON Schema.
///
/// Not part of the public API; no stability guarantees.
#[doc(hidden)]
pub mod __private {
    use core::marker::PhantomData;

    /// A type-carrying probe the macro reads a payload schema off.
    ///
    /// Schema selection uses inherent-vs-trait specialization: the schema
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

    /// The trait fallback for the serialized-wire marker: chosen for any `T` that does not
    /// implement [`Serialized`](crate::runtime::Serialized).
    pub trait NoSerializedProbe {
        /// Returns `false` (the probed type encodes through a codec, or is no message at all).
        fn serialized_wire(&self) -> bool;
    }

    impl<T> NoSerializedProbe for Probe<T> {
        fn serialized_wire(&self) -> bool {
            false
        }
    }

    impl<T: crate::runtime::Serialized> Probe<T> {
        /// Returns `true`: the probed type carries its own wire bytes (inherent; preferred
        /// over the trait fallback).
        #[must_use]
        pub fn serialized_wire(&self) -> bool {
            true
        }
    }

    /// The trait fallback for [`Message`](crate::MessageInfo) metadata: chosen for any `T` the
    /// inherent methods below do not cover.
    pub trait NoMessageProbe {
        /// Returns `None` (the probed type does not implement `MessageInfo`).
        fn message_name(&self) -> Option<&'static str>;
        /// Returns `None` (the probed type does not implement `MessageInfo`).
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

    impl<T: crate::MessageInfo> Probe<T> {
        /// Returns [`Message::NAME`](crate::MessageInfo::NAME) for `T` (inherent; preferred over the
        /// trait fallback).
        #[must_use]
        pub fn message_name(&self) -> Option<&'static str> {
            Some(T::NAME)
        }

        /// Returns [`Message::DESCRIPTION`](crate::MessageInfo::DESCRIPTION) for `T` (inherent;
        /// preferred over the trait fallback).
        #[must_use]
        pub fn message_description(&self) -> Option<&'static str> {
            T::DESCRIPTION
        }
    }

    /// The trait fallback for header-contract schemas.
    ///
    /// Chosen for any `T` the inherent method below does not cover: no
    /// [`MessageHeaders`](crate::MessageHeaders) impl, a [`NoHeaders`](crate::NoHeaders)
    /// contract, or a contract type without a schema.
    pub trait NoHeadersSchemaProbe {
        /// Returns `None` (no headers schema available for the probed type).
        fn headers_schema_json(&self) -> Option<String>;
    }

    impl<T> NoHeadersSchemaProbe for Probe<T> {
        fn headers_schema_json(&self) -> Option<String> {
            None
        }
    }

    /// Renders a [`HeadersContract`](crate::HeadersContract) shape as a schema:
    /// [`WithHeaders<H>`](crate::WithHeaders) yields `H`'s JSON Schema.
    #[cfg(feature = "asyncapi")]
    pub trait ContractSchema {
        /// The serialized JSON Schema of the contract's header type, if any.
        fn schema_json() -> Option<String>;
    }

    #[cfg(feature = "asyncapi")]
    impl ContractSchema for crate::NoHeaders {
        fn schema_json() -> Option<String> {
            None
        }
    }

    #[cfg(feature = "asyncapi")]
    impl<H: schemars::JsonSchema> ContractSchema for crate::WithHeaders<H> {
        fn schema_json() -> Option<String> {
            serde_json::to_string(&schemars::schema_for!(H)).ok()
        }
    }

    #[cfg(feature = "asyncapi")]
    impl<T> Probe<T>
    where
        T: crate::MessageHeaders,
        T::Contract: ContractSchema,
    {
        /// Returns the schema of `T`'s declared header contract (inherent; preferred over the
        /// trait fallback).
        #[must_use]
        pub fn headers_schema_json(&self) -> Option<String> {
            <T::Contract as ContractSchema>::schema_json()
        }
    }
}

/// Builds a [`NonZero`](core::num::NonZero) integer from a literal, rejecting zero at compile
/// time.
///
/// The expansion is an inline `const` block, so `nonzero!(0)` fails the build instead of
/// panicking at runtime, and the `NonZero` width is inferred from the call site - the same
/// literal works for [`Buffered::max_size`](crate::Buffered::max_size) (`NonZeroUsize`) and any
/// other `NonZero` parameter.
///
/// # Examples
///
/// ```
/// use ruststream::{Buffered, Name, nonzero};
///
/// let source = Buffered::new(Name::new("orders")).max_size(nonzero!(128));
/// # let _ = source;
/// ```
///
/// Zero does not compile:
///
/// ```compile_fail
/// let _: core::num::NonZeroUsize = ruststream::nonzero!(0);
/// ```
#[macro_export]
macro_rules! nonzero {
    ($value:expr) => {
        const {
            match ::core::num::NonZero::new($value) {
                ::core::option::Option::Some(value) => value,
                ::core::option::Option::None => panic!("nonzero!(..) requires a non-zero value"),
            }
        }
    };
}
