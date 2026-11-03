//! Rust core of the [`RustStream`](https://github.com/powersemmi/ruststream) messaging
//! framework: broker-agnostic traits, message types, codecs, router runtime, and a
//! conformance harness for broker authors.
//!
//! # A service
//!
//! A handler is an `async fn` over a decoded payload. The application object mounts it on a
//! broker, and the attribute writes the `main`:
//!
//! ```
//! # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
//! # mod demo {
//! use ruststream::memory::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[subscriber("orders")]
//! async fn handle(order: &Order) -> HandlerOutcome {
//!     println!("got order {}", order.id);
//!     HandlerOutcome::ack()
//! }
//!
//! #[ruststream::app]
//! fn app() -> RustStream {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
//!         b.include(handle)
//!     })
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! `cargo run -- run` starts it. The broker here is the in-process one; a real broker is a
//! crate of its own with the same shape, and its prelude replaces `memory::prelude`.
//!
//! # Where things are
//!
//! Each module's documentation is a map of its items, and each topic links to its component page
//! on the [site][components], which shows how to use it.
//!
//! * [`runtime`]: everything a service is made of: the [application][application] (builder,
//!   lifecycle, running, state) and its [subscribers][subscribers] (settings, messages, context,
//!   dispatch, middleware, publishing).
//! * [`codec`]: how payload bytes become values and back, and the byte lanes that need none.
//! * [`memory`]: the in-process broker, a real broker and the reference implementation.
//! * [`testing`]: testing the service's own app, in process or against a running broker.
//! * [`asyncapi`]: the generated document. [`metrics`], [`logging`], [`otel`]:
//!   [observability][observability].
//! * [`runtime::cli`]: the generated entry point and the `ruststream` command.
//! * [`conformance`]: the contract suite a broker crate runs against itself.
//!
//! The broker crates document their own descriptors and publish policies. Installation, the
//! guides and the list of brokers are on the site: <https://powersemmi.github.io/ruststream/>.
//!
//! [components]: https://powersemmi.github.io/ruststream/latest/components/
//! [application]: https://powersemmi.github.io/ruststream/latest/components/application/
//! [subscribers]: https://powersemmi.github.io/ruststream/latest/components/subscribers/
//! [observability]: https://powersemmi.github.io/ruststream/latest/components/observability/
//!
//! # Cargo features
//!
//! The core traits, the router, middleware and dispatch are always present. Every feature is
//! additive and opt-in.
//!
//! The runtime runs the app's subscriptions. `tokio` and `embassy` provide one each, and they
//! build together: the handlers are the same for both, and the app picks its runner. A function
//! marked `#[ruststream::app]` runs on tokio, and `#[ruststream::app(runtime = embassy)]` runs on
//! embassy.
//!
//! * `tokio` (default): the [`runtime::RustStream`] application object on tokio's runtime, its
//!   lifecycle and the generated entry point ([`runtime::cli`]).
//! * `embassy`: the embedded runner on embassy's executor and clock, for a device with no
//!   operating system. Its items (the `EmbeddedApp` application object, its `Control` and
//!   `Health`, the `EmbassyClock`) are re-exported in [`runtime`], where their reference is.
//! * `alloc`: the heap form of the framework, which `tokio`, `json`, `cbor` and `msgpack` turn
//!   on. Headers, payloads, continuations and failures live on the heap. A build without it is the
//!   fixed form of the embassy runtime: they live in storage of a fixed size and the build
//!   allocates nothing. One build holds one form, so a crate of the graph that turns `alloc` on
//!   turns it on for all.
//!
//! Codec features are mutually compatible and enable only the deserializers you need. They work
//! on either runtime. On a device without the standard library `json` and `cbor` build, with the
//! same codec types as with it; `msgpack` needs `std` and reports it at compile time.
//!
//! * `json` (default): [`codec::JsonCodec`].
//! * `msgpack`: [`codec::MsgpackCodec`].
//! * `cbor`: [`codec::CborCodec`].
//!
//! The embassy runtime without `alloc` mounts with one of these two, which encode into a slice and
//! decode from one:
//!
//! * `postcard`: `codec::PostcardCodec`.
//! * `json-core`: `codec::JsonCoreCodec`, JSON over `serde-json-core`.
//!
//! The embassy runtime without `alloc` reports what it loses or refuses (a reply the bus does not
//! take, a continuation that never runs) to the logger the device has, each line naming the
//! subscription and the message type. The feature picks the logger. With `alloc` the same reports
//! are `tracing` events.
//!
//! * `log`: records for the `log` crate, for a host or a board with a logger.
//! * `defmt`: lines for `defmt`, for a board with a probe. Both may be on and both receive.
//!
//! Two more features work on either runtime:
//!
//! * `memory`: the in-process broker of the selected runtime. With `tokio` it is
//!   [`memory::MemoryBroker`], usable in applications, prototypes and tests; with `embassy` it is
//!   the local bus in `memory::local`.
//! * `macros`: the `#[subscriber]`, [`#[ruststream::app]`](macro@app),
//!   [`#[derive(Outgoing)]`](macro@Outgoing), [`#[derive(MessageInfo)]`](macro@MessageInfo) and
//!   [`#[derive(InMessages)]`](macro@InMessages) macros.
//!
//! The rest run on tokio, and each turns `tokio` on:
//!
//! * `asyncapi`: `AsyncAPI` document generation and the HTML viewer.
//! * `metrics`: Prometheus metrics middleware and exporter.
//! * `logging`: colored, `RUST_LOG`-driven console logging via `tracing-subscriber`
//!   ([`logging::init`]). The generated `cli` `run` command installs it automatically.
//! * `otel`: OpenTelemetry SDK integration: OTLP export for traces and metrics via
//!   [`otel::OtelBuilder::init`], plus per-handler dispatch metrics middleware and W3C
//!   trace-context propagation ([`otel::propagation`]).
//! * `conformance`: the [`conformance::harness`] contract suite, per-capability suites in
//!   [`conformance::capabilities`], the in-process transport held to the server in
//!   [`conformance::in_process`], the checks of what a message carries in
//!   [`conformance::message_shape`], and broker-agnostic [`conformance::helpers`] for application
//!   tests. Generic over any broker's [`testing::TestableBroker`], so it pulls in no concrete broker
//!   (enable `memory` too to run it against [`memory::MemoryBroker`]).
//! * `testing`: the [`testing::TestApp`] harness, which runs the service's production app in
//!   process or against running brokers.
//! * `cli`: the `ruststream` binary (`run`, `asyncapi gen`, `new`).
//! * `poll-diagnostics`: every subscription samples the time its handler spends in `poll` and
//!   warns when a handler computes on the app's runtime ([`runtime::PollDiagnostics`]).
//!
//! A crate that disables the defaults (`default-features = false`) names its runtime among the
//! features it enables: `features = ["tokio"]`, `features = ["embassy"]` or both. A broker crate on
//! tokio enables `tokio`. A device service on a bare-metal target picks from `embassy`, `macros`
//! and `memory`, a codec (`json` and `cbor` with a heap, `postcard` and `json-core` without), and
//! `log` or `defmt` for its diagnostics. On a target with an operating system the crate uses the
//! standard library with either runtime.
//!
//! # Getting the imports
//!
//! [`prelude`] carries what a service writes every time (the application object, the handler
//! surface, the extractor parameters, the macros): `use ruststream::prelude::*;`. The broker and
//! the codec stay explicit, since those are the choices a service makes for itself.

#![forbid(unsafe_code)]
// The embassy runtime runs on a device with no standard library; the tokio runner needs it.
#![cfg_attr(not(feature = "tokio"), no_std)]

#[cfg(any(ruststream_alloc, feature = "testing"))]
extern crate alloc;

pub mod prelude;
pub mod testing;

/// Registers a production broker type with the test harness; see
/// [`testing::TestableRegistration`].
#[cfg(all(feature = "testing", ruststream_alloc))]
#[doc(inline)]
pub use ruststream_runtime::register_testable_broker;

/// Re-exported for the [`register_testable_broker!`] macro's expansion; not a stable API.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use inventory;

/// The buffer the crate's byte-producing surfaces speak: what a [`Codec`](codec::Codec) returns,
/// what a [`Serialized`](runtime::Serialized) value writes into, and the payload a publisher
/// that declared [`Take`] is handed.
///
/// Re-exported so that implementing either trait, or taking a published payload, does not also
/// mean depending on `bytes` directly; a service that already does keeps its own import, which
/// shadows this one.
#[cfg(any(ruststream_alloc, feature = "testing"))]
pub use ruststream_core::__alloc::bytes::BytesMut;

/// The shared byte buffer the message path hands around: a payload, a header value taken with
/// [`HeaderMap::get_shared`].
///
/// Re-exported for the same reason as [`BytesMut`]: reading a value out of a header map, or
/// handing one to a publisher, should not also mean depending on `bytes` directly.
#[cfg(any(ruststream_alloc, feature = "testing"))]
pub use ruststream_core::__alloc::bytes::Bytes;

/// The owned string of the message path: a destination name, a header key.
///
/// A `Bytes` that is valid UTF-8, so it reads as a `str`, clones by reference count and can be
/// taken as a slice of a buffer the broker already holds. `Str::from_static("orders")` costs
/// nothing at all; `String` moves in without a copy; `&str` copies once.
#[cfg(any(ruststream_alloc, feature = "testing"))]
pub use ruststream_core::__alloc::bytes_utils::Str;

#[cfg(ruststream_alloc)]
pub use ruststream_core::HeaderMap;
#[cfg(any(ruststream_alloc, feature = "testing"))]
pub use ruststream_core::RawMessage;
pub use ruststream_core::SendBroker;
pub use ruststream_core::{AckError, DeclareRetryError};
pub use ruststream_core::{
    AddressedCopies, BrokerMoves, CopyPath, FromName, Name, NamedCopies, RedeliveryAddress,
    RedeliveryAddressed, RetryDeclaration, StartAt, SubscriptionSource, Unnamed,
};
#[cfg(ruststream_alloc)]
pub use ruststream_core::{
    ApiKeyLocation, DescribeServer, HttpApiKeyLocation, SecurityScheme, ServerSpec,
};
#[cfg(ruststream_alloc)]
pub use ruststream_core::{AppId, AppIdError, Contact, ExternalDocs, License, Tag};
pub use ruststream_core::{
    BatchSlot, BatchStream, BatchSubscriber, Carries, CarriesBatch, OwnedTransactions, Partitioned,
    Positioned, RequestReply, Seekable, Seeker, Subscribe, Transaction, TransactionalPublisher,
};
pub use ruststream_core::{Broker, Connected, ConnectedBroker};
pub use ruststream_core::{BufferFull, EncodeBuf, Headers, PayloadBuf};
pub use ruststream_core::{BuildBatchContext, BuildContext, ContextField, Field, FieldMut};
pub use ruststream_core::{
    CallerName, DestinationForm, FixedName, HeadersContract, MessageHeaders, MessageInfo,
    NameTemplate, NoHeaders, OutgoingDestination, WithHeaders,
};
pub use ruststream_core::{
    DefaultPublish, Lend, OutgoingFor, PairError, PayloadForm, PublishMessageError, PublishOutcome,
    PublishPolicy, Publisher, Take,
};
pub use ruststream_core::{DeserializeHeadersError, SerializeHeadersError};
pub use ruststream_core::{IncomingMessage, OutgoingMessage, Settle};
pub use ruststream_core::{ReadyHint, Subscriber};

pub use ruststream_core::nonzero;
#[cfg(feature = "tokio")]
pub use ruststream_runtime_tokio::{Buffered, BufferedSubscriber};

pub mod codec;

/// The loggers an app selects with `.logger(..)`: the framework reports nothing until it does.
///
/// [`TracingLogger`] needs the `tracing` feature (the tokio runner turns it on), [`LogLogger`] the
/// `log` feature, [`DefmtLogger`] the `defmt` feature. The selection is process-wide, and the
/// level the user's backend filters at decides which events show: a `tracing` subscriber's
/// filter, `log::max_level()`, defmt's level.
///
/// # Examples
///
/// See [`Logger`].
pub mod diagnostics {
    #[cfg(feature = "defmt")]
    pub use ruststream_core::diag::DefmtLogger;
    #[cfg(feature = "log")]
    pub use ruststream_core::diag::LogLogger;
    pub use ruststream_core::diag::Logger;
    #[cfg(ruststream_tracing)]
    pub use ruststream_core::diag::TracingLogger;
}

// The bus is the runtime's: the tokio one, or the local one on embassy.
#[cfg(all(feature = "memory", any(feature = "tokio", feature = "embassy")))]
pub mod memory;

pub mod runtime;

#[cfg(feature = "tokio")]
pub use runtime::RustStream;

/// Time for service code: the sleep that runs on the app's clock, which the test harness moves.
#[cfg(feature = "tokio")]
pub mod time {
    pub use ruststream_runtime_tokio::time::sleep;
}

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

/// Derive macro for a declared set of message models a handler receives
/// ([`InMessages`](runtime::InMessages)): an enum whose variants each wrap one model, used as
/// the handler's input. Each variant decodes through its model's own path, the set picks the
/// variant left to right, by a function or by a header, and the generated document lists one
/// message per variant.
///
/// Available with the `macros` feature.
#[cfg(feature = "macros")]
pub use ruststream_macros::InMessages;

/// Derive macro for a self-deserializing input type
/// ([`Deserialized`](runtime::Deserialized)): a newtype or single-field struct over `&'a [u8]`
/// gains the construction and the [`Input`](runtime::Input) spelling; the batch spelling
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
    #[cfg(feature = "embassy")]
    use core::fmt::Display;
    use core::marker::PhantomData;

    // The standard names the macros emit, reached through the facade so that a `#![no_std]`
    // crate, which has neither `::std` nor these names in its prelude, can use the derives.
    // The text a schema probe returns: owned with a heap, where a schema can be rendered, and
    // borrowed without one, where every probe answers `None`.
    #[cfg(any(ruststream_alloc, feature = "testing"))]
    pub type SchemaText = String;
    #[cfg(not(any(ruststream_alloc, feature = "testing")))]
    pub type SchemaText = &'static str;
    #[cfg(any(ruststream_alloc, feature = "testing"))]
    pub use alloc::boxed::Box;
    #[cfg(any(ruststream_alloc, feature = "testing"))]
    pub use alloc::format;
    #[cfg(any(ruststream_alloc, feature = "testing"))]
    pub use alloc::string::{String, ToString};
    #[cfg(any(ruststream_alloc, feature = "testing"))]
    pub use alloc::vec;
    #[cfg(any(ruststream_alloc, feature = "testing"))]
    pub use alloc::vec::Vec;
    // The batch outcome a batch body returns: a `Vec` with `alloc`, a fixed buffer without.
    pub use core::error::Error;
    pub use ruststream_runtime::BatchOutcomes;
    // What a batch body replies with, one value per message: a `Vec` with `alloc`, a fixed
    // buffer without.
    #[cfg(ruststream_alloc)]
    pub use alloc::vec::Vec as BatchVec;
    #[cfg(not(ruststream_alloc))]
    pub use ruststream_runtime::BatchVec;
    // What the tokio `main` that `#[ruststream::app]` generates returns.
    #[cfg(feature = "tokio")]
    pub use std::process::ExitCode;
    /// Reports the error an embassy app stopped on, which the task `#[ruststream::app]` emits
    /// calls.
    #[cfg(feature = "embassy")]
    pub fn app_stopped(error: &impl Display) {
        ruststream_core::diag::error!(
            target: "ruststream::embassy",
            error = %error,
            "the app stopped on an error"
        );
    }

    use crate::runtime::Serialized;
    #[cfg(feature = "asyncapi")]
    use ruststream_runtime::schema_json;

    /// The decode errors `#[derive(InMessages)]` returns when a set picks no variant.
    pub mod in_messages {
        pub use crate::runtime::{
            every_variant_failed, frame_names_no_variant, header_missing, header_unknown,
            no_variant_chosen,
        };
    }

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
        fn schema_json(&self) -> Option<SchemaText>;
    }

    impl<T> NoSchemaProbe for Probe<T> {
        fn schema_json(&self) -> Option<SchemaText> {
            None
        }
    }

    #[cfg(feature = "asyncapi")]
    impl<T: schemars::JsonSchema> Probe<T> {
        /// Returns the serialized JSON Schema for `T` (inherent; preferred over the trait fallback).
        #[must_use]
        pub fn schema_json(&self) -> Option<SchemaText> {
            schema_json::<T>()
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

    impl<T: Serialized> Probe<T> {
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
        fn headers_schema_json(&self) -> Option<SchemaText>;
    }

    impl<T> NoHeadersSchemaProbe for Probe<T> {
        fn headers_schema_json(&self) -> Option<SchemaText> {
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
            schema_json::<H>()
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
        pub fn headers_schema_json(&self) -> Option<SchemaText> {
            <T::Contract as ContractSchema>::schema_json()
        }
    }
}
