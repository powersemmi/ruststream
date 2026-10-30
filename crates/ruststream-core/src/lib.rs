//! The broker contract of [RustStream](https://docs.rs/ruststream): the traits a broker crate
//! implements and the message types that cross them.
//!
//! A service and a broker crate depend on [`ruststream`](https://docs.rs/ruststream) and import
//! from it; every item here is re-exported there under the same name, and the guides and the
//! module overviews live there.
//!
//! - The broker lifecycle: [`Broker`], [`ConnectedBroker`], [`Connected`].
//! - Subscribing: [`Subscriber`], [`SubscriptionSource`] and its descriptors.
//! - Publishing: [`Publisher`], [`PublishPolicy`], [`DefaultPublish`], the payload forms
//!   [`Lend`] and [`Take`].
//! - Messages: [`IncomingMessage`], [`OutgoingMessage`], [`RawMessage`], [`HeaderMap`].
//! - Capabilities a broker may add: [`BatchSubscriber`], [`Seekable`], [`TransactionalPublisher`],
//!   [`RequestReply`], [`Partitioned`], [`Carries`].
//! - What a service documents about itself: [`ServerSpec`], [`AppId`], [`Contact`], [`License`].
//!
//! # Features
//!
//! The crate is `#![no_std]`. Its features:
//!
//! - `alloc`: the contract itself. Headers, payloads and names live on the heap, so every trait
//!   and message type needs an allocator.
//! - `std` (default): `alloc`, the standard library's side of the dependencies, and
//!   [`Bindable`](bindable::Bindable), whose slot is a `std` mutex.
//! - `json`: [`SecurityScheme`] values that carry JSON.
//! - `asyncapi`: the binding types and the `*_bindings` methods of the contract.
//! - `testing`: the hook the test harness reads a pairing through; it runs on a host.
//!
//! A device service takes the contract without `std`:
//!
//! ```toml
//! [dependencies]
//! ruststream-core = { version = "0.7", default-features = false, features = ["alloc"] }
//! ```
//!
//! On a target without compare-and-swap, such as `thumbv6m-none-eabi`, `bytes` needs its
//! `extra-platforms` feature, and the binary chooses how `portable-atomic` provides the missing
//! operations (a `critical-section` implementation, or its single-core option).

#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;
#[cfg(any(feature = "std", test))]
extern crate std;

#[cfg(feature = "std")]
pub mod bindable;
#[cfg(feature = "asyncapi")]
pub mod bindings;
#[cfg(feature = "alloc")]
mod broker;
#[cfg(feature = "alloc")]
mod capability;
#[cfg(feature = "alloc")]
pub mod codec;
#[cfg(feature = "alloc")]
mod describe;
#[cfg(feature = "alloc")]
mod error;
#[cfg(feature = "alloc")]
mod field;
#[cfg(feature = "alloc")]
mod form;
#[cfg(feature = "alloc")]
mod headers;
#[cfg(feature = "alloc")]
mod message;
#[cfg(feature = "alloc")]
pub mod outgoing;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod pairing;
#[cfg(feature = "alloc")]
mod publisher;
#[cfg(feature = "alloc")]
mod schema;
#[cfg(feature = "alloc")]
mod subscriber;
#[cfg(feature = "alloc")]
mod subscription;
#[cfg(feature = "alloc")]
mod twin;
#[cfg(feature = "alloc")]
mod typed_headers;
#[cfg(feature = "alloc")]
pub mod wire;

#[cfg(feature = "alloc")]
pub use bytes::{Bytes, BytesMut};
#[cfg(feature = "alloc")]
pub use bytes_utils::Str;

#[cfg(feature = "alloc")]
pub use broker::{Broker, Connected, ConnectedBroker, LocalBroker, LocalConnectedBroker};
#[cfg(feature = "alloc")]
pub use capability::{
    ApiKeyLocation, BatchSubscriber, Carries, CarriesBatch, DescribeServer, HttpApiKeyLocation,
    LocalBatchSubscriber, LocalDescribeServer, LocalOwnedTransactions, LocalPositioned,
    LocalRequestReply, LocalSeekable, LocalSeeker, LocalSubscribe, LocalTransaction,
    LocalTransactionalPublisher, OwnedTransactions, Partitioned, Positioned, RequestReply,
    SecurityScheme, Seekable, Seeker, ServerSpec, Subscribe, Transaction, TransactionalPublisher,
};
#[cfg(feature = "alloc")]
pub use describe::{AppId, AppIdError, Contact, ExternalDocs, License, Tag};
#[cfg(feature = "alloc")]
pub use error::{AckError, DeclareRetryError};
#[cfg(feature = "alloc")]
pub use field::{BuildBatchContext, BuildContext, ContextField, Field, FieldMut};
#[cfg(feature = "alloc")]
pub use headers::HeaderMap;
#[cfg(feature = "alloc")]
pub use message::{IncomingMessage, LocalIncomingMessage, OutgoingMessage, RawMessage};
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub use publisher::PayloadEncode;
#[cfg(feature = "alloc")]
pub use publisher::{
    DefaultPublish, Lend, LocalDefaultPublish, LocalPublishPolicy, LocalPublisher, OutgoingFor,
    PairError, PayloadForm, PublishPolicy, Publisher, Take,
};
#[cfg(feature = "alloc")]
pub use schema::{
    CallerName, DestinationForm, FixedName, HeadersContract, MessageHeaders, MessageInfo,
    NameTemplate, NoHeaders, OutgoingDestination, WithHeaders,
};
#[cfg(feature = "alloc")]
pub use subscriber::{LocalSubscriber, Subscriber};
#[cfg(feature = "alloc")]
pub use subscription::{
    AddressedCopies, BrokerMoves, CopyPath, FromName, LocalRedeliveryAddressed,
    LocalSubscriptionSource, Name, NamedCopies, RedeliveryAddress, RedeliveryAddressed,
    RetryDeclaration, StartAt, SubscriptionSource, Unnamed,
};
#[cfg(feature = "alloc")]
pub use typed_headers::{DeserializeHeadersError, SerializeHeadersError};

/// Builds a [`NonZero`](core::num::NonZero) integer from a literal, rejecting zero at compile
/// time.
///
/// The expansion is an inline `const` block, so `nonzero!(0)` fails the build instead of
/// panicking at runtime, and the `NonZero` width is inferred from the call site - the same
/// literal works for a batch size (`NonZeroUsize`) and any other `NonZero` parameter.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn settle(orders: &[Order]) -> HandlerOutcome {
///     tracing::info!(count = orders.len(), "settled a batch");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(settle.batch(nonzero!(128)).workers(nonzero!(4)));
///     })
/// }
/// # }
/// # fn main() {}
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
