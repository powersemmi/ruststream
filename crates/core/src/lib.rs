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
//! ruststream-core = { version = "0.8", default-features = false, features = ["alloc"] }
//! ```
//!
//! On a target without compare-and-swap, such as `thumbv6m-none-eabi`, `bytes` needs its
//! `extra-platforms` feature, and the binary chooses how `portable-atomic` provides the missing
//! operations (a `critical-section` implementation, or its single-core option).

#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;
// The harness's side (`testing`) is `std` code of its own: its task-local needs the standard
// library without the contract's `std` feature, which turns the heap on.
#[cfg(any(feature = "std", feature = "testing", test))]
extern crate std;

#[doc(hidden)]
pub mod __alloc;
#[cfg(feature = "std")]
pub mod bindable;
#[cfg(feature = "asyncapi")]
pub mod bindings;
mod broker;
mod capability;
pub mod codec;
#[cfg(feature = "alloc")]
mod describe;
#[doc(hidden)]
pub mod diag;
mod error;
mod field;
mod fixed_buf;
mod form;
mod headers;
mod message;
pub mod outgoing;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod pairing;
mod publisher;
mod schema;
mod send;
mod subscriber;
mod subscription;
mod typed_headers;
pub mod wire;

#[cfg(feature = "alloc")]
pub use bytes::{Bytes, BytesMut};
#[cfg(feature = "alloc")]
pub use bytes_utils::Str;

pub use broker::{Broker, Connected, ConnectedBroker, SendBroker};
#[cfg(feature = "alloc")]
pub use capability::{
    ApiKeyLocation, DescribeServer, HttpApiKeyLocation, SecurityScheme, ServerSpec,
};
pub use capability::{
    BATCH_CAPACITY, BatchSlot, BatchStream, BatchSubscriber, BatchVec, Carries, CarriesBatch,
    OwnedTransactions, Partitioned, Positioned, RequestReply, Seekable, Seeker, Subscribe,
    Transaction, TransactionalPublisher,
};
#[cfg(feature = "alloc")]
pub use describe::{AppId, AppIdError, Contact, ExternalDocs, License, Tag};
#[cfg(not(feature = "alloc"))]
pub use error::Cause;
pub use error::{AckError, DeclareRetryError};
pub use field::{BuildBatchContext, BuildContext, ContextField, Field, FieldMut};
#[cfg(not(feature = "alloc"))]
pub use fixed_buf::PAYLOAD_CAPACITY;
pub use fixed_buf::{BufferFull, EncodeBuf, FixedBuf, FixedBufOf, FixedBufView, PayloadBuf};
#[cfg(feature = "alloc")]
pub use headers::HeaderMap;
pub use headers::Headers;
#[cfg(not(feature = "alloc"))]
pub use headers::{FixedHeaders, HEADER_BYTES, HEADER_ENTRIES, HeadersFull};
#[cfg(any(feature = "alloc", feature = "testing"))]
pub use message::RawMessage;
pub use message::{IncomingMessage, OutgoingMessage, Settle};
pub use publisher::{
    Capped, DefaultPublish, Lend, OutgoingFor, PairError, PayloadForm, PublishMessageError,
    PublishOutcome, PublishPolicy, Publisher, Take, Unbounded,
};
#[doc(hidden)]
pub use publisher::{KeptPayload, PayloadEncode, SinkOutcome, SinkRefusal};
pub use schema::{
    CallerName, DestinationForm, FixedName, HeadersContract, MessageHeaders, MessageInfo,
    NameTemplate, NoHeaders, OutgoingDestination, WithHeaders,
};
#[doc(hidden)]
pub use send::{IfSend, MaybeSend, MaybeSendSync, MaybeSync};
pub use subscriber::{ReadyHint, Subscriber};
pub use subscription::{
    AddressedCopies, BrokerMoves, CopyPath, FromName, Name, NameText, NamedCopies,
    RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, StartAt, SubscriptionSource, Unnamed,
};
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
///         b.include(settle.batch(nonzero!(128)).workers(nonzero!(4)))
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
