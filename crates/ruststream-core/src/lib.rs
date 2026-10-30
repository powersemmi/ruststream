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

pub mod bindable;
#[cfg(feature = "asyncapi")]
pub mod bindings;
mod broker;
mod capability;
pub mod codec;
mod describe;
mod error;
mod field;
mod form;
mod headers;
mod message;
pub mod outgoing;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod pairing;
mod publisher;
mod schema;
mod subscriber;
mod subscription;
mod typed_headers;
pub mod wire;

pub use bytes::{Bytes, BytesMut};
pub use bytes_utils::Str;

pub use broker::{Broker, Connected, ConnectedBroker};
pub use capability::{
    ApiKeyLocation, BatchSubscriber, Carries, CarriesBatch, DescribeServer, HttpApiKeyLocation,
    OwnedTransactions, Partitioned, Positioned, RequestReply, SecurityScheme, Seekable, Seeker,
    ServerSpec, Subscribe, Transaction, TransactionalPublisher,
};
pub use describe::{AppId, AppIdError, Contact, ExternalDocs, License, Tag};
pub use error::{AckError, DeclareRetryError};
pub use field::{BuildBatchContext, BuildContext, ContextField, Field, FieldMut};
pub use headers::HeaderMap;
pub use message::{IncomingMessage, OutgoingMessage, RawMessage};
#[doc(hidden)]
pub use publisher::PayloadEncode;
pub use publisher::{
    DefaultPublish, Lend, OutgoingFor, PairError, PayloadForm, PublishPolicy, Publisher, Take,
};
pub use schema::{
    CallerName, DestinationForm, FixedName, HeadersContract, MessageHeaders, MessageInfo,
    NameTemplate, NoHeaders, OutgoingDestination, WithHeaders,
};
pub use subscriber::Subscriber;
pub use subscription::{
    AddressedCopies, BrokerMoves, CopyPath, FromName, Name, NamedCopies, RedeliveryAddress,
    RedeliveryAddressed, RetryDeclaration, StartAt, SubscriptionSource, Unnamed,
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
