//! The publish builder's entry points on a bare [`Publisher`].

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::OutgoingDestination;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::{CallerName, Publisher};

use super::builder::{HeadersUnset, PublishBuilder, RawBody, raw_of};
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use super::builder::{MessageBody, message_of};
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use super::sink::CallCodec;

/// The publish builder on any [`Publisher`]: `message(..)` for a value, `raw(..)` for bytes.
///
/// Blanket-implemented for every publisher, so a broker publisher held in the application state
/// publishes through the same builder as an [`Out`](crate::runtime::Out) slot. The difference is
/// the codec ladder: a bare publisher carries no codec of its own, so `message(..)` encodes with
/// the crate's [`DefaultCodec`](crate::codec::DefaultCodec) unless the call names one with
/// `with_codec(..)`. A surface that has a codec (an `Out` slot, a
/// [`TypedPublisher`](super::TypedPublisher), a transaction scope) shadows these with its own
/// entry points and uses that codec instead.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::PublishExt;
/// use ruststream::Outgoing;
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "orders.done")]
/// struct OrderDone {
///     id: u64,
/// }
///
/// let publisher = MemoryBroker::new().publisher();
/// publisher.message(&OrderDone { id: 7 }).publish().await?;
/// publisher.raw(b"{}").to("orders.audit").publish().await?;
/// # Ok(())
/// # }
/// ```
pub trait PublishExt: Publisher {
    /// Starts a byte publish: the payload travels as it is, to the destination named with
    /// `to(..)`.
    fn raw<'a, B>(
        &'a self,
        payload: &'a B,
    ) -> PublishBuilder<&'a Self, RawBody<'a>, (), HeadersUnset, CallerName>
    where
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(self, payload)
    }

    /// Starts a typed publish of a `#[derive(Outgoing)]` value, encoded with the crate's
    /// default codec (name another one with `with_codec(..)`). A
    /// [`Serialized`](super::Serialized) value's bytes leave as they are instead - the wire is
    /// the type's own ([`MessageWire`](super::MessageWire)), and no codec runs on it.
    ///
    /// Available when a codec feature is enabled; with none, every publish names its codec.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    fn message<'a, T>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a Self, MessageBody<'a, T>, CallCodec<DefaultCodec>, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        // A bare publisher has no codec of its own, so the bottom of the ladder (the crate
        // default) rides in the call position - the only one this surface has.
        message_of(self, value, CallCodec(DefaultCodec::default()))
    }
}

impl<P: Publisher + ?Sized> PublishExt for P {}
