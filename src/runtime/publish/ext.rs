//! The publish builder's entry points on a bare [`Publisher`].

use crate::{CallerName, OutgoingDestination, Publisher};

use super::builder::{HeadersUnset, MessageBody, PublishBuilder, RawBody, message_of, raw_of};
use super::sink::UnnamedCodec;

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
    /// The entry point exists whatever the build; what a build without a codec feature cannot do
    /// is *encode*, so a `Serialize` value published through it is a compile error naming the
    /// three ways out, and a `Serialized` value publishes as it always did.
    fn message<'a, T>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a Self, MessageBody<'a, T>, UnnamedCodec, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        // A bare publisher has no codec of its own, so the position stays unnamed and resolves to
        // the crate default - the bottom of the ladder, and the only rung this surface has.
        message_of(self, value, UnnamedCodec::new())
    }
}

impl<P: Publisher + ?Sized> PublishExt for P {}
