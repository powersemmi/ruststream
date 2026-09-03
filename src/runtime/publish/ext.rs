//! The publish builder's entry points on a bare [`Publisher`].

use crate::{OutgoingDestination, Publisher};

use super::builder::{HeadersUnset, MessageBody, PublishBuilder, message_of};
use super::sink::UnnamedCodec;

/// The publish builder on any [`Publisher`]: `message(..)`, the one entry point of a publish.
///
/// Blanket-implemented for every publisher, so a broker publisher held in the application state
/// publishes through the same builder as an [`Out`](crate::runtime::Out) slot. The difference is
/// the codec ladder: a bare publisher carries no codec of its own, so `message(..)` encodes with
/// the crate's [`DefaultCodec`](crate::codec::DefaultCodec) unless the call names one with
/// `with_codec(..)`. A surface that has a codec (an `Out` slot, a
/// [`TypedPublisher`](super::TypedPublisher), a transaction scope) shadows this entry point with
/// its own and uses that codec instead.
///
/// One entry point covers both wires. A `serde::Serialize` value takes the resolved codec; a
/// [`Serialized`](super::Serialized) value carries its own bytes and no codec runs on them, which
/// is how a payload that is not a model of its own still travels as a declared type.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Outgoing, Serialized};
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "orders.done")]
/// struct OrderDone {
///     id: u64,
/// }
///
/// // Bytes that are already the payload, under a name of their own. It declares no
/// // destination, so the call site names one.
/// #[derive(Outgoing, Serialized)]
/// struct Audit(Vec<u8>);
///
/// let publisher = MemoryBroker::new().publisher();
/// publisher.message(&OrderDone { id: 7 }).publish().await?;
/// publisher
///     .message(&Audit(b"{}".to_vec()))
///     .to("orders.audit")
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait PublishExt: Publisher {
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
