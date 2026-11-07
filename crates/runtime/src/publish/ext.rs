//! The publish builder's entry points on a bare [`Publisher`].

use crate::{OutgoingDestination, OwnedTransactions, Publisher, TransactionalPublisher};
use ruststream_core::MaybeSend;

use super::builder::{HeadersUnset, MessageBody, PublishBuilder, message_of};
use super::placed::WithPlaces;
use super::sink::UnnamedCodec;
use super::transaction::{AnyDeclared, TransactionScope, TypedTransaction};

/// The publish builder on any [`Publisher`]: `message(..)`, the one entry point of a publish.
///
/// Blanket-implemented for every publisher, so a broker publisher held in the application state
/// publishes through the same builder as an [`Out`](crate::Out) slot. The difference is
/// the codec ladder: a bare publisher carries no codec of its own, so `message(..)` encodes with
/// the crate's [`DefaultCodec`](crate::codec::DefaultCodec) unless the call names one with
/// `with_codec(..)`. A surface that has a codec (an `Out` slot, a transaction scope opened on
/// one) shadows this entry point with its own and uses that codec instead.
///
/// One entry point covers both wires. A `serde::Serialize` value takes the resolved codec; a
/// [`Serialized`](super::Serialized) value carries its own bytes and no codec runs on them, which
/// is how a payload that is not a model of its own still travels as a declared type.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Serialize;
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "services.started")]
/// struct Started {
///     service: &'static str,
/// }
///
/// /// Declares no destination, so the call site names one.
/// #[derive(Serialize, Outgoing)]
/// struct Seeded {
///     rows: u64,
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("catalog", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         // The hook is handed the live publisher; `message(..)` is the same builder an `Out`
///         // slot offers, encoding with the default codec.
///         b.after_startup(Publish, async move |publisher| {
///             publisher
///                 .message(&Seeded { rows: 1200 })
///                 .to("catalog.seeded")
///                 .publish()
///                 .await?;
///             publisher
///                 .message(&Started { service: "catalog" })
///                 .publish()
///                 .await
///         })
///     })
/// }
/// # }
/// # fn main() {}
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

    /// Opens a broker transaction on this publisher and returns the [`TransactionScope`] that
    /// owns it.
    ///
    /// The scope is the only handle on the transaction: publishes go through it, and it is
    /// consumed by [`commit`](TransactionScope::commit) or [`abort`](TransactionScope::abort).
    /// A second `begin` on this handle, a commit without a begin, or a publish after the commit
    /// are not expressible - the methods do not exist on the types those states leave behind.
    ///
    /// This is the typed sugar over the borrowed transaction kind
    /// ([`TransactionalPublisher`]): the scope claims the handle's single broker-side
    /// transaction, so one scope per handle is open at a time. Brokers whose transactions are
    /// client buffers additionally offer the owned kind through
    /// [`owned_transaction`](Self::owned_transaction), where every call opens an independent
    /// buffer-owning [`TypedTransaction`].
    ///
    /// The scope inherits this surface's codec position, so it encodes with the crate default;
    /// name another one per publish with `with_codec(..)`.
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::PublishExt;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let publisher = MemoryBroker::new().publisher();
    ///
    /// let mut scope = publisher.begin().await?;
    /// scope.publish("orders.settled", &42_u32).await?;
    /// scope.commit().await?;
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to start a transaction.
    fn begin(
        &self,
    ) -> impl Future<
        Output = Result<TransactionScope<'_, Self, UnnamedCodec, AnyDeclared>, Self::Error>,
    > + MaybeSend
    where
        Self: TransactionalPublisher + Sized,
    {
        TransactionScope::open(self, UnnamedCodec::new())
    }

    /// Opens an owned broker transaction and returns the [`TypedTransaction`] that owns it.
    ///
    /// The typed sugar over the owned transaction kind ([`OwnedTransactions`]), the counterpart
    /// of the borrowed [`begin`](Self::begin): every call opens its own independent transaction
    /// and the returned value owns its buffer, so any number can be open on one publisher at a
    /// time - settling one never touches another. Kafka-like brokers, whose client holds exactly
    /// one transaction per producer, offer only the borrowed kind.
    ///
    /// The name spells out *owned* because [`OwnedTransactions::transaction`] - the raw
    /// capability this wraps - is in scope wherever this is, and two same-named trait methods on
    /// one publisher would be ambiguous at the call.
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::PublishExt;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let publisher = MemoryBroker::new().publisher();
    ///
    /// let mut orders = publisher.owned_transaction().await?;
    /// let mut audit = publisher.owned_transaction().await?; // concurrent with `orders`
    /// orders.publish("orders.settled", &42_u32).await?;
    /// audit.publish("audit.trail", &7_u32).await?;
    /// orders.commit().await?;
    /// audit.commit().await?;
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to open a transaction; pure
    /// client-buffer implementations are infallible in practice.
    fn owned_transaction(
        &self,
    ) -> impl Future<
        Output = Result<
            TypedTransaction<<Self as OwnedTransactions>::Transaction, UnnamedCodec, AnyDeclared>,
            Self::Error,
        >,
    > + MaybeSend
    where
        Self: OwnedTransactions + Sized,
    {
        TypedTransaction::open(self, UnnamedCodec::new())
    }

    /// Gives this publisher `N` places of its own for the messages its refused publishes hand
    /// back, in a build without `alloc`: for a publisher used outside a subscription.
    ///
    /// A publisher without the step keeps one place in each of its publishes, in the publish's
    /// own state. With the step its publishes keep none and share the publisher's `N` places, so
    /// the places do not grow with the publishes that run at once. A refused publish moves its
    /// message into a free place and reads it back before it returns; when every place is taken,
    /// it comes back without its message: the error reaches the caller, the message's bytes and
    /// headers do not. A place is a message's destination, payload and headers in fixed storage,
    /// about 700 bytes. With `alloc` a refused message is boxed and the step changes nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Outgoing, Serialized};
    ///
    /// /// A reading as the sensor reports it.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "readings")]
    /// struct Reading(&'static [u8]);
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// // The sensor's task publishes on its own, outside every subscription, and two of its
    /// // publishes may run at once.
    /// let readings = MemoryBroker::new().publisher().refusal_capacity::<2>();
    /// let (first, second) = futures::join!(
    ///     readings.message(&Reading(b"21.5")).publish(),
    ///     readings.message(&Reading(b"21.7")).publish(),
    /// );
    /// first?;
    /// second?;
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    ///
    /// A publisher keeps at least one place:
    ///
    /// ```compile_fail
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::PublishExt;
    ///
    /// let readings = MemoryBroker::new().publisher().refusal_capacity::<0>();
    /// ```
    fn refusal_capacity<const N: usize>(self) -> WithPlaces<Self, N>
    where
        Self: Sized,
    {
        WithPlaces::new(self)
    }
}

impl<P: Publisher> PublishExt for P {}
