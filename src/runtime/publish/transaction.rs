//! Transaction scopes over a publisher, borrowed and owned.

use std::fmt;

use serde::Serialize;
use thiserror::Error;
use tracing::warn;

use crate::codec::{Codec, CodecError};
// `DefaultCodec` only exists when a codec feature is on; the impl that names it is gated the same
// way, so an ungated import would break `--no-default-features`.
use super::{HeadersUnset, MessageBody, Publish, RawBody, TypedPublisher, message_of, raw_of};
use crate::{
    CallerName, OutgoingDestination, OutgoingMessage, OwnedTransactions, Transaction,
    TransactionalPublisher,
};

/// A live broker transaction, opened by [`Transactional::begin`](crate::runtime::Transactional::begin).
///
/// Publishes issued through the scope become visible together on [`commit`](Self::commit), or
/// not at all after [`abort`](Self::abort); both consume the scope, so a double commit or a
/// publish after settling is a compile error. This is the manual counterpart of the per-batch
/// transaction the runtime drives for batch reply-publishing mounts.
///
/// The scope encodes values with the wrapper's codec and sends them directly: the reply
/// [`PublishTransform`](crate::runtime::PublishTransform) stack and the app-wide [`publish_layer`](crate::runtime::RustStream::publish_layer)
/// middleware do not run here - both belong to the dispatch path, where a delivery context
/// exists.
///
/// Dropping an unsettled scope logs a warning and leaves the broker transaction open (destructors
/// cannot run async work); always settle explicitly.
#[must_use = "a transaction scope must be settled with commit() or abort()"]
pub struct TransactionScope<'a, P, C> {
    // The transactional reply wiring opens a scope over the publisher it wraps.
    pub(super) publisher: &'a P,
    pub(super) codec: &'a C,
    pub(super) open: bool,
}

impl<'s, P, C> TransactionScope<'s, P, C> {
    /// Starts a typed publish inside the transaction, encoded with the wrapper's codec: the same
    /// builder as everywhere else, sending into the open transaction instead of straight to the
    /// broker.
    ///
    /// Nothing published this way is visible before [`commit`](Self::commit).
    pub fn message<'a, T>(
        &'a self,
        value: &'a T,
    ) -> Publish<&'s P, MessageBody<'a, T>, &'s C, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        message_of(self.publisher, value, self.codec)
    }

    /// Starts a byte publish inside the transaction: the payload travels as it is, to the
    /// destination named with `to(..)`.
    pub fn raw<'a, B>(
        &'a self,
        payload: &'a B,
    ) -> Publish<&'s P, RawBody<'a>, (), HeadersUnset, CallerName>
    where
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(self.publisher, payload)
    }
}

impl<P, C> TransactionScope<'_, P, C>
where
    P: TransactionalPublisher,
    C: Codec,
{
    /// Encodes `value` with the wrapper's codec and publishes it to `name` inside the
    /// transaction.
    ///
    /// A failed publish does not settle the scope: the caller decides between retrying and
    /// [`abort`](Self::abort). Aborting is the safe default - after an error the broker-side
    /// transaction state is implementation-defined.
    ///
    /// The builder did not replace this one, which is why it outlived the rest of the
    /// pre-builder surface. [`message`](Self::message) reads the destination form off the
    /// value's type through [`OutgoingDestination`], which `#[derive(Outgoing)]` implements - so a
    /// `Serialize` type the service does not own is out of reach: the orphan rule forbids both
    /// the derive and a hand-written impl on a foreign type. Defaulting the form for types that
    /// declare nothing would need a blanket impl, which collides with every derived one, and
    /// picking the derived impl over the blanket is specialization, unavailable on stable. Own
    /// the type, derive `Outgoing` on it and publish it through the builder wherever that is
    /// possible; this method stays for the case where it is not.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionPublishError::Encode`] when the codec rejects the value, and
    /// [`TransactionPublishError::Publish`] when the broker rejects the message.
    ///
    /// # Cancel safety
    ///
    /// As with [`Publisher::publish`](crate::Publisher::publish), cancel safety is broker-defined: dropping the future
    /// mid-flight may leave the message in an indeterminate state inside the transaction.
    pub async fn publish<T: Serialize + Sync>(
        &mut self,
        name: &str,
        value: &T,
    ) -> Result<(), TransactionPublishError<P::Error>> {
        let payload = self
            .codec
            .encode(value)
            .map_err(TransactionPublishError::Encode)?;
        self.publisher
            .publish(OutgoingMessage::new(name, &payload))
            .await
            .map_err(TransactionPublishError::Publish)
    }

    /// Commits the transaction: every publish issued through the scope becomes visible at once.
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker rejects the commit. Per the
    /// [`TransactionalPublisher`] contract a failed commit closes the transaction, so the spent
    /// scope leaves the handle free for a fresh [`begin`](crate::runtime::Transactional::begin).
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe: dropping the future mid-commit leaves the broker transaction in an
    /// implementation-defined state. The scope treats itself as unsettled then - its drop
    /// warning fires - which is why `open` clears only after the broker call completes.
    pub async fn commit(mut self) -> Result<(), P::Error> {
        let result = self.publisher.commit().await;
        self.open = false;
        result
    }

    /// Aborts the transaction: nothing published through the scope becomes visible.
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker fails to abort.
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe, exactly like [`commit`](Self::commit): a future dropped mid-abort
    /// leaves the scope unsettled and its drop warning fires.
    pub async fn abort(mut self) -> Result<(), P::Error> {
        let result = self.publisher.abort().await;
        self.open = false;
        result
    }
}

impl<P, C> Drop for TransactionScope<'_, P, C> {
    fn drop(&mut self) {
        if self.open {
            warn!(
                target: "ruststream::dispatch",
                "transaction scope dropped without commit or abort; the broker transaction stays \
                 open on this publisher handle until it is settled or the handle is dropped"
            );
        }
    }
}

impl<P, C> fmt::Debug for TransactionScope<'_, P, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionScope")
            .field("open", &self.open)
            .finish_non_exhaustive()
    }
}

impl<P, C, PL, BL> TypedPublisher<P, C, PL, BL>
where
    P: OwnedTransactions,
    C: Sync,
    PL: Sync,
    BL: Sync,
{
    /// Opens an owned broker transaction and returns the [`TypedTransaction`] that owns it.
    ///
    /// This is the typed sugar over the owned transaction kind ([`OwnedTransactions`]), the
    /// counterpart of the borrowed [`Transactional::begin`](crate::runtime::Transactional::begin): every call opens its own
    /// independent transaction and the returned value owns its buffer, so any number can be
    /// open on one publisher at a time - settling one never touches another. Kafka-like
    /// brokers, whose client holds exactly one transaction per producer, offer only the
    /// borrowed kind.
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::TypedPublisher;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MemoryBroker::new();
    /// let publisher = TypedPublisher::new(broker.publisher());
    ///
    /// let mut orders = publisher.transaction().await?;
    /// let mut audit = publisher.transaction().await?; // concurrent with `orders`
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
    pub async fn transaction(&self) -> Result<TypedTransaction<'_, P::Transaction, C>, P::Error> {
        Ok(TypedTransaction {
            txn: self.publisher.transaction().await?,
            codec: &self.codec,
        })
    }
}

/// An owned broker transaction carrying the publisher's codec, opened by
/// [`TypedPublisher::transaction`].
///
/// The owned counterpart of [`TransactionScope`]: the scope borrows the handle's single
/// broker-side transaction, while this value owns an independent one
/// ([`OwnedTransactions::Transaction`]), so any number can be open on one publisher and driven
/// concurrently. Publishes encode with the publisher's codec and buffer into the transaction;
/// the whole buffer becomes visible atomically on [`commit`](Self::commit) and is discarded by
/// [`abort`](Self::abort), both of which consume the value, so a double commit or a publish
/// after settling is a compile error.
///
/// Like the scope, it encodes values and sends them directly: the reply [`PublishTransform`](crate::runtime::PublishTransform)
/// stack and the app-wide [`publish_layer`](crate::runtime::RustStream::publish_layer) middleware belong
/// to the dispatch path, where a delivery context exists, and do not run here.
///
/// Dropping an unsettled value discards the client buffer like an abort - unlike the scope, no
/// broker-side transaction is left open on the handle. The missed-settle warning comes from the
/// underlying [`Transaction`] value's own drop (per its contract), so this wrapper does not add
/// a second one.
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct TypedTransaction<'a, Txn, C> {
    txn: Txn,
    codec: &'a C,
}

impl<'c, Txn, C> TypedTransaction<'c, Txn, C> {
    /// Starts a typed publish into the transaction's buffer, encoded with the publisher's codec.
    ///
    /// The unique borrow is what the buffer needs, so one publish is built and awaited at a
    /// time; nothing is visible before [`commit`](Self::commit).
    pub fn message<'a, T>(
        &'a mut self,
        value: &'a T,
    ) -> Publish<&'a mut Txn, MessageBody<'a, T>, &'c C, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        message_of(&mut self.txn, value, self.codec)
    }

    /// Starts a byte publish into the transaction's buffer: the payload travels as it is, to the
    /// destination named with `to(..)`.
    pub fn raw<'a, B>(
        &'a mut self,
        payload: &'a B,
    ) -> Publish<&'a mut Txn, RawBody<'a>, (), HeadersUnset, CallerName>
    where
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(&mut self.txn, payload)
    }
}

impl<Txn, C> TypedTransaction<'_, Txn, C>
where
    Txn: Transaction,
    C: Codec,
{
    /// Encodes `value` with the publisher's codec and publishes it into the transaction:
    /// buffered, not visible before [`commit`](Self::commit).
    ///
    /// A failed publish does not settle the transaction; the caller decides between retrying
    /// and [`abort`](Self::abort).
    ///
    /// Kept for the reason spelled out on
    /// [`TransactionScope::publish`](TransactionScope::publish): a `Serialize` type the service
    /// does not own cannot declare an [`OutgoingDestination`], so the builder's
    /// [`message`](Self::message) entry point does not reach it.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionPublishError::Encode`] when the codec rejects the value, and
    /// [`TransactionPublishError::Publish`] when the message cannot be buffered (a pure client
    /// buffer is infallible in practice).
    ///
    /// # Cancel safety
    ///
    /// As with [`Transaction::publish`], cancel safety is implementation-defined: dropping the
    /// future mid-flight may leave the message in an indeterminate state inside the
    /// transaction.
    pub async fn publish<T>(
        &mut self,
        name: &str,
        value: &T,
    ) -> Result<(), TransactionPublishError<Txn::Error>>
    where
        T: Serialize + Sync,
    {
        let payload = self
            .codec
            .encode(value)
            .map_err(TransactionPublishError::Encode)?;
        self.txn
            .publish(OutgoingMessage::new(name, &payload))
            .await
            .map_err(TransactionPublishError::Publish)
    }

    /// Commits the transaction: the whole buffer becomes visible atomically, in publish order.
    ///
    /// # Errors
    ///
    /// Returns the transaction's error when the flush fails. A failed commit has still consumed
    /// the transaction and its buffer is lost; redelivery of the inputs, not resubmission of the
    /// buffer, is the recovery path (the [`Transaction::commit`] contract).
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe: the future owns the transaction, so dropping it mid-commit discards
    /// what is left of the buffer with the flush incomplete - which messages became visible is
    /// implementation-defined, as for a [`TransactionScope::commit`] dropped mid-flight.
    pub async fn commit(self) -> Result<(), Txn::Error> {
        self.txn.commit().await
    }

    /// Aborts the transaction, discarding the buffer.
    ///
    /// # Errors
    ///
    /// Returns the transaction's error when staged broker-side state cannot be discarded; for a
    /// pure client buffer this is infallible in practice.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future mid-abort still discards the buffer - the future owns the
    /// transaction, and an unsettled drop is itself the discard (the underlying value may log
    /// its missed-settle warning).
    pub async fn abort(self) -> Result<(), Txn::Error> {
        self.txn.abort().await
    }
}

impl<Txn, C> fmt::Debug for TypedTransaction<'_, Txn, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedTransaction").finish_non_exhaustive()
    }
}

/// Error returned by [`TransactionScope::publish`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransactionPublishError<E> {
    /// The codec rejected the value.
    #[error("failed to encode the value for a transactional publish")]
    Encode(#[source] CodecError),
    /// The broker rejected the message.
    #[error("failed to publish inside the transaction")]
    Publish(#[source] E),
}
