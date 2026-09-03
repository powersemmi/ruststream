//! Transaction scopes over a publisher, borrowed and owned.

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;
use thiserror::Error;
use tracing::warn;

use super::{HeadersUnset, MessageBody, PublishBuilder, PublishCodec, message_of};
use crate::codec::{Codec, CodecError};
use crate::{
    OutgoingDestination, OutgoingMessage, OwnedTransactions, Transaction, TransactionalPublisher,
};

/// What a surface's transactions admit into their typed publish entry: implemented by the
/// surface a scope or an owned transaction is opened on, which rides the transaction as its
/// `Admit` parameter. Machinery; never named in user code.
///
/// A transaction opened on a bare publisher ([`PublishExt::begin`](super::PublishExt::begin),
/// [`PublishExt::owned_transaction`](super::PublishExt::owned_transaction)) admits every declared
/// message,
/// like that publisher's own entry point; one opened on an `Out` slot admits exactly what the
/// slot's `message` admits - the marker's `#[publishes(..)]` dictionary narrowed by the
/// parameter's declared set - so a transaction cannot publish what the generated document never
/// declared. `Index` is inferred per call, like the declared-set membership it forwards to.
#[doc(hidden)]
pub trait Admits<T, Index> {}

/// The admit surface of a transaction opened straight on a publisher: it carries no dictionary,
/// so it takes every declared message.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct AnyDeclared;

impl<T> Admits<T, ()> for AnyDeclared {}

/// A live broker transaction, opened by `begin()` on a transactional surface.
///
/// The surfaces are a bare publisher ([`PublishExt::begin`](super::PublishExt::begin)) and an
/// `Out` slot whose wired value is a [`TransactionalPublisher`]
/// ([`Slot::begin`](crate::runtime::Slot::begin)).
/// Publishes issued through the scope become visible together on [`commit`](Self::commit), or
/// not at all after [`abort`](Self::abort); both consume the scope, so a double commit or a
/// publish after settling is a compile error. This is the hand-written counterpart of the
/// per-page transaction the runtime drives for a `.transactional()` reply wiring.
///
/// The scope encodes values with the surface's codec and sends them into the open transaction
/// directly: it opens on the surface's own publisher, so a slot's
/// [`OutTransform`](crate::runtime::OutTransform) stack, the reply
/// [`PublishTransform`](crate::runtime::PublishTransform) stack and the app-wide
/// [`publish_layer`](crate::runtime::RustStream::publish_layer) middleware do not run here.
///
/// `Admit` is the surface the scope was opened on, and gates what [`message`](Self::message)
/// admits (see [`Admits`]): a scope opened on a slot admits what the slot's typed entry admits,
/// one opened on a publisher admits every declared message.
///
/// Dropping an unsettled scope logs a warning and leaves the broker transaction open (destructors
/// cannot run async work); always settle explicitly.
#[must_use = "a transaction scope must be settled with commit() or abort()"]
pub struct TransactionScope<'a, P, Enc, Admit> {
    publisher: &'a P,
    enc: Enc,
    open: bool,
    _admit: PhantomData<fn() -> Admit>,
}

impl<'a, P, Enc, Admit> TransactionScope<'a, P, Enc, Admit>
where
    P: TransactionalPublisher,
{
    /// Begins a broker transaction on `publisher` and returns the scope owning it. The one
    /// constructor of every surface that opens scopes (a bare publisher, the slot entries), so
    /// the begin-then-own step is written once. `enc` is the surface's codec position: a borrow
    /// of the slot's codec, or the unnamed position of a publisher that carries none.
    pub(crate) async fn open(publisher: &'a P, enc: Enc) -> Result<Self, P::Error> {
        publisher.begin_transaction().await?;
        Ok(Self {
            publisher,
            enc,
            open: true,
            _admit: PhantomData,
        })
    }
}

impl<'s, P, Enc: Copy, Admit> TransactionScope<'s, P, Enc, Admit> {
    /// Starts a typed publish inside the transaction, encoded with the surface's codec: the same
    /// builder as everywhere else, sending into the open transaction instead of straight to the
    /// broker.
    ///
    /// Nothing published this way is visible before [`commit`](Self::commit). A scope opened on
    /// an `Out` slot admits what the slot's own entry admits: the message type has to be in the
    /// marker's dictionary and the parameter's declared set (see [`Admits`]).
    pub fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'s P, MessageBody<'a, T>, Enc, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
        Admit: Admits<T, Index>,
    {
        message_of(self.publisher, value, self.enc)
    }
}

impl<P, Enc, Admit> TransactionScope<'_, P, Enc, Admit>
where
    P: TransactionalPublisher,
    Enc: PublishCodec,
{
    /// Encodes `value` with the wrapper's codec and publishes it to `name` inside the
    /// transaction.
    ///
    /// A failed publish does not settle the scope: the caller decides between retrying and
    /// [`abort`](Self::abort). Aborting is the safe default - after an error the broker-side
    /// transaction state is implementation-defined.
    ///
    /// Use it for a `Serialize` type the service does not own: [`message`](Self::message) reads
    /// the destination form off the value's type through [`OutgoingDestination`], which the orphan
    /// rule keeps a foreign type from declaring. Own the type, derive `Outgoing` on it and publish
    /// it through the builder wherever that is possible.
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
            .enc
            .codec()
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
    /// scope leaves the handle free for a fresh [`begin`](super::PublishExt::begin).
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

impl<P, Enc, Admit> Drop for TransactionScope<'_, P, Enc, Admit> {
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

impl<P, Enc, Admit> fmt::Debug for TransactionScope<'_, P, Enc, Admit> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionScope")
            .field("open", &self.open)
            .finish_non_exhaustive()
    }
}

/// An owned broker transaction carrying the surface's codec, opened by `transaction()` on a
/// surface with owned transactions.
///
/// The surfaces are a bare publisher
/// ([`PublishExt::owned_transaction`](super::PublishExt::owned_transaction))
/// and an `Out` slot whose wired value is an [`OwnedTransactions`] publisher
/// ([`Slot::transaction`](crate::runtime::Slot::transaction)).
/// The owned counterpart of [`TransactionScope`]: the scope borrows the handle's single
/// broker-side transaction, while this value owns an independent one
/// ([`OwnedTransactions::Transaction`]), so any number can be open on one publisher and driven
/// concurrently. Publishes encode with the publisher's codec and buffer into the transaction;
/// the whole buffer becomes visible atomically on [`commit`](Self::commit) and is discarded by
/// [`abort`](Self::abort), both of which consume the value, so a double commit or a publish
/// after settling is a compile error.
///
/// Like the scope, it encodes values and buffers them directly: the publish paths a mount site
/// composes - a slot's [`OutTransform`](crate::runtime::OutTransform) stack, the reply
/// [`PublishTransform`](crate::runtime::PublishTransform) stack, the app-wide
/// [`publish_layer`](crate::runtime::RustStream::publish_layer) middleware - end in a send, and
/// the buffer is not one, so none of them run here. `Admit` is the
/// surface the transaction was opened on and gates what [`message`](Self::message) admits, as
/// on the scope (see [`Admits`]).
///
/// Dropping an unsettled value discards the client buffer like an abort - unlike the scope, no
/// broker-side transaction is left open on the handle. The missed-settle warning comes from the
/// underlying [`Transaction`] value's own drop (per its contract), so this wrapper does not add
/// a second one.
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct TypedTransaction<Txn, Enc, Admit> {
    txn: Txn,
    enc: Enc,
    _admit: PhantomData<fn() -> Admit>,
}

impl<Txn, Enc, Admit> TypedTransaction<Txn, Enc, Admit> {
    /// Opens an owned transaction on `publisher` and wraps it with the surface's codec position.
    /// The one constructor of every surface that opens owned transactions (a bare publisher, the
    /// slot entries).
    pub(crate) async fn open<P>(publisher: &P, enc: Enc) -> Result<Self, P::Error>
    where
        P: OwnedTransactions<Transaction = Txn>,
    {
        Ok(Self {
            txn: publisher.transaction().await?,
            enc,
            _admit: PhantomData,
        })
    }
}

impl<Txn, Enc: Copy, Admit> TypedTransaction<Txn, Enc, Admit> {
    /// Starts a typed publish into the transaction's buffer, encoded with the surface's codec.
    ///
    /// The unique borrow is what the buffer needs, so one publish is built and awaited at a
    /// time; nothing is visible before [`commit`](Self::commit). A transaction opened on an
    /// `Out` slot admits what the slot's own entry admits (see [`Admits`]).
    pub fn message<'a, T, Index>(
        &'a mut self,
        value: &'a T,
    ) -> PublishBuilder<&'a mut Txn, MessageBody<'a, T>, Enc, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
        Admit: Admits<T, Index>,
    {
        message_of(&mut self.txn, value, self.enc)
    }
}

impl<Txn, Enc, Admit> TypedTransaction<Txn, Enc, Admit>
where
    Txn: Transaction,
    Enc: PublishCodec,
{
    /// Encodes `value` with the publisher's codec and publishes it into the transaction:
    /// buffered, not visible before [`commit`](Self::commit).
    ///
    /// A failed publish does not settle the transaction; the caller decides between retrying
    /// and [`abort`](Self::abort).
    ///
    /// Same reach as [`TransactionScope::publish`](TransactionScope::publish): a `Serialize` type
    /// the service does not own cannot declare an [`OutgoingDestination`], so the builder's
    /// [`message`](Self::message) entry point does not take it.
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
            .enc
            .codec()
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

impl<Txn, Enc, Admit> fmt::Debug for TypedTransaction<Txn, Enc, Admit> {
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
