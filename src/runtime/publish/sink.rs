//! The two positions a publish builder resolves against the surface it started from: the byte
//! sink that finally takes the message, and the codec that encodes a value into it.

use std::error::Error as StdError;
use std::future::Future;

use crate::codec::Codec;
use crate::{Headers, OutgoingMessage, Publisher, Transaction};

/// The byte sink a publish builder resolves down to.
///
/// Every publish surface ends in one call carrying an [`OutgoingMessage`]: a live
/// [`Publisher`] for the ordinary path, a [`Transaction`] for a buffered one. The builder is
/// generic over this trait so both paths share one set of positions instead of growing a
/// parallel method each.
///
/// You never implement it: it is blanket-implemented for `&P` of every [`Publisher`] and for
/// `&mut T` of every [`Transaction`]. A broker crate implements [`Publisher`], which is what
/// makes its publisher a sink.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::PublishSink;
/// use ruststream::OutgoingMessage;
///
/// let broker = MemoryBroker::new();
/// let publisher = broker.publisher();
/// let mut sink = &publisher; // any &Publisher is a sink
/// sink.send(OutgoingMessage::new("orders", b"{}".as_slice()))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait PublishSink: Send {
    /// The error the sink reports when the message cannot be sent.
    type Error: StdError + Send + Sync + 'static;

    /// Sends one message into the sink.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker (or the transaction buffer) rejects the message.
    ///
    /// # Cancel safety
    ///
    /// Inherited from the underlying publisher or transaction: dropping the future mid-flight
    /// may leave the message in an indeterminate state.
    fn send(
        &mut self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// The headers the sink contributes underneath the publish's own, or `None` when it
    /// contributes nothing.
    ///
    /// This is where [`Publisher::base_headers`] and [`Transaction::base_headers`] reach the
    /// builder: the blanket impls forward theirs, and a sink of the runtime's own (the erased
    /// publisher behind deferred redelivery, the test harness's injection point) forwards
    /// whatever it wraps. The builder starts the outgoing header map from this and writes the
    /// publish's headers over it key by key, so the call site has the last word.
    fn base_headers(&self) -> Option<&Headers> {
        None
    }
}

// A shared publisher reference is the ordinary sink: publishing takes `&self`, so the builder
// only ever borrows it.
impl<P: Publisher + ?Sized> PublishSink for &P {
    type Error = P::Error;

    fn send(
        &mut self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        (**self).publish(msg)
    }

    fn base_headers(&self) -> Option<&Headers> {
        (**self).base_headers()
    }
}

// A transaction buffers into itself, so its sink is the unique borrow the builder holds for the
// duration of one publish.
impl<T: Transaction> PublishSink for &mut T {
    type Error = T::Error;

    fn send(
        &mut self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        (**self).publish(msg)
    }

    fn base_headers(&self) -> Option<&Headers> {
        (**self).base_headers()
    }
}

/// The codec position of a publish builder: the surface's own codec, or one named at the call
/// with [`with_codec`](super::Publish::with_codec).
///
/// The two forms are `&C` (the surface's codec, borrowed - the scope, router or application
/// level of the codec ladder) and [`CallCodec<C>`] (a codec named at the call, the most specific
/// level). Like [`PublishSink`], it exists so the builder carries one codec position rather than
/// one method per level.
pub trait PublishCodec {
    /// The resolved codec.
    type Codec: Codec;

    /// Borrows the resolved codec.
    fn codec(&self) -> &Self::Codec;
}

impl<C: Codec> PublishCodec for &C {
    type Codec = C;

    fn codec(&self) -> &C {
        self
    }
}

/// A codec named at the call site, the most specific level of the codec ladder.
///
/// Produced by [`Publish::with_codec`](super::Publish::with_codec); you never construct it
/// directly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CallCodec<C>(pub(crate) C);

impl<C: Codec> PublishCodec for CallCodec<C> {
    type Codec = C;

    fn codec(&self) -> &C {
        &self.0
    }
}
