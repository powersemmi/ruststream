//! The two positions a publish builder resolves against the surface it started from: the byte
//! sink that finally takes the message, and the codec that encodes a value into it.

use std::error::Error as StdError;
use std::future::Future;

use crate::codec::Codec;
use crate::{HeaderMap, OutgoingMessage, Publisher, Transaction};

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
    fn base_headers(&self) -> Option<&HeaderMap> {
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

    fn base_headers(&self) -> Option<&HeaderMap> {
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

    fn base_headers(&self) -> Option<&HeaderMap> {
        (**self).base_headers()
    }
}

/// The codec position of a publish builder: the surface's own codec, one named at the call with
/// [`with_codec`](super::PublishBuilder::with_codec), or [`UnnamedCodec`] when nothing named one.
///
/// The forms are `&C` (the surface's codec, borrowed - the scope, router or application level of
/// the codec ladder), [`CallCodec<C>`] (a codec named at the call, the most specific level), and
/// `UnnamedCodec` (the bottom of the ladder, which resolves to
/// [`DefaultCodec`](crate::codec::DefaultCodec) when the build has one). Like [`PublishSink`], it
/// exists so the builder carries one codec position rather than one method per level.
#[diagnostic::on_unimplemented(
    message = "no codec is available for this publish",
    label = "this codec position resolves to nothing",
    note = "enable a codec feature on `ruststream` (`json`, `cbor` or `msgpack`), name one for \
            this publish with `.with_codec(JsonCodec)`, or give the message type its own bytes \
            with `#[derive(Serialized)]` so no codec is needed"
)]
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

/// The codec position of a publish nothing named a codec for: the bottom of the ladder.
///
/// It stands in the `Enc` position of every surface that carries no codec of its own (a bare
/// [`Publisher`](crate::Publisher) through [`PublishExt`](super::PublishExt), the test harness),
/// so those entry points exist whatever the build. Whether it *resolves* is the build's business:
/// it implements [`PublishCodec`] (as [`DefaultCodec`](crate::codec::DefaultCodec)) only when a
/// codec feature is on. So a publish that needs encoding and never named a codec is a compile
/// error naming the fix, while a `#[derive(Serialized)]` value - which asks nothing of the codec
/// position - publishes with no codec feature at all.
///
/// You never name this type; the entry points return it and the compiler carries it.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # {
/// use ruststream::codec::JsonCodec;
/// use ruststream::runtime::UnnamedCodec;
///
/// // The position is inert on its own; a publish resolves it, or names a codec over it.
/// let unnamed = UnnamedCodec::new();
/// let _ = (unnamed, JsonCodec);
/// # }
/// ```
// A codec is stateless but carries no equality or hash of its own, so this position derives
// exactly what every codec provides.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnnamedCodec {
    // The resolution is the only feature-dependent part: with a codec feature the position owns
    // the default codec the ladder falls back to, and without one it owns nothing and satisfies
    // no bound.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    resolved: crate::codec::DefaultCodec,
}

impl UnnamedCodec {
    /// The codec position of a surface that names no codec.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl PublishCodec for UnnamedCodec {
    type Codec = crate::codec::DefaultCodec;

    fn codec(&self) -> &Self::Codec {
        &self.resolved
    }
}

/// A codec named at the call site, the most specific level of the codec ladder.
///
/// Produced by [`PublishBuilder::with_codec`](super::PublishBuilder::with_codec); you never construct it
/// directly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CallCodec<C>(pub(crate) C);

impl<C: Codec> PublishCodec for CallCodec<C> {
    type Codec = C;

    fn codec(&self) -> &C {
        &self.0
    }
}
