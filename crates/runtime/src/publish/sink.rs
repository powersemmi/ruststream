//! The two positions a publish builder resolves against the surface it started from: the byte
//! sink that finally takes the message, and the codec that encodes a value into it.

use core::error::Error as StdError;

use crate::codec::ActiveCodec as Codec;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::{
    HeaderSet, MaybeSend, MaybeSendSync, OutgoingFor, PayloadForm, Publisher, Transaction,
};
use ruststream_core::{PlaceRoom, Places};
pub use ruststream_core::{SinkOutcome, SinkRefusal};

/// The byte sink a publish builder resolves down to.
///
/// Every publish surface ends in one call carrying an [`OutgoingMessage`](crate::OutgoingMessage): a live
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
/// use ruststream::runtime::{PublishBuilder, PublishSink};
///
/// /// An AMQP broker's per-message settings: every field optional, so a call says only what it
/// /// changes.
/// #[derive(Clone, Default)]
/// struct AmqpOptions {
///     priority: Option<u8>,
/// }
///
/// /// The broker's own step. The bound on the sink's options is what puts it on a builder over
/// /// this broker's publisher, or a transaction on it, and on no other.
/// trait AmqpSteps {
///     #[must_use]
///     fn priority(self, priority: u8) -> Self;
/// }
///
/// impl<Sink, Body, Enc, Hdrs, Dest> AmqpSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
/// where
///     Sink: PublishSink<Options = AmqpOptions>,
/// {
///     fn priority(mut self, priority: u8) -> Self {
///         self.options_mut()
///             .get_or_insert_with(AmqpOptions::default)
///             .priority = Some(priority);
///         self
///     }
/// }
/// ```
pub trait PublishSink: MaybeSend {
    /// How the publisher or transaction underneath consumes the payload, forwarded so the
    /// builder encodes into the form it declared. See
    /// [`Publisher::Payload`](crate::Publisher::Payload).
    type Payload: PayloadForm;

    /// The error the sink reports when the message cannot be sent.
    type Error: StdError + Send + Sync + 'static;

    /// The per-message settings of the publisher or transaction underneath, forwarded so the
    /// publish builder can carry them.
    ///
    /// This is the type a broker's builder steps are bounded on: an extension trait written
    /// `where Sink: PublishSink<Options = MyOptions>` appears on a builder over that broker's
    /// publisher and on no other. See [`Publisher::Options`](crate::Publisher::Options).
    type Options: Clone + MaybeSendSync + 'static;

    /// The room of places the publish that sends through this sink holds: one place for a sink
    /// whose publisher keeps no places, and a room of none for one whose publisher keeps its own.
    /// Machinery; without `alloc` a refused message moves into one of the places, and with it the
    /// message is boxed and the room is a value of no size.
    #[doc(hidden)]
    type OwnPlaces: Default + MaybeSendSync;

    /// Sends one message into the sink, with the settings the builder's steps put on it.
    ///
    /// `own` is the publish's room of [`OwnPlaces`](Self::OwnPlaces): a refused message moves
    /// into it, or into the places the sink brings, and the refusal holds the place.
    ///
    /// # Errors
    ///
    /// Returns the failure of the broker (or the transaction buffer) rejecting the message: its
    /// [`Self::Error`] and, when the client gave it back, the message it did not send.
    ///
    /// # Cancel safety
    ///
    /// Inherited from the underlying publisher or transaction: dropping the future mid-flight
    /// may leave the message in an indeterminate state.
    fn send<'p>(
        &'p mut self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
        own: &'p Self::OwnPlaces,
    ) -> impl Future<Output = SinkOutcome<'p, Self::Error>> + MaybeSend;

    /// The headers the sink contributes underneath the publish's own, or `None` when it
    /// contributes nothing.
    ///
    /// This is where [`Publisher::base_headers`] and [`Transaction::base_headers`] reach the
    /// builder: the blanket impls forward theirs, and a sink of the runtime's own (the erased
    /// publisher behind deferred redelivery, the test harness's injection point) forwards
    /// whatever it wraps. The builder starts the outgoing header map from this and writes the
    /// publish's headers over it key by key, so the call site has the last word.
    fn base_headers(&self) -> Option<&HeaderSet> {
        None
    }
}

// A shared publisher reference is the ordinary sink: publishing takes `&self`, so the builder
// only ever borrows it. A broker's publisher keeps no places, so the publish keeps one of its own,
// which a refused message moves into.
impl<P: Publisher> PublishSink for &P {
    type Payload = P::Payload;
    type Error = P::Error;
    type Options = P::Options;
    type OwnPlaces = PlaceRoom<1>;

    fn send<'p>(
        &'p mut self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
        own: &'p PlaceRoom<1>,
    ) -> impl Future<Output = SinkOutcome<'p, Self::Error>> + MaybeSend {
        let publisher: &P = self;
        publisher.publish_kept(msg, options, own.places())
    }

    fn base_headers(&self) -> Option<&HeaderSet> {
        (**self).base_headers()
    }
}

// A transaction buffers into itself, so its sink is the unique borrow the builder holds for the
// duration of one publish.
impl<T: Transaction> PublishSink for &mut T {
    type Payload = T::Payload;
    type Error = T::Error;
    type Options = T::Options;
    type OwnPlaces = PlaceRoom<1>;

    // An `async fn`, so the transaction's publish future is built in the state that awaits it:
    // built first and captured by a block, it would be copied into the await slot on the first
    // poll, a copy of the whole client future per publish.
    async fn send<'p>(
        &'p mut self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
        own: &'p PlaceRoom<1>,
    ) -> SinkOutcome<'p, Self::Error> {
        (**self)
            .publish(msg, options)
            .await
            .map_err(|failed| SinkRefusal::returned::<T::Payload>(failed, own.places()))
    }

    fn base_headers(&self) -> Option<&HeaderSet> {
        (**self).base_headers()
    }
}

/// A publisher that keeps places of its own for the messages its refusals hand back: an `Out`
/// slot of a subscription. Machinery; never named in user code.
#[doc(hidden)]
pub trait KeepsPlaces: Publisher {
    /// The publisher's places.
    fn places(&self) -> Places<'_>;
}

/// The sink of a publisher that keeps places of its own: a refusal takes one of them, so the
/// publish keeps none. Machinery; the typed `message(..)` of such a publisher builds on it.
#[doc(hidden)]
#[derive(Debug)]
pub struct KeptPlaces<'a, P>(pub(crate) &'a P);

impl<P: KeepsPlaces> PublishSink for KeptPlaces<'_, P> {
    type Payload = P::Payload;
    type Error = P::Error;
    type Options = P::Options;
    type OwnPlaces = PlaceRoom<0>;

    fn send<'p>(
        &'p mut self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
        _own: &'p PlaceRoom<0>,
    ) -> impl Future<Output = SinkOutcome<'p, Self::Error>> + MaybeSend {
        let publisher: &P = self.0;
        publisher.publish_kept(msg, options, publisher.places())
    }

    fn base_headers(&self) -> Option<&HeaderSet> {
        self.0.base_headers()
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
    resolved: DefaultCodec,
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
    type Codec = DefaultCodec;

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

#[cfg(test)]
mod tests {
    use core::future::ready;
    use core::hint::black_box;
    use std::io;

    use super::*;
    use crate::{Lend, OutgoingMessage, PublishOutcome};

    /// A transaction whose publish future holds a large buffer across its await point, as a
    /// client's own buffered write does.
    struct LargeTransaction;

    impl Transaction for LargeTransaction {
        type Payload = Lend;
        type Error = io::Error;
        type Options = ();

        async fn publish<'msg>(
            &mut self,
            msg: OutgoingMessage<'msg, &'msg [u8]>,
            _options: Option<&()>,
        ) -> PublishOutcome<'msg, io::Error, Lend> {
            let buffer = [0_u8; 8192];
            ready(()).await;
            black_box((&buffer, msg));
            Ok(())
        }

        fn commit(self) -> impl Future<Output = Result<(), io::Error>> {
            ready(Ok(()))
        }

        fn abort(self) -> impl Future<Output = Result<(), io::Error>> {
            ready(Ok(()))
        }
    }

    #[test]
    fn a_transaction_sink_holds_the_publish_future_once() {
        let payload = b"order";
        let mut transaction = LargeTransaction;
        let publish =
            size_of_val(&transaction.publish(OutgoingMessage::new("orders", payload), None));
        let mut sink = &mut transaction;
        let own = PlaceRoom::<1>::new();
        let sent = size_of_val(&sink.send(OutgoingMessage::new("orders", payload), None, &own));
        let message = size_of::<OutgoingMessage<'_, &[u8]>>();
        // The sink adds the message it carries until the first poll, never a second copy of the
        // transaction's future.
        assert!(
            sent <= publish + message + 64,
            "send: {sent} bytes for a {publish}-byte future and a {message}-byte message"
        );
    }
}
