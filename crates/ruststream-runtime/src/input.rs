//! The input axis: how one delivery materializes into the handler's message parameter.
//!
//! A definition names its input as a marker type: [`Decoded<T>`] decodes the payload with the
//! scope codec and lends the handler `&T`, [`Provided<F>`] lends the payload itself as `&[u8]`
//! so the [`Deserialized`](crate::Deserialized) type `F` constructs itself from it -
//! no codec, no copy - [`DecodedPair<H, P>`] decodes the payload and the delivery's typed
//! header contract together, lending a [`Message<H, P>`](crate::Message) pair, and
//! [`Carried<T>`] lends the value the delivery already holds ([`Carries`]). The adapter holds
//! what one delivery materializes into for the duration of the call ([`Materialize::Held`], on
//! its stack) and the handler borrows a reference to [`InputKind::Target`], so no allocation,
//! copying, or boxing appears on the delivery path: the provided and carried forms borrow
//! straight out of the delivery on every broker.

use alloc::boxed::Box;
use alloc::format;
use alloc::string::ToString;
use alloc::vec::Vec;
use core::any::type_name;
use core::fmt;
use core::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::Message;
use crate::codec::{Codec, CodecError};
use crate::handle::{Input, Solo, SoloSet};
use crate::in_messages::DecodeSet;
use crate::metadata::IncomingMessageMetadata;
use crate::{Carries, IncomingMessage, LocalSettle, SendSource, Subscriber};

#[cfg(feature = "testing")]
use super::context::Context;

/// What a delivery its broker reports as undecodable ([`IncomingMessage::decode_error`]) is logged
/// as, beside the subscription, the type and the reported error. Every lane, single and batch,
/// logs it the same way.
pub(crate) const REPORTED: &str = "the broker reports the delivery does not decode";

/// One kind of handler input: the owned element of a batch and the borrowed view lent to the
/// handler.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a handler input kind",
    note = "the input type selects its kind: `Decoded<T>` for a `serde` type, `Provided<F>` for \
            a `Deserialized` one"
)]
pub trait InputKind: Send + Sync + 'static {
    /// True on the self-deserializing lane: the input has no serde model, so its missing JSON
    /// Schema is by design rather than a documentation gap.
    const DESERIALIZED: bool = false;

    /// One element of a batch of this kind, as the batch adapters hand the slice to the body.
    type Owned: Send + Sync;

    /// What the handler borrows: `&T` for a decoded or carried input, `[u8]` behind the reference
    /// for a raw one.
    type Target: ?Sized + Sync;

    /// The label `AsyncAPI` metadata uses for this input.
    fn input_label() -> &'static str;
}

/// An [`InputKind`] that decodes one batch element with the codec `C`, owning the result.
///
/// The batch adapters build their slice out of these, so only the kinds whose product owns
/// nothing of the delivery implement it. The delivery itself is what a kind decodes from, so a
/// kind reads only what it needs of it: a payload input never asks for the header map, and a
/// pair input (`DecodedPair`) asks for it because its typed header contract materializes in the
/// same stage, under the same decode failure policy.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be decoded with the codec `{DecodeCodec}`",
    note = "a typed input derives `serde::Deserialize` or `InMessages`; a `Deserialized` input \
            decodes with any codec (it never calls one)",
    note = "a batch of values a broker's batches carry mounts in the plain batch form only, \
            with no reply and no `Out` slots"
)]
pub trait DecodeWith<DecodeCodec>: InputKind {
    /// The media type this input arrives in, when a codec decodes it at all: the codec's own
    /// [`CONTENT_TYPE`](crate::codec::Codec::CONTENT_TYPE). `None` on the self-deserializing
    /// lane, whose bytes are their own wire format and never reach a codec.
    ///
    /// It rides here rather than on a `Codec` bound at the mount site, because a byte input
    /// mounts with no codec at all: the kind that uses one is the kind that can name it.
    const CONTENT_TYPE: Option<&'static str> = None;

    /// Decodes one delivery's payload (and, for a pair input, its headers).
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the payload or the header contract does not decode; the
    /// adapter applies the definition's decode failure policy.
    fn decode<M: IncomingMessage>(codec: &DecodeCodec, msg: &M) -> Result<Self::Owned, CodecError>;

    /// The models of the input's message set, for the generated document: one per variant of
    /// an `InMessages` enum, none for an input that is one message.
    #[must_use]
    fn incoming() -> Vec<IncomingMessageMetadata> {
        Vec::new()
    }
}

/// How one kind materializes from a delivery of the message type `M`, for the single-delivery
/// adapters: what the adapter holds across the handler call, and the view the handler borrows.
///
/// The message type is part of the trait because a kind may borrow what it lends straight out
/// of the delivery: the carried lane's value is the broker's, and only a delivery that
/// [`Carries`] it can lend it. `Decoder` is what the mount resolved for the kind
/// ([`DecodeSite`]): the codec of a decoded input, the subscription itself for a carried one.
pub trait Materialize<Decoder, M>: InputKind {
    /// The media type this input arrives in, when a codec decodes it. See
    /// [`DecodeWith::CONTENT_TYPE`].
    const CONTENT_TYPE: Option<&'static str> = None;

    /// What a failure to materialize is logged as, beside the subscription and the type.
    const FAILURE: &'static str = "codec decode failed";

    /// What the adapter holds across the call: the decode product, or a borrow of the delivery.
    type Held<'m>: Send + Sync
    where
        M: 'm;

    /// Materializes one delivery.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the delivery does not materialize; the adapter applies the
    /// definition's decode failure policy.
    fn materialize<'m>(decoder: &Decoder, msg: &'m M) -> Result<Self::Held<'m>, CodecError>;

    /// Lends the handler its view of what the delivery materialized into.
    fn view<'a>(held: &'a Self::Held<'_>, msg: &'a M) -> &'a Self::Target;

    /// The models of the input's message set, for the generated document. See
    /// [`DecodeWith::incoming`].
    #[must_use]
    fn incoming() -> Vec<IncomingMessageMetadata> {
        Vec::new()
    }

    /// Records the materialized value for the test harness, on the lane whose value never was
    /// bytes. The other kinds are recorded from the payload, so they record nothing here.
    #[cfg(feature = "testing")]
    fn record<C, S>(_held: &Self::Held<'_>, _ctx: &mut Context<'_, C, S>) {}
}

/// How one axis of the typed lane decodes a delivery into `T` with the codec of the mount:
/// [`Solo<T>`] through the codec, for a `serde` type; [`SoloSet<T>`] through the set's own rule,
/// for an [`InMessages`] enum. Machinery behind [`Decoded`]; never named in user code.
#[doc(hidden)]
pub trait TypedLane<T, DecodeCodec> {
    /// Decodes one delivery.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the delivery does not decode; the adapter applies the
    /// definition's decode failure policy.
    fn decode<M: IncomingMessage>(codec: &DecodeCodec, msg: &M) -> Result<T, CodecError>;

    /// The models of the type's message set, for the generated document: one per variant of an
    /// `InMessages` enum, none for a type that is one message.
    #[must_use]
    fn incoming() -> Vec<IncomingMessageMetadata> {
        Vec::new()
    }
}

impl<T: DeserializeOwned, DecodeCodec: Codec> TypedLane<T, DecodeCodec> for Solo<T> {
    fn decode<M: IncomingMessage>(codec: &DecodeCodec, msg: &M) -> Result<T, CodecError> {
        codec.decode(msg.payload())
    }
}

impl<T: DecodeSet<DecodeCodec>, DecodeCodec: Codec> TypedLane<T, DecodeCodec> for SoloSet<T> {
    fn decode<M: IncomingMessage>(codec: &DecodeCodec, msg: &M) -> Result<T, CodecError> {
        <T as DecodeSet<DecodeCodec>>::decode(codec, msg)
    }

    fn incoming() -> Vec<IncomingMessageMetadata> {
        <T as DecodeSet<DecodeCodec>>::incoming()
    }
}

/// What a mount hands the decode step of one kind, given the codec it resolved and the
/// subscription it mounts on: the codec itself for the kinds that decode bytes, the
/// subscription for a carried kind, whose value comes from the subscription's deliveries.
pub trait DecodeSite<Codec, Src, Conn> {
    /// What the adapter is built with.
    type Decoder: Clone + Send + Sync + 'static;

    /// Builds it from the resolved codec.
    fn decoder(codec: Codec) -> Self::Decoder;
}

/// The typed input kind: the payload decodes into an owned `T`, the handler borrows `&T`.
pub struct Decoded<T>(PhantomData<T>);

impl<T> fmt::Debug for Decoded<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decoded").finish_non_exhaustive()
    }
}

impl<T: Send + Sync + 'static> InputKind for Decoded<T> {
    type Owned = T;
    type Target = T;

    fn input_label() -> &'static str {
        type_name::<T>()
    }
}

// The typed lane decodes by the type's own axis: a `serde` type through the codec, a message
// set through its rule. Both are `Input` impls the type carries, so the bound names what the
// mount site sees.
impl<DecodeCodec, T> DecodeWith<DecodeCodec> for Decoded<T>
where
    DecodeCodec: Codec,
    T: Input + Send + Sync + 'static,
    T::Axis: TypedLane<T, DecodeCodec>,
{
    const CONTENT_TYPE: Option<&'static str> = Some(DecodeCodec::CONTENT_TYPE);

    fn decode<M: IncomingMessage>(codec: &DecodeCodec, msg: &M) -> Result<T, CodecError> {
        <T::Axis as TypedLane<T, DecodeCodec>>::decode(codec, msg)
    }

    fn incoming() -> Vec<IncomingMessageMetadata> {
        <T::Axis as TypedLane<T, DecodeCodec>>::incoming()
    }
}

impl<DecodeCodec, T, M> Materialize<DecodeCodec, M> for Decoded<T>
where
    DecodeCodec: Codec,
    T: Input + Send + Sync + 'static,
    T::Axis: TypedLane<T, DecodeCodec>,
    M: IncomingMessage,
{
    const CONTENT_TYPE: Option<&'static str> = Some(DecodeCodec::CONTENT_TYPE);

    type Held<'m>
        = T
    where
        M: 'm;

    fn materialize(codec: &DecodeCodec, msg: &M) -> Result<T, CodecError> {
        <T::Axis as TypedLane<T, DecodeCodec>>::decode(codec, msg)
    }

    fn view<'a>(held: &'a T, _msg: &'a M) -> &'a T {
        held
    }

    fn incoming() -> Vec<IncomingMessageMetadata> {
        <T::Axis as TypedLane<T, DecodeCodec>>::incoming()
    }
}

impl<T, Codec, Src, Conn> DecodeSite<Codec, Src, Conn> for Decoded<T>
where
    Codec: Clone + Send + Sync + 'static,
{
    type Decoder = Codec;

    fn decoder(codec: Codec) -> Codec {
        codec
    }
}

/// The self-deserializing input kind: no codec runs.
///
/// The adapter lends the payload bytes as delivered and the
/// [`Deserialized`](crate::Deserialized) family `F` constructs its borrowed form from
/// them right before the body runs.
pub struct Provided<F>(PhantomData<F>);

impl<F> fmt::Debug for Provided<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Provided").finish_non_exhaustive()
    }
}

impl<F: Send + Sync + 'static> InputKind for Provided<F> {
    const DESERIALIZED: bool = true;

    type Owned = ();
    type Target = [u8];

    fn input_label() -> &'static str {
        type_name::<F>()
    }
}

impl<DecodeCodec, F: Send + Sync + 'static> DecodeWith<DecodeCodec> for Provided<F> {
    fn decode<M: IncomingMessage>(_codec: &DecodeCodec, _msg: &M) -> Result<(), CodecError> {
        Ok(())
    }
}

impl<DecodeCodec, F, M> Materialize<DecodeCodec, M> for Provided<F>
where
    F: Send + Sync + 'static,
    M: LocalSettle,
{
    type Held<'m>
        = ()
    where
        M: 'm;

    fn materialize(_codec: &DecodeCodec, _msg: &M) -> Result<(), CodecError> {
        Ok(())
    }

    fn view<'a>(_held: &'a (), msg: &'a M) -> &'a [u8] {
        msg.payload()
    }
}

impl<F, Codec, Src, Conn> DecodeSite<Codec, Src, Conn> for Provided<F>
where
    Codec: Clone + Send + Sync + 'static,
{
    type Decoder = Codec;

    fn decoder(codec: Codec) -> Codec {
        codec
    }
}

/// The pair input kind: the payload decodes into `P` and the delivery's headers into the
/// contract `H`, both under the same decode failure policy; the handler borrows the
/// [`Message<H, P>`](crate::Message) pair.
pub struct DecodedPair<H, P>(PhantomData<(H, P)>);

impl<H, P> fmt::Debug for DecodedPair<H, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecodedPair").finish_non_exhaustive()
    }
}

impl<H, P> InputKind for DecodedPair<H, P>
where
    H: Send + Sync + 'static,
    P: Send + Sync + 'static,
{
    type Owned = Message<H, P>;
    type Target = Message<H, P>;

    fn input_label() -> &'static str {
        type_name::<P>()
    }
}

/// Decodes a pair: the header contract off the map, the payload through the codec.
fn decode_pair<DecodeCodec, H, P, M>(
    codec: &DecodeCodec,
    msg: &M,
) -> Result<Message<H, P>, CodecError>
where
    DecodeCodec: Codec,
    H: DeserializeOwned,
    P: DeserializeOwned,
    M: IncomingMessage,
{
    let contract: H = msg
        .headers()
        .to_typed()
        .map_err(|err| CodecError::Decode(Box::from(err.to_string())))?;
    let body: P = codec.decode(msg.payload())?;
    Ok(Message::new(contract, body))
}

impl<DecodeCodec: Codec, H, P> DecodeWith<DecodeCodec> for DecodedPair<H, P>
where
    H: DeserializeOwned + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
{
    const CONTENT_TYPE: Option<&'static str> = Some(DecodeCodec::CONTENT_TYPE);

    fn decode<M: IncomingMessage>(codec: &DecodeCodec, msg: &M) -> Result<Self::Owned, CodecError> {
        decode_pair(codec, msg)
    }
}

impl<DecodeCodec, H, P, M> Materialize<DecodeCodec, M> for DecodedPair<H, P>
where
    DecodeCodec: Codec,
    H: DeserializeOwned + Send + Sync + 'static,
    P: DeserializeOwned + Send + Sync + 'static,
    M: IncomingMessage,
{
    const CONTENT_TYPE: Option<&'static str> = Some(DecodeCodec::CONTENT_TYPE);

    type Held<'m>
        = Message<H, P>
    where
        M: 'm;

    fn materialize(codec: &DecodeCodec, msg: &M) -> Result<Message<H, P>, CodecError> {
        decode_pair(codec, msg)
    }

    fn view<'a>(held: &'a Message<H, P>, _msg: &'a M) -> &'a Message<H, P> {
        held
    }
}

impl<H, P, Codec, Src, Conn> DecodeSite<Codec, Src, Conn> for DecodedPair<H, P>
where
    Codec: Clone + Send + Sync + 'static,
{
    type Decoder = Codec;

    fn decoder(codec: Codec) -> Codec {
        codec
    }
}

/// The carried input kind: the delivery lends the value it already holds ([`Carries<T>`]), and
/// the handler borrows it where it lies. No codec runs and nothing is copied.
///
/// `T: Clone` is the lane's own requirement in every build: the test harness records the value
/// by cloning it, and a bound present only with the `testing` feature would make that feature
/// break a build that compiles without it. The clone itself runs only under the harness.
pub struct Carried<T>(PhantomData<T>);

impl<T> fmt::Debug for Carried<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Carried").finish_non_exhaustive()
    }
}

impl<T: Clone + Send + Sync + 'static> InputKind for Carried<T> {
    type Owned = T;
    type Target = T;

    fn input_label() -> &'static str {
        type_name::<T>()
    }
}

/// What a carried kind decodes with: the subscription `Src` on the connected broker `Conn`,
/// whose deliveries lend the value. Zero-sized; the types are the whole of it.
pub struct Lend<Src, Conn>(PhantomData<fn() -> (Src, Conn)>);

impl<Src, Conn> Lend<Src, Conn> {
    const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Src, Conn> Clone for Lend<Src, Conn> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Src, Conn> Copy for Lend<Src, Conn> {}

impl<Src, Conn> fmt::Debug for Lend<Src, Conn> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lend").finish_non_exhaustive()
    }
}

impl<T, Codec, Src, Conn> DecodeSite<Codec, Src, Conn> for Carried<T>
where
    Src: 'static,
    Conn: 'static,
{
    type Decoder = Lend<Src, Conn>;

    fn decoder(_codec: Codec) -> Lend<Src, Conn> {
        Lend::new()
    }
}

/// A subscription whose deliveries lend a `T`: what a handler taking `&T` on the carried lane
/// asks of the subscription it mounts on.
///
/// It names the subscription rather than its delivery type, so mounting such a handler on a
/// subscription that carries nothing is an error about the subscription the mount site wrote.
#[diagnostic::on_unimplemented(
    message = "the subscription `{Self}` does not carry `{T}`",
    label = "a `&{T}` handler reads the value this subscription's deliveries hold",
    note = "mount it on a subscription whose deliveries implement `Carries<{T}>`; a `{T}` that \
            arrives as bytes derives `Deserialize` instead and rides the codec"
)]
pub trait SourceCarries<T, Conn> {
    /// The subscription's delivery type.
    type Delivery;

    /// The value one delivery lends.
    fn lend(delivery: &Self::Delivery) -> Option<&T>;
}

// The nested obligation (the delivery type's `Carries`) is the machinery of the check, not the
// mistake: the trait's own message names the subscription and the type, so the impl stays out of
// the error.
#[diagnostic::do_not_recommend]
impl<T, Conn, Src> SourceCarries<T, Conn> for Src
where
    Conn: crate::ConnectedBroker,
    Src: SendSource<Conn>,
    <Src::Subscriber as Subscriber>::Message: Carries<T>,
{
    type Delivery = <Src::Subscriber as Subscriber>::Message;

    fn lend(delivery: &Self::Delivery) -> Option<&T> {
        delivery.carried()
    }
}

impl<T, Src, Conn, M> Materialize<Lend<Src, Conn>, M> for Carried<T>
where
    T: Clone + Send + Sync + 'static,
    Src: SourceCarries<T, Conn, Delivery = M>,
    M: IncomingMessage,
{
    const FAILURE: &'static str = "the delivery carries no value";

    type Held<'m>
        = &'m T
    where
        M: 'm;

    fn materialize<'m>(_decoder: &Lend<Src, Conn>, msg: &'m M) -> Result<&'m T, CodecError> {
        Src::lend(msg).ok_or_else(|| {
            CodecError::Decode(Box::from(format!(
                "the delivery carries no `{}`",
                type_name::<T>()
            )))
        })
    }

    fn view<'a>(held: &'a &T, _msg: &'a M) -> &'a T {
        held
    }

    #[cfg(feature = "testing")]
    fn record<C, S>(held: &&T, ctx: &mut Context<'_, C, S>) {
        ctx.record_carried(*held);
    }
}
