//! The publish builder: one call shape for every publish, with the positions a message type
//! leaves open.
//!
//! A publish is a value (or bytes), a destination, optionally typed headers, and - for a value -
//! a codec. The builder carries one position per piece and resolves each at the most specific
//! level that names it, so the surfaces differ in what they *declare*, never in which method to
//! call. Which positions exist at a call site is decided by the message type's
//! [`OutgoingDestination`] declaration, so an under-specified publish does not compile.

use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use serde::Serialize;
use thiserror::Error;

use crate::codec::{Codec, CodecError};
use crate::{
    CallerName, FixedName, HeaderMap, HeadersContract, MessageHeaders, NameTemplate, NoHeaders,
    OutgoingDestination, OutgoingMessage, SerializeHeadersError, WithHeaders,
};

use super::sink::{CallCodec, PublishCodec, PublishSink};

/// A publish under construction: the entry point's payload plus the positions still open.
///
/// Built by the `message(..)` and `raw(..)` entry points of every publish surface (an
/// [`Out`](crate::runtime::Out) slot, a [`TypedPublisher`](super::TypedPublisher), a transaction
/// scope, any [`Publisher`](crate::Publisher) through [`PublishExt`](super::PublishExt)), and
/// finished by
/// [`publish`](Self::publish). The type parameters are the positions:
///
/// * `Sink` - where the bytes go, a [`PublishSink`].
/// * `Body` - [`MessageBody`] (a value to encode) or [`RawBody`] (bytes as they are).
/// * `Enc` - the codec position ([`PublishCodec`]); `()` on the byte entry point, which has none.
/// * `Hdrs` - [`HeadersUnset`], [`TypedHeaders`] or [`MapHeaders`].
/// * `Dest` - the destination: [`FixedName`] and [`SuppliedName`] are resolved, [`CallerName`]
///   and [`NameTemplate`] are still waiting for the call site.
///
/// You never name this type; the entry points return it and the compiler carries it.
#[must_use = "a publish builder does nothing until publish() is awaited"]
pub struct PublishBuilder<Sink, Body, Enc, Hdrs, Dest> {
    sink: Sink,
    body: Body,
    codec: Enc,
    headers: Hdrs,
    dest: Dest,
}

impl<Sink, Body, Enc, Hdrs, Dest> fmt::Debug for PublishBuilder<Sink, Body, Enc, Hdrs, Dest> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishBuilder").finish_non_exhaustive()
    }
}

/// The payload of a typed publish: the value to encode with the resolved codec.
#[derive(Debug, Clone, Copy)]
pub struct MessageBody<'a, T>(&'a T);

/// The payload of a byte publish: the bytes to send as they are.
#[derive(Debug, Clone, Copy)]
pub struct RawBody<'a>(&'a [u8]);

/// The headers position before anything fills it: the message travels with no headers of its own.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct HeadersUnset;

/// The headers position filled with a typed contract value, serialized into the header map one
/// entry per field when the publish runs.
#[derive(Debug, Clone, Copy)]
pub struct TypedHeaders<'a, H: ?Sized>(&'a H);

/// The headers position filled with an already-built [`HeaderMap`], sent as it is.
#[derive(Debug, Clone, Default)]
pub struct MapHeaders(HeaderMap);

/// A destination named at the call site, the resolved form of [`CallerName`].
#[derive(Debug, Clone)]
pub struct SuppliedName<'a>(Cow<'a, str>);

/// A destination that the publish can be sent to: fixed by the declaration, or named at the call.
///
/// This is what separates a publish that is ready to run from one whose call site has not said
/// where the message goes. [`CallerName`] and [`NameTemplate`] deliberately do not implement it.
#[diagnostic::on_unimplemented(
    message = "this publish has no destination yet",
    note = "a message type declaring no name takes one at the call: `.to(\"orders.archived\")`; \
            one declaring a name template opens its placeholder setters with `.to()`, and the \
            publish appears once every placeholder is bound"
)]
pub trait ResolvedName {
    /// The destination this publish goes to, given the type's declared address (used by the
    /// fixed form, ignored by the supplied one).
    fn resolve<'n>(&'n self, declared: &'n str) -> &'n str;
}

impl ResolvedName for FixedName {
    fn resolve<'n>(&'n self, declared: &'n str) -> &'n str {
        declared
    }
}

impl ResolvedName for SuppliedName<'_> {
    fn resolve<'n>(&'n self, _declared: &'n str) -> &'n str {
        &self.0
    }
}

/// What a headers position contributes to the outgoing message.
///
/// Implemented by the three header states; the publish calls it once, just before the message
/// leaves, against the map the sink's base
/// ([`base_headers`](crate::Publisher::base_headers)) already filled - empty when there is no
/// base. A position writes over that map key by key, which is what gives the call site the last
/// word over the handle it published through.
pub trait PublishHeaders {
    /// Writes this position's headers over the outgoing message's map, one key at a time.
    ///
    /// # Errors
    ///
    /// Returns [`SerializeHeadersError`] when a typed contract value is not a flat struct or
    /// string-keyed map.
    fn write(self, headers: &mut HeaderMap) -> Result<(), SerializeHeadersError>;
}

impl PublishHeaders for HeadersUnset {
    fn write(self, _headers: &mut HeaderMap) -> Result<(), SerializeHeadersError> {
        Ok(())
    }
}

impl<H: Serialize + ?Sized> PublishHeaders for TypedHeaders<'_, H> {
    fn write(self, headers: &mut HeaderMap) -> Result<(), SerializeHeadersError> {
        headers.insert_typed(self.0)
    }
}

impl PublishHeaders for MapHeaders {
    fn write(self, headers: &mut HeaderMap) -> Result<(), SerializeHeadersError> {
        headers.overwrite_with(self.0);
        Ok(())
    }
}

/// A headers position that satisfies a message's declared header contract.
///
/// The contract comes from the message type ([`MessageHeaders`]): a type declaring
/// `#[outgoing(headers = Meta)]` publishes only with `with_headers(&meta)`, and a type declaring
/// none publishes without typed headers. The byte entry point has no contract, so every position
/// is admissible there.
#[diagnostic::on_unimplemented(
    message = "the headers of this publish do not match the message's `{C}` contract",
    note = "a message declaring `#[outgoing(headers = Meta)]` is published as \
            `.message(&value).with_headers(&meta)`; one declaring no contract takes no contract \
            value (an arbitrary map still travels through `.with_headers(headers)`)"
)]
pub trait SatisfiesContract<C: HeadersContract> {}

impl SatisfiesContract<NoHeaders> for HeadersUnset {}

// A contract-less message may still carry arbitrary transport headers; that map says nothing
// about the message's declared shape, so it does not stand in for a contract.
impl SatisfiesContract<NoHeaders> for MapHeaders {}

impl<H> SatisfiesContract<WithHeaders<H>> for TypedHeaders<'_, H> {}

/// The error of the publish builder: encoding the payload, serializing the typed headers, or the
/// sink itself.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PublishError<E> {
    /// The codec failed to encode the message payload.
    #[error("encoding the message failed: {0}")]
    Encode(#[source] CodecError),
    /// The typed headers did not serialize into a header map.
    #[error("serializing the typed headers failed: {0}")]
    Headers(#[source] SerializeHeadersError),
    /// The sink (the broker publisher, or the transaction) rejected the message.
    #[error("publishing the message failed: {0}")]
    Publish(#[source] E),
}

impl<Sink, Body, Enc, Hdrs, Dest> PublishBuilder<Sink, Body, Enc, Hdrs, Dest> {
    /// The whole-tuple constructor the entry points share.
    pub(crate) const fn new(sink: Sink, body: Body, codec: Enc, headers: Hdrs, dest: Dest) -> Self {
        Self {
            sink,
            body,
            codec,
            headers,
            dest,
        }
    }
}

/// Starts a typed publish: `value` is encoded with `codec`, and the destination position is the
/// one the type declares.
pub(crate) fn message_of<Sink, T, Enc>(
    sink: Sink,
    value: &T,
    codec: Enc,
) -> PublishBuilder<Sink, MessageBody<'_, T>, Enc, HeadersUnset, T::Form>
where
    T: OutgoingDestination,
{
    PublishBuilder::new(
        sink,
        MessageBody(value),
        codec,
        HeadersUnset,
        <T::Form as Default>::default(),
    )
}

/// Starts a byte publish: the payload travels as it is, and the destination is named at the call.
pub(crate) fn raw_of<Sink, B>(
    sink: Sink,
    payload: &B,
) -> PublishBuilder<Sink, RawBody<'_>, (), HeadersUnset, CallerName>
where
    B: AsRef<[u8]> + ?Sized,
{
    PublishBuilder::new(
        sink,
        RawBody(payload.as_ref()),
        (),
        HeadersUnset,
        CallerName,
    )
}

impl<'a, Sink, T, Enc, Hdrs, Dest> PublishBuilder<Sink, MessageBody<'a, T>, Enc, Hdrs, Dest> {
    /// Names the codec for this publish, the most specific level of the codec ladder: it wins
    /// over the surface's codec, which in turn won over the application's and the crate default.
    ///
    /// Only the typed entry point has this position; bytes are sent as they are.
    pub fn with_codec<C: Codec>(
        self,
        codec: C,
    ) -> PublishBuilder<Sink, MessageBody<'a, T>, CallCodec<C>, Hdrs, Dest> {
        PublishBuilder::new(
            self.sink,
            self.body,
            CallCodec(codec),
            self.headers,
            self.dest,
        )
    }
}

/// What may fill the headers position of a publish: a borrowed contract value, or an
/// already-built [`HeaderMap`].
///
/// The two differ in what they say about the message, not in how they are written: a borrowed
/// value is the message's declared contract, serialized one entry per field, while a map is the
/// transport level and stands for no contract at all (a message type declaring one still demands
/// its value). Which of the two a call site passed rides in the type, so
/// [`with_headers`](PublishBuilder::with_headers) is one method and the contract check stays a compile
/// error either way.
///
/// You never implement it: [`HeaderMap`] and `&H` of every serializable `H` already do.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot fill the headers of a publish",
    note = "pass the message's declared contract by reference (`.with_headers(&meta)`), or an \
            already-built header map by value (`.with_headers(headers)`)"
)]
pub trait HeaderSource {
    /// The headers position this source resolves to: [`TypedHeaders`] or [`MapHeaders`].
    type Position;

    /// Moves the source into its position.
    fn into_position(self) -> Self::Position;
}

// A borrowed value is the contract form: it is serialized into the map when the publish runs, so
// the `Serialize` bound stays on `PublishHeaders` - an unserializable value must fail at the
// publish, with serde's own guidance, not at "no such method".
impl<'a, H: ?Sized> HeaderSource for &'a H {
    type Position = TypedHeaders<'a, H>;

    fn into_position(self) -> Self::Position {
        TypedHeaders(self)
    }
}

impl HeaderSource for HeaderMap {
    type Position = MapHeaders;

    fn into_position(self) -> Self::Position {
        MapHeaders(self)
    }
}

impl<Sink, Body, Enc, Dest> PublishBuilder<Sink, Body, Enc, HeadersUnset, Dest> {
    /// Supplies the headers of this publish: the message's declared contract by reference, or an
    /// already-built [`HeaderMap`] by value.
    ///
    /// A contract value is checked against the message's declaration, so a missing, extra or
    /// mismatched one is a compile error; a map carries whatever the caller put in it and stands
    /// for no contract, so a message type declaring one is not satisfied by a map. On the byte
    /// entry point either form is accepted: bytes declare no contract.
    ///
    /// The position is filled once. A borrow is cheap - nothing is serialized until the publish
    /// runs.
    pub fn with_headers<S: HeaderSource>(
        self,
        headers: S,
    ) -> PublishBuilder<Sink, Body, Enc, S::Position, Dest> {
        PublishBuilder::new(
            self.sink,
            self.body,
            self.codec,
            headers.into_position(),
            self.dest,
        )
    }
}

impl<Sink, Body, Enc, Hdrs> PublishBuilder<Sink, Body, Enc, Hdrs, CallerName> {
    /// Names the destination of this publish.
    ///
    /// Present when the message type declares no name of its own, and on the byte entry point.
    /// Pass a `&str` (the borrowed, no-allocation case) or a `String` (a computed one).
    pub fn to<'n>(
        self,
        name: impl Into<Cow<'n, str>>,
    ) -> PublishBuilder<Sink, Body, Enc, Hdrs, SuppliedName<'n>> {
        PublishBuilder::new(
            self.sink,
            self.body,
            self.codec,
            self.headers,
            SuppliedName(name.into()),
        )
    }
}

impl<Sink, T, Enc, Hdrs> PublishBuilder<Sink, MessageBody<'_, T>, Enc, Hdrs, NameTemplate>
where
    T: TemplateAddress<Self>,
{
    /// Opens the placeholder setters of a templated destination: one per `{placeholder}` of the
    /// type's declared name, in any order, with the publish terminal appearing once the last one
    /// is bound.
    ///
    /// An unbound placeholder rides in the returned builder's type, so the compile error names
    /// the segment that is missing.
    pub fn to(self) -> T::Builder {
        T::begin(self)
    }
}

/// A destination template's binding builder: what `to()` opens for a type whose declared name
/// carries placeholders.
///
/// Implemented by `#[derive(Outgoing)]` on a type with a templated `name`; the associated
/// [`Builder`](Self::Builder) is the generated value carrying one setter per placeholder. You
/// never implement it by hand.
pub trait TemplateAddress<Cont> {
    /// The generated builder, with every placeholder still unbound.
    type Builder;

    /// Opens the builder over the publish it will finish.
    fn begin(cont: Cont) -> Self::Builder;
}

/// The unbound state of one address placeholder.
///
/// The segment marker rides in the type so the compile error of an unfinished address names the
/// placeholder that was forgotten, the way an unbound [`Out`](crate::runtime::Out) slot names
/// itself. `#[derive(Outgoing)]` generates one marker per placeholder of the declared name.
#[derive(Debug, Clone, Copy, Default)]
pub struct MissingSegment<S>(PhantomData<fn() -> S>);

impl<S> MissingSegment<S> {
    /// The unbound placeholder, as the generated builder starts it.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

/// One address placeholder that carries a value, and so can be rendered into the destination.
///
/// The publish terminal of a generated address builder takes this witness once per placeholder,
/// so an address with a placeholder still in its [`MissingSegment`] state is rejected by a bound
/// the compiler can explain rather than by the terminal not existing. Implemented for the bound
/// representation only; you never implement it by hand.
#[diagnostic::on_unimplemented(
    message = "this address still has an unbound placeholder",
    note = "a name template opens one setter per placeholder, and the publish appears once \
            every one of them is bound"
)]
pub trait BoundSegment: fmt::Display {}

impl BoundSegment for String {}

/// A publish whose destination is rendered by a generated address builder.
///
/// The terminal a templated `to()` chain calls once every placeholder is bound; the rendered
/// address is the only thing the generated code adds to the publish the builder already carries.
pub trait PublishAt {
    /// The sink's error, as reported through [`PublishError`].
    type Error;

    /// Runs the publish against the rendered address.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError`] when the payload cannot be encoded, the typed headers cannot be
    /// serialized, or the sink rejects the message.
    fn publish_at(
        self,
        name: &str,
    ) -> impl Future<Output = Result<(), PublishError<Self::Error>>> + Send;
}

/// Sends one message, applying the headers position and the sink.
async fn deliver<Sink: PublishSink, Hdrs: PublishHeaders>(
    mut sink: Sink,
    name: &str,
    payload: &[u8],
    headers: Hdrs,
) -> Result<(), PublishError<Sink::Error>> {
    // The one place the outgoing map is built: the sink's base first, the publish's own headers
    // over it. A sink without a base yields the empty map this always started from, so nothing is
    // cloned and nothing is allocated on the path every publisher takes today.
    let mut map = sink.base_headers().cloned().unwrap_or_default();
    headers.write(&mut map).map_err(PublishError::Headers)?;
    let msg = OutgoingMessage::new(name, payload).with_headers(map);
    sink.send(msg).await.map_err(PublishError::Publish)
}

/// Encodes `value` and sends it, shared by the resolved and the rendered terminals.
async fn encode_and_send<Sink, T, Enc, Hdrs>(
    sink: Sink,
    codec: Enc,
    value: &T,
    headers: Hdrs,
    name: &str,
) -> Result<(), PublishError<Sink::Error>>
where
    Sink: PublishSink,
    T: Serialize + Sync,
    Enc: PublishCodec,
    Hdrs: PublishHeaders,
{
    let payload = codec.codec().encode(value).map_err(PublishError::Encode)?;
    deliver(sink, name, &payload, headers).await
}

impl<Sink, T, Enc, Hdrs, Dest> PublishBuilder<Sink, MessageBody<'_, T>, Enc, Hdrs, Dest>
where
    Sink: PublishSink,
    T: OutgoingDestination + MessageHeaders + Sync,
    Enc: PublishCodec,
    Hdrs: PublishHeaders,
{
    /// Encodes the value with the resolved codec and publishes it to the resolved destination.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError`] when the codec rejects the value, the typed headers do not
    /// serialize, or the sink rejects the message.
    ///
    /// # Cancel safety
    ///
    /// As cancel-safe as the sink's own send: dropping the future mid-flight may leave the
    /// message in an indeterminate state.
    // The position bounds sit on the method, not on the impl block: on the block an
    // under-specified publish reads as "method not found" and loses the guidance the bounds
    // carry, serde's own note about a missing derive included.
    pub async fn publish(self) -> Result<(), PublishError<Sink::Error>>
    where
        T: Serialize,
        Hdrs: SatisfiesContract<T::Contract>,
        Dest: ResolvedName,
    {
        let Self {
            sink,
            body,
            codec,
            headers,
            dest,
        } = self;
        // The declared address is the fixed form's name, borrowed straight from the declaration;
        // the supplied form ignores it and lends its own.
        let name = dest.resolve(T::ADDRESS);
        encode_and_send(sink, codec, body.0, headers, name).await
    }
}

impl<Sink, T, Enc, Hdrs> PublishAt for PublishBuilder<Sink, MessageBody<'_, T>, Enc, Hdrs, NameTemplate>
where
    Sink: PublishSink,
    T: OutgoingDestination + MessageHeaders + Serialize + Sync,
    Enc: PublishCodec + Send,
    Hdrs: PublishHeaders + SatisfiesContract<T::Contract> + Send,
{
    type Error = Sink::Error;

    async fn publish_at(self, name: &str) -> Result<(), PublishError<Sink::Error>> {
        encode_and_send(self.sink, self.codec, self.body.0, self.headers, name).await
    }
}

impl<Sink, Hdrs, Dest> PublishBuilder<Sink, RawBody<'_>, (), Hdrs, Dest>
where
    Sink: PublishSink,
    Hdrs: PublishHeaders,
{
    /// Publishes the bytes as they are, to the destination named at the call site.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError`] when the typed headers do not serialize, or the sink rejects the
    /// message.
    ///
    /// # Cancel safety
    ///
    /// As cancel-safe as the sink's own send: dropping the future mid-flight may leave the
    /// message in an indeterminate state.
    // On the method for the same reason as on the typed terminal: a bytes publish that never
    // named its destination has to read as the missing `to(..)`, not as a missing method.
    pub async fn publish(self) -> Result<(), PublishError<Sink::Error>>
    where
        Dest: ResolvedName,
    {
        let Self {
            sink,
            body,
            headers,
            dest,
            ..
        } = self;
        // Bytes declare no address of their own, so the destination is always the supplied one.
        let name = dest.resolve("");
        deliver(sink, name, body.0, headers).await
    }
}
