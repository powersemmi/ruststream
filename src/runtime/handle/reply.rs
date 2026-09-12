//! The reply cells of the matrix: a sealed definition whose body declared a reply mounts
//! through the publishing machinery, with the policy the chain attached (or the broker's
//! default) committed right at `include`.

use std::any::type_name;

use crate::runtime::batch::BatchResult;
use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingDef};
use crate::runtime::context::Context;
use crate::runtime::handler::HandlerOutcome;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publishing::{PublishingCall, PublishingDef};
use crate::runtime::router::{IncludeDef, forms};
use crate::{CallerName, FixedName, Name, OutgoingDestination, Unnamed};

use super::Handle;
use super::axis::{
    Axis, AxisDocs, Batch, BatchPair, BatchedAxis, Deserialized, Input, Message, Solo, SoloAxis,
    SoloDeserialized, SoloPair,
};
use super::docs::DocState;
use super::eager::construct;
use super::value::{
    DeclaredDest, EncodedReply, HandleValue, NamedDest, ReplyValue, ResolvedDest, Sealed,
    SerializedReply,
};
use super::verdict::{Batched, OneByOne};
// The self-serialized vocabulary lives with the publish builder (the general wire seam serves
// every typed surface); re-exported here so the reply seam keeps reading as one module.
pub use crate::runtime::publish::Serialized;

/// The shape and wire of a reply value: what payload it publishes, what typed header contract
/// rides it, and whether the framework's codec serializes it.
///
/// Implemented for every `serde::Serialize` type and every [`Message`] pair of them (the
/// [`EncodedReply`] wire), and per-type for [`Serialized`] replies (the [`SerializedReply`]
/// wire) - `#[derive(Serialized)]` writes that impl, or see [`Serialized`] for the hand-written
/// form.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a reply value",
    note = "a reply is a `serde::Serialize` value (the reply codec encodes it), a \
            `Message<Headers, Payload>` pair of them, or a `#[derive(Serialized)]` type (its \
            bytes leave as they are)"
)]
pub trait ReplyShape: Send + Sync {
    /// The published payload type (the pair's body, or the reply itself).
    #[doc(hidden)]
    type Body: Send + Sync;
    /// The typed header contract riding the reply (`()` when none does).
    #[doc(hidden)]
    type Headers;
    /// The reply's wire: [`EncodedReply`] or [`SerializedReply`].
    type Wire;
}

impl<R: serde::Serialize + Send + Sync> ReplyShape for R {
    type Body = R;
    type Headers = ();
    type Wire = EncodedReply;
}

impl<H, P> ReplyShape for Message<H, P>
where
    H: serde::Serialize + Send + Sync,
    P: serde::Serialize + Send + Sync,
{
    type Body = P;
    type Headers = H;
    type Wire = EncodedReply;
}

/// The reply's headers schema, produced only where headers actually ride the reply (a unit
/// contract reports nothing rather than the unit type's schema).
#[doc(hidden)]
pub trait ReplyHeadersSchema<Doc>: ReplyShape {
    fn headers_schema() -> Option<String>;
}

impl<R: serde::Serialize + Send + Sync, Doc> ReplyHeadersSchema<Doc> for R {
    fn headers_schema() -> Option<String> {
        None
    }
}

impl<H, P, Doc> ReplyHeadersSchema<Doc> for Message<H, P>
where
    H: serde::Serialize + Send + Sync,
    P: serde::Serialize + Send + Sync,
    Doc: DocState<H>,
{
    fn headers_schema() -> Option<String> {
        Doc::schema()
    }
}

/// What one documentation state reports for one reply wire: the encoded wire reports the
/// reply's schemas, the serialized wire has no serde model to report. Machinery behind the
/// generated document; keyed by the wire marker so a `Serialized` reply mounts documented
/// without a `JsonSchema` obligation.
#[doc(hidden)]
pub trait WireDocs<R: ReplyShape + ?Sized, Doc> {
    /// True on the serialized wire: the missing payload schema is by design there.
    const SERIALIZED: bool;

    /// The serialized JSON Schema of the reply payload.
    fn payload_schema() -> Option<String>;

    /// The serialized JSON Schema of the typed header contract riding the reply.
    fn headers_schema() -> Option<String>;
}

impl<R, Doc> WireDocs<R, Doc> for EncodedReply
where
    R: ReplyShape + ReplyHeadersSchema<Doc>,
    Doc: DocState<R::Body>,
{
    const SERIALIZED: bool = false;

    fn payload_schema() -> Option<String> {
        <Doc as DocState<R::Body>>::schema()
    }

    fn headers_schema() -> Option<String> {
        <R as ReplyHeadersSchema<Doc>>::headers_schema()
    }
}

impl<R: ReplyShape + ?Sized, Doc> WireDocs<R, Doc> for SerializedReply {
    const SERIALIZED: bool = true;

    fn payload_schema() -> Option<String> {
        None
    }

    fn headers_schema() -> Option<String> {
        None
    }
}

/// Resolves one declared destination form against the mount site's name: a fixed name is the
/// destination and the mount-site name does not apply, a type declaring none takes it.
///
/// [`NameTemplate`](crate::NameTemplate) has no impl on purpose: a reply is published by the
/// runtime, which has nothing to bind the placeholders with.
#[doc(hidden)]
pub trait ResolveDestination<Form> {
    /// The destination, given the name the mount site supplied.
    fn resolve(default: &str) -> &str;
}

impl<R: OutgoingDestination<Form = FixedName>> ResolveDestination<FixedName> for R {
    fn resolve(_default: &str) -> &str {
        <R as OutgoingDestination>::DESTINATION
    }
}

impl<R: OutgoingDestination<Form = CallerName>> ResolveDestination<CallerName> for R {
    fn resolve(default: &str) -> &str {
        default
    }
}

/// Where a reply type is published, resolved from its payload's own declaration and the
/// mount-site name.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not declare where it is published",
    note = "derive `Outgoing` on the reply type: `#[outgoing(name = \"orders.done\")]` fixes the \
            destination, and a derive without a name is published where the mount site says \
            (`publish(\"..\")` on the attribute, `.to(\"..\")` on the chain). A name template has \
            no placeholders to bind on the reply path."
)]
pub trait ReplyDestination {
    /// The destination, given the name the mount site supplied.
    fn destination(default: &str) -> &str;
}

// The two nested obligations are the machinery of the resolution, not the user's mistake: the
// trait's own note names the derive, so the impl stays out of the error.
#[diagnostic::do_not_recommend]
impl<R> ReplyDestination for R
where
    R: ReplyShape<Body: OutgoingDestination>,
    R::Body: ResolveDestination<<R::Body as OutgoingDestination>::Form>,
{
    fn destination(default: &str) -> &str {
        <R::Body as ResolveDestination<<R::Body as OutgoingDestination>::Form>>::resolve(default)
    }
}

/// The reply type a definition produces, as a mount chain can read it before the registration
/// commits.
///
/// A definition names its reply type the moment it exists - the handler's return type does not
/// depend on the policies a chain is still binding - so a step that has to know the reply, like a
/// `.transform(..)` mounting a transform that names the destination, reads it here. Implemented by
/// `#[subscriber]` next to the definition and by the value path's sealed reply definition; you
/// never name it.
#[doc(hidden)]
pub trait DeclaresReply {
    /// The reply type, as [`ReplyShape`] sees it.
    type Reply;

    /// The broker's typed per-delivery context the handler reads, which is what a transform on
    /// this reply sees through its [`PublishContext`](crate::runtime::PublishContext).
    type Context;
}

impl<A, R, O, C, H, Doc, Dest> DeclaresReply
    for Sealed<ReplyValue<HandleValue<A, R, O, C, H, Doc>, Dest>>
{
    type Reply = R;
    type Context = C;
}

/// A reply whose destination a mount chain may name per delivery.
///
/// A reply type that declares its own channel (`#[outgoing(name = "..")]`) is published there, and
/// the generated document says so; letting a transform move it would put the two back out of
/// step, which is the whole reason the destination lives on the type. So a transform that
/// declares `Destination = Names` mounts only on a reply type that leaves the destination open -
/// the mount site's `publish("dest")` name is then the declared fallback, and the transform names
/// where each answer actually goes.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` declares where it is published, so a transform cannot name its destination",
    label = "this reply's destination is its type's own",
    note = "drop `#[outgoing(name = \"..\")]` from the reply type and name the fallback at the \
            mount site (`publish(\"dest\")` on the attribute, `.to(\"dest\")` on the chain), or \
            publish the reply where its declaration says and mount a transform that declares \
            `Destination = Reads`"
)]
pub trait RedirectableReply {}

// The nested obligation is the machinery of the resolution, not the user's mistake: the trait's
// own note names the fix, so the impl stays out of the error.
#[diagnostic::do_not_recommend]
impl<R> RedirectableReply for R where R: ReplyShape<Body: OutgoingDestination<Form = CallerName>> {}

/// The destination of one reply type at the mount-site name, resolved at the expansion site.
/// Machinery behind the macro expansion; not part of the public API.
#[doc(hidden)]
#[must_use]
pub fn reply_destination<R: ReplyDestination>(default: &'static str) -> &'static str {
    R::destination(default)
}

/// The destination of one reply type that declares its own. Machinery behind the macro
/// expansion; not part of the public API.
#[doc(hidden)]
#[must_use]
pub fn declared_reply_destination<R>() -> &'static str
where
    R: ReplyShape<Body: OutgoingDestination<Form = FixedName>>,
{
    <R::Body as OutgoingDestination>::DESTINATION
}

/// Where a wired reply goes: the reply type's own declaration, or the mount-site name where the
/// type declares none.
#[doc(hidden)]
pub trait ReplyDest<R>: Send + Sync {
    /// The subject the reply publishes to.
    fn name(&self) -> &str;
}

// No obligation on `R`: the destination was resolved where the reply type was named, so a type
// that declares none is reported at the handler rather than at the chain that mounts it.
impl<R> ReplyDest<R> for ResolvedDest {
    fn name(&self) -> &str {
        self.0
    }
}

impl<R: ReplyDestination> ReplyDest<R> for NamedDest {
    fn name(&self) -> &str {
        R::destination(&self.0)
    }
}

impl<R> ReplyDest<R> for DeclaredDest
where
    R: ReplyShape<Body: OutgoingDestination<Form = FixedName>>,
{
    fn name(&self) -> &str {
        <R::Body as OutgoingDestination>::DESTINATION
    }
}

/// The form tokens of one reply wire on one verdict family: the sealed value-path tokens and
/// the attribute path's builder-producing forms, with and without slots. The serialized wire
/// has no batch form: a batch's replies publish through the reply codec.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this reply's wire does not mount on this input family",
    note = "an encoded reply (`serde::Serialize`) mounts one-by-one and per batch; a \
            `Serialized` (raw-byte) reply mounts one-by-one only"
)]
pub trait ReplyFormFor<Fam> {
    /// The mount token of a reply definition.
    type Form;
    /// The mount token of a reply definition that also carries slots.
    type SlotForm;
}

impl ReplyFormFor<OneByOne> for EncodedReply {
    type Form = forms::Publishing;
    type SlotForm = forms::PublishingOut;
}

impl ReplyFormFor<Batched> for EncodedReply {
    type Form = forms::BatchPublishing;
    type SlotForm = forms::BatchPublishingOut;
}

impl ReplyFormFor<OneByOne> for SerializedReply {
    type Form = forms::RawReply;
    type SlotForm = forms::RawReplyOut;
}

/// The route of one reply type on one verdict family: its wire, and the form tokens that wire
/// selects. One-by-one the reply type routes itself; per batch the `Vec<Reply>` verdict routes
/// by its element. Machinery behind `include` and the reply chain; never named in user code.
#[doc(hidden)]
pub trait ReplyRoute<Fam> {
    /// The reply's wire marker.
    type Wire: ReplyFormFor<Fam>;
    /// See [`ReplyFormFor::Form`].
    type Form;
    /// See [`ReplyFormFor::SlotForm`].
    type SlotForm;
}

impl<R> ReplyRoute<OneByOne> for R
where
    R: ReplyShape,
    R::Wire: ReplyFormFor<OneByOne>,
{
    type Wire = R::Wire;
    type Form = <R::Wire as ReplyFormFor<OneByOne>>::Form;
    type SlotForm = <R::Wire as ReplyFormFor<OneByOne>>::SlotForm;
}

impl<R> ReplyRoute<Batched> for Vec<R>
where
    R: ReplyShape,
    R::Wire: ReplyFormFor<Batched>,
{
    type Wire = R::Wire;
    type Form = <R::Wire as ReplyFormFor<Batched>>::Form;
    type SlotForm = <R::Wire as ReplyFormFor<Batched>>::SlotForm;
}

impl<A, R, C, H, Doc, Dest> IncludeDef
    for Sealed<ReplyValue<HandleValue<A, R, (), C, H, Doc>, Dest>>
where
    A: Axis,
    R: ReplyRoute<A::Family>,
{
    type Form = R::Form;
}
impl<A, R, C, H, Doc, Dest> PublishingDef
    for Sealed<ReplyValue<HandleValue<A, R, (), C, H, Doc>, Dest>>
where
    A: SoloAxis,
    R: ReplyShape<Wire: WireDocs<R, Doc>>,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    Dest: ReplyDest<R>,
{
    type Input = A::Kind;
    type Injections = ();
    type Reply = R;
    type Context = C;
    // See the eager cells: the settings builder carries the real source.
    type Source = Unnamed<Name>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn reply_name(&self) -> &str {
        self.0.dest.name()
    }

    fn description(&self) -> Option<&str> {
        self.0.value.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .value
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .value
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.value.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.value.docs.message_description
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        if let Some(declared) = &self.0.value.docs.outgoing {
            return declared.clone();
        }
        vec![
            OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R::Body>())
                .with_payload_schema(<R::Wire as WireDocs<R, Doc>>::payload_schema())
                .with_headers_schema(<R::Wire as WireDocs<R, Doc>>::headers_schema())
                .with_serialized(<R::Wire as WireDocs<R, Doc>>::SERIALIZED),
        ]
    }
}

impl<T, R, C, S, H, Doc, Dest> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<Solo<T>, R, (), C, H, Doc>, Dest>>
where
    Self: PublishingDef<Input = <Solo<T> as Axis>::Kind, Injections = (), Reply = R, Context = C>,
    T: Input<Axis = Solo<T>> + Send + Sync + 'static,
    R: ReplyShape,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<T, R, (), C, S>,
{
    async fn call(
        &self,
        input: &T,
        _injections: &(),
        ctx: &mut Context<'_, C, S>,
    ) -> Result<R, HandlerOutcome> {
        self.0.value.body.handle(input, &(), ctx).await
    }
}

impl<F, R, C, S, H, Doc, Dest> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<SoloDeserialized<F>, R, (), C, H, Doc>, Dest>>
where
    Self: PublishingDef<
            Input = <SoloDeserialized<F> as Axis>::Kind,
            Injections = (),
            Reply = R,
            Context = C,
        >,
    F: Deserialized + Send + Sync + 'static,
    for<'p> F::Output<'p>: Input<Axis = SoloDeserialized<F>>,
    R: ReplyShape,
    C: Send + Sync,
    S: Send + Sync,
    H: for<'p> Handle<F::Output<'p>, R, (), C, S>,
{
    async fn call(
        &self,
        input: &[u8],
        _injections: &(),
        ctx: &mut Context<'_, C, S>,
    ) -> Result<R, HandlerOutcome> {
        let input = construct::<F, C, S>(input, ctx)?;
        self.0.value.body.handle(&input, &(), ctx).await
    }
}

impl<Hd, P, R, C, S, H, Doc, Dest> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<SoloPair<Hd, P>, R, (), C, H, Doc>, Dest>>
where
    Self: PublishingDef<
            Input = <SoloPair<Hd, P> as Axis>::Kind,
            Injections = (),
            Reply = R,
            Context = C,
        >,
    Message<Hd, P>: Input<Axis = SoloPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    R: ReplyShape,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<Message<Hd, P>, R, (), C, S>,
{
    async fn call(
        &self,
        input: &Message<Hd, P>,
        _injections: &(),
        ctx: &mut Context<'_, C, S>,
    ) -> Result<R, HandlerOutcome> {
        self.0.value.body.handle(input, &(), ctx).await
    }
}
impl<A, R, C, H, Doc, Dest> BatchPublishingDef
    for Sealed<ReplyValue<HandleValue<A, Vec<R>, (), C, H, Doc>, Dest>>
where
    A: BatchedAxis,
    R: ReplyShape<Wire: WireDocs<R, Doc>>,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    Dest: ReplyDest<R>,
{
    type Input = A::Kind;
    type Injections = ();
    type Context = C;
    type Reply = R;
    type Source = Unnamed<Name>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn reply_name(&self) -> &str {
        self.0.dest.name()
    }

    fn description(&self) -> Option<&str> {
        self.0.value.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .value
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .value
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.value.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.value.docs.message_description
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        if let Some(declared) = &self.0.value.docs.outgoing {
            return declared.clone();
        }
        vec![
            OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R::Body>())
                .with_payload_schema(<R::Wire as WireDocs<R, Doc>>::payload_schema())
                .with_headers_schema(<R::Wire as WireDocs<R, Doc>>::headers_schema())
                .with_serialized(<R::Wire as WireDocs<R, Doc>>::SERIALIZED),
        ]
    }
}

/// Applies the batch reply contract: one reply per element, or one outcome per element.
pub(super) fn batch_reply_verdict<R>(
    verdict: Result<Vec<R>, Vec<HandlerOutcome>>,
    batch_len: usize,
    subscription: &str,
) -> Result<Vec<R>, BatchResult> {
    match verdict {
        Ok(replies) => {
            assert!(
                replies.len() == batch_len,
                "subscriber '{subscription}' returned {} replies for a batch of {batch_len}",
                replies.len(),
            );
            Ok(replies)
        }
        Err(outcomes) => {
            assert!(
                outcomes.len() == batch_len,
                "subscriber '{subscription}' returned {} per-element outcomes for a batch of \
                 {batch_len}",
                outcomes.len(),
            );
            Err(BatchResult::PerElement(outcomes))
        }
    }
}

impl<T, R, C, S, H, Doc, Dest> BatchPublishingCall<S>
    for Sealed<ReplyValue<HandleValue<Batch<T>, Vec<R>, (), C, H, Doc>, Dest>>
where
    Self: BatchPublishingDef<
            Input = <Batch<T> as Axis>::Kind,
            Injections = (),
            Context = C,
            Reply = R,
        >,
    [T]: Input<Axis = Batch<T>>,
    T: Send + Sync + 'static,
    R: ReplyShape,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[T], Vec<R>, (), C, S>,
{
    async fn call(
        &self,
        batch: &[T],
        _injections: &(),
        ctx: &mut Context<'_, C, S>,
    ) -> Result<Vec<R>, BatchResult> {
        let verdict = self.0.value.body.handle(batch, &(), ctx).await;
        batch_reply_verdict(verdict, batch.len(), ctx.name())
    }
}

impl<Hd, P, R, C, S, H, Doc, Dest> BatchPublishingCall<S>
    for Sealed<ReplyValue<HandleValue<BatchPair<Hd, P>, Vec<R>, (), C, H, Doc>, Dest>>
where
    Self: BatchPublishingDef<
            Input = <BatchPair<Hd, P> as Axis>::Kind,
            Injections = (),
            Context = C,
            Reply = R,
        >,
    [Message<Hd, P>]: Input<Axis = BatchPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    R: ReplyShape,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[Message<Hd, P>], Vec<R>, (), C, S>,
{
    async fn call(
        &self,
        batch: &[Message<Hd, P>],
        _injections: &(),
        ctx: &mut Context<'_, C, S>,
    ) -> Result<Vec<R>, BatchResult> {
        let verdict = self.0.value.body.handle(batch, &(), ctx).await;
        batch_reply_verdict(verdict, batch.len(), ctx.name())
    }
}
