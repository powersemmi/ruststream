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
use crate::runtime::router::{
    BatchPublishMount, IncludeDef, PublishMount, RawReplyMount, Router, RouterCommit, RouterMount,
    forms,
};
use crate::runtime::settings::SubscriberBuilder;
use crate::{Broker, FixedName, Name, OutgoingDestination, Unnamed};

use super::Handle;
use super::axis::{
    Axis, AxisDocs, Deserialized, Input, Message, Page, PagePair, PagedAxis, Solo, SoloAxis,
    SoloDeserialized, SoloPair,
};
use super::docs::DocState;
use super::eager::construct;
use super::reply_slots::{SealedBatchPublishingOut, SealedPublishingOut, SealedRawReplyOut};
use super::value::{
    DeclaredDest, EncodedReply, HandleValue, NamedDest, ReplyValue, Sealed, SerializedReply,
};
use super::verdict::{OneByOne, Paged};

// ------------------------------------------------------------------------------ reply shapes

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

/// Where a wired reply goes: the chain-named subject, or the reply type's own declaration.
#[doc(hidden)]
pub trait ReplyDest<R>: Send + Sync {
    /// The subject the reply publishes to.
    fn name(&self) -> &str;
}

impl<R> ReplyDest<R> for NamedDest {
    fn name(&self) -> &str {
        &self.0
    }
}

impl<R> ReplyDest<R> for DeclaredDest
where
    R: ReplyShape<Body: OutgoingDestination<Form = FixedName>>,
{
    fn name(&self) -> &str {
        <R::Body as OutgoingDestination>::ADDRESS
    }
}

/// The form tokens of one reply wire on one verdict family: the sealed value-path tokens and
/// the attribute path's builder-producing forms, with and without slots. The serialized wire
/// has no page form: a page's replies publish through the reply codec.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this reply's wire does not mount on this input family",
    note = "an encoded reply (`serde::Serialize`) mounts one-by-one and per page; a \
            `Serialized` (raw-byte) reply mounts one-by-one only"
)]
pub trait ReplyFormFor<Fam> {
    /// The sealed mount token.
    type Form;
    /// The sealed slot-carrying mount token.
    type SlotForm;
    /// The attribute path's builder-producing form.
    type DeclaredForm;
    /// The attribute path's builder-producing form with slots.
    type DeclaredSlotForm;
}

impl ReplyFormFor<OneByOne> for EncodedReply {
    type Form = SealedPublishing;
    type SlotForm = SealedPublishingOut;
    type DeclaredForm = forms::Publishing;
    type DeclaredSlotForm = forms::PublishingOut;
}

impl ReplyFormFor<Paged> for EncodedReply {
    type Form = SealedBatchPublishing;
    type SlotForm = SealedBatchPublishingOut;
    type DeclaredForm = forms::BatchPublishing;
    type DeclaredSlotForm = forms::BatchPublishingOut;
}

impl ReplyFormFor<OneByOne> for SerializedReply {
    type Form = SealedRawReply;
    type SlotForm = SealedRawReplyOut;
    type DeclaredForm = forms::RawReply;
    type DeclaredSlotForm = forms::RawReplyOut;
}

/// The route of one reply type on one verdict family: its wire, and the form tokens that wire
/// selects. One-by-one the reply type routes itself; per page the `Vec<Reply>` verdict routes
/// by its element. Machinery behind `include` and the reply chain; never named in user code.
#[doc(hidden)]
pub trait ReplyRoute<Fam> {
    /// The reply's wire marker.
    type Wire: ReplyFormFor<Fam>;
    /// See [`ReplyFormFor::Form`].
    type Form;
    /// See [`ReplyFormFor::SlotForm`].
    type SlotForm;
    /// See [`ReplyFormFor::DeclaredForm`].
    type DeclaredForm;
    /// See [`ReplyFormFor::DeclaredSlotForm`].
    type DeclaredSlotForm;
}

impl<R> ReplyRoute<OneByOne> for R
where
    R: ReplyShape,
    R::Wire: ReplyFormFor<OneByOne>,
{
    type Wire = R::Wire;
    type Form = <R::Wire as ReplyFormFor<OneByOne>>::Form;
    type SlotForm = <R::Wire as ReplyFormFor<OneByOne>>::SlotForm;
    type DeclaredForm = <R::Wire as ReplyFormFor<OneByOne>>::DeclaredForm;
    type DeclaredSlotForm = <R::Wire as ReplyFormFor<OneByOne>>::DeclaredSlotForm;
}

impl<R> ReplyRoute<Paged> for Vec<R>
where
    R: ReplyShape,
    R::Wire: ReplyFormFor<Paged>,
{
    type Wire = R::Wire;
    type Form = <R::Wire as ReplyFormFor<Paged>>::Form;
    type SlotForm = <R::Wire as ReplyFormFor<Paged>>::SlotForm;
    type DeclaredForm = <R::Wire as ReplyFormFor<Paged>>::DeclaredForm;
    type DeclaredSlotForm = <R::Wire as ReplyFormFor<Paged>>::DeclaredSlotForm;
}

/// The mount token of a sealed single-message reply definition.
#[derive(Debug, Clone, Copy)]
pub struct SealedPublishing;

/// The mount token of a sealed serialized-reply definition.
#[derive(Debug, Clone, Copy)]
pub struct SealedRawReply;

/// The mount token of a sealed page reply definition.
#[derive(Debug, Clone, Copy)]
pub struct SealedBatchPublishing;

impl<A, R, C, H, Doc, Dest, Attach> IncludeDef
    for Sealed<ReplyValue<HandleValue<A, R, (), C, H, Doc>, Dest, Attach>>
where
    A: Axis,
    R: ReplyRoute<A::Family>,
{
    type Form = R::Form;
}

// ------------------------------------------------------------------------- the solo reply def

impl<A, R, C, H, Doc, Dest, Attach> PublishingDef
    for Sealed<ReplyValue<HandleValue<A, R, (), C, H, Doc>, Dest, Attach>>
where
    A: SoloAxis,
    R: ReplyShape<Wire: WireDocs<R, Doc>>,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    Dest: ReplyDest<R>,
    Attach: Send + Sync,
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

impl<T, R, C, S, H, Doc, Dest, Attach> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<Solo<T>, R, (), C, H, Doc>, Dest, Attach>>
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

impl<F, R, C, S, H, Doc, Dest, Attach> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<SoloDeserialized<F>, R, (), C, H, Doc>, Dest, Attach>>
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

impl<Hd, P, R, C, S, H, Doc, Dest, Attach> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<SoloPair<Hd, P>, R, (), C, H, Doc>, Dest, Attach>>
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

// ------------------------------------------------------------------------- the page reply def

impl<A, R, C, H, Doc, Dest, Attach> BatchPublishingDef
    for Sealed<ReplyValue<HandleValue<A, Vec<R>, (), C, H, Doc>, Dest, Attach>>
where
    A: PagedAxis,
    R: ReplyShape<Wire: WireDocs<R, Doc>>,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    Dest: ReplyDest<R>,
    Attach: Send + Sync,
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

/// Applies the page reply contract: one reply per element, or one outcome per element.
pub(super) fn page_reply_verdict<R>(
    verdict: Result<Vec<R>, Vec<HandlerOutcome>>,
    page_len: usize,
    subscription: &str,
) -> Result<Vec<R>, BatchResult> {
    match verdict {
        Ok(replies) => {
            assert!(
                replies.len() == page_len,
                "subscriber '{subscription}' returned {} replies for a page of {page_len}",
                replies.len(),
            );
            Ok(replies)
        }
        Err(outcomes) => {
            assert!(
                outcomes.len() == page_len,
                "subscriber '{subscription}' returned {} per-element outcomes for a page of \
                 {page_len}",
                outcomes.len(),
            );
            Err(BatchResult::PerElement(outcomes))
        }
    }
}

impl<T, R, C, S, H, Doc, Dest, Attach> BatchPublishingCall<S>
    for Sealed<ReplyValue<HandleValue<Page<T>, Vec<R>, (), C, H, Doc>, Dest, Attach>>
where
    Self: BatchPublishingDef<Input = <Page<T> as Axis>::Kind, Injections = (), Context = C, Reply = R>,
    [T]: Input<Axis = Page<T>>,
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
        page_reply_verdict(verdict, batch.len(), ctx.name())
    }
}

impl<Hd, P, R, C, S, H, Doc, Dest, Attach> BatchPublishingCall<S>
    for Sealed<ReplyValue<HandleValue<PagePair<Hd, P>, Vec<R>, (), C, H, Doc>, Dest, Attach>>
where
    Self: BatchPublishingDef<
            Input = <PagePair<Hd, P> as Axis>::Kind,
            Injections = (),
            Context = C,
            Reply = R,
        >,
    [Message<Hd, P>]: Input<Axis = PagePair<Hd, P>>,
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
        page_reply_verdict(verdict, batch.len(), ctx.name())
    }
}

// --------------------------------------------------------------------------- the mount seams

/// Splits a sealed reply chain into the definition the publishing machinery drives and the
/// attach it commits with. Machinery; never named in user code.
#[doc(hidden)]
pub trait SplitAttach: Sized {
    /// The definition without its attach.
    type Rest;
    /// The chain-attached reply commit (a policy, or a default marker).
    type Attach;

    fn split_attach(self) -> (Self::Rest, Self::Attach);
}

impl<V, Dest, Attach, Src, St, DC> SplitAttach
    for SubscriberBuilder<Sealed<ReplyValue<V, Dest, Attach>>, Src, St, DC>
{
    type Rest = SubscriberBuilder<Sealed<ReplyValue<V, Dest, ()>>, Src, St, DC>;
    type Attach = Attach;

    fn split_attach(self) -> (Self::Rest, Attach) {
        self.split_def(|Sealed(def)| {
            let ReplyValue {
                value,
                dest,
                attach,
            } = def;
            (
                Sealed(ReplyValue {
                    value,
                    dest,
                    attach: (),
                }),
                attach,
            )
        })
    }
}

/// Implements the router mount of one sealed reply token: split the attach off and commit it
/// through the same machinery a post-include `.publisher(..)` resolves.
macro_rules! sealed_reply_router_mount {
    ($($token:ty => $mount:ty),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
            for $token
        where
            B: Broker + 'static,
            Def: SplitAttach,
            Def::Attach: RouterCommit<$mount, B, Routes, RouteCodec, RouteLayers, Def::Rest>,
        {
            type Out = <Def::Attach as RouterCommit<
                $mount,
                B,
                Routes,
                RouteCodec,
                RouteLayers,
                Def::Rest,
            >>::Out;

            fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
                let (rest, attach) = def.split_attach();
                attach.commit(rest, router)
            }
        }
    )+};
}

sealed_reply_router_mount! {
    SealedPublishing => PublishMount,
    SealedRawReply => RawReplyMount,
    SealedBatchPublishing => BatchPublishMount,
}
