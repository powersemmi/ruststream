//! The reply cells of the matrix: a sealed definition whose body declared a reply mounts
//! through the publishing machinery, with the policy the chain attached (or the broker's
//! default) committed right at `include`.

use std::any::type_name;

use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingDef};
use crate::runtime::context::Context;
use crate::runtime::handler::HandlerResult;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publishing::{PublishingCall, PublishingDef};
use crate::runtime::router::{
    BatchPublishMount, IncludeDef, PublishMount, RawReplyMount, Router, RouterCommit, RouterMount,
};
use crate::runtime::settings::SubscriberBuilder;
use crate::{Broker, FixedName, Name, OutgoingDestination, Unnamed};

use super::axis::{
    Axis, AxisDocs, Input, Message, Page, PagePair, PagedAxis, Payload, Solo, SoloAxis, SoloBytes,
    SoloPair,
};
use super::docs::DocState;
use super::value::{
    BareReply, DeclaredDest, EncodedReply, HandleValue, NamedDest, ReplyValue, Sealed,
};
use super::verdict::{OneByOne, Paged};
use super::{Handle, IntoVerdict};

// ------------------------------------------------------------------------------ reply shapes

/// The two shapes a reply value takes: a bare payload, or a [`Message`] pair whose headers ride
/// the reply. Machinery behind the reply metadata; never named in user code.
#[doc(hidden)]
pub trait ReplyShape: Send + Sync {
    /// The published payload type (the pair's body, or the reply itself).
    type Body: Send + Sync;
    /// The typed header contract riding the reply (`()` when none does).
    type Headers;
}

impl<R: serde::Serialize + Send + Sync> ReplyShape for R {
    type Body = R;
    type Headers = ();
}

impl<H, P> ReplyShape for Message<H, P>
where
    H: serde::Serialize + Send + Sync,
    P: serde::Serialize + Send + Sync,
{
    type Body = P;
    type Headers = H;
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

/// The reply-form token of one route on one verdict family. A bare reply has no page form: the
/// attribute admits none either.
#[doc(hidden)]
pub trait ReplyFormFor<Fam> {
    /// The sealed mount token.
    type Form;
}

impl ReplyFormFor<OneByOne> for EncodedReply {
    type Form = SealedPublishing;
}

impl ReplyFormFor<Paged> for EncodedReply {
    type Form = SealedBatchPublishing;
}

impl ReplyFormFor<OneByOne> for BareReply {
    type Form = SealedRawReply;
}

/// The mount token of a sealed single-message reply definition.
#[derive(Debug, Clone, Copy)]
pub struct SealedPublishing;

/// The mount token of a sealed bare-byte reply definition.
#[derive(Debug, Clone, Copy)]
pub struct SealedRawReply;

/// The mount token of a sealed page reply definition.
#[derive(Debug, Clone, Copy)]
pub struct SealedBatchPublishing;

impl<A, R, C, S, H, Doc, Dest, Route, Attach> IncludeDef
    for Sealed<ReplyValue<HandleValue<A, R, (), C, S, H, Doc>, Dest, Route, Attach>>
where
    A: Axis,
    Route: ReplyFormFor<A::Family>,
{
    type Form = Route::Form;
}

// ------------------------------------------------------------------------- the solo reply def

impl<A, R, C, S, H, Doc, Dest, Route, Attach> PublishingDef
    for Sealed<ReplyValue<HandleValue<A, R, (), C, S, H, Doc>, Dest, Route, Attach>>
where
    A: SoloAxis,
    R: ReplyShape + ReplyHeadersSchema<Doc>,
    C: Send + Sync,
    S: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + DocState<R::Body> + Send + Sync,
    Dest: ReplyDest<R>,
    Route: Send + Sync,
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
        Doc::payload_schema()
    }

    fn headers_schema(&self) -> Option<String> {
        Doc::headers_schema()
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R::Body>())
                .with_payload_schema(<Doc as DocState<R::Body>>::schema())
                .with_headers_schema(<R as ReplyHeadersSchema<Doc>>::headers_schema()),
        ]
    }
}

/// Downgrades the settle side of one solo reply verdict: a reply verdict is only constructible
/// from `Result<R, HandlerResult>`, so no continuation is lost here.
pub(super) fn solo_verdict<R>(
    verdict: Result<R, crate::runtime::Settle>,
) -> Result<R, HandlerResult> {
    verdict.map_err(|settle| settle.outcome())
}

impl<T, R, C, S, H, Doc, Dest, Route, Attach> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<Solo<T>, R, (), C, S, H, Doc>, Dest, Route, Attach>>
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
    ) -> Result<R, HandlerResult> {
        solo_verdict(
            self.0
                .value
                .body
                .handle(input, &(), ctx)
                .await
                .into_verdict(),
        )
    }
}

impl<R, C, S, H, Doc, Dest, Route, Attach> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<SoloBytes, R, (), C, S, H, Doc>, Dest, Route, Attach>>
where
    Self: PublishingDef<Input = crate::runtime::RawBytes, Injections = (), Reply = R, Context = C>,
    R: ReplyShape,
    C: Send + Sync,
    S: Send + Sync,
    H: for<'p> Handle<Payload<'p>, R, (), C, S>,
{
    async fn call(
        &self,
        input: &[u8],
        _injections: &(),
        ctx: &mut Context<'_, C, S>,
    ) -> Result<R, HandlerResult> {
        let payload = Payload::new(input);
        solo_verdict(
            self.0
                .value
                .body
                .handle(&payload, &(), ctx)
                .await
                .into_verdict(),
        )
    }
}

impl<Hd, P, R, C, S, H, Doc, Dest, Route, Attach> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<SoloPair<Hd, P>, R, (), C, S, H, Doc>, Dest, Route, Attach>>
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
    ) -> Result<R, HandlerResult> {
        solo_verdict(
            self.0
                .value
                .body
                .handle(input, &(), ctx)
                .await
                .into_verdict(),
        )
    }
}

// ------------------------------------------------------------------------- the page reply def

impl<A, R, S, H, Doc, Dest, Route, Attach> BatchPublishingDef
    for Sealed<ReplyValue<HandleValue<A, R, (), (), S, H, Doc>, Dest, Route, Attach>>
where
    A: PagedAxis,
    R: ReplyShape + ReplyHeadersSchema<Doc>,
    S: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + DocState<R::Body> + Send + Sync,
    Dest: ReplyDest<R>,
    Route: Send + Sync,
    Attach: Send + Sync,
{
    type Input = A::Kind;
    type Injections = ();
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
        Doc::payload_schema()
    }

    fn headers_schema(&self) -> Option<String> {
        Doc::headers_schema()
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R::Body>())
                .with_payload_schema(<Doc as DocState<R::Body>>::schema())
                .with_headers_schema(<R as ReplyHeadersSchema<Doc>>::headers_schema()),
        ]
    }
}

/// Applies the page reply contract: one reply per element, one outcome per element.
pub(super) fn page_reply_verdict<R>(
    verdict: Result<Vec<R>, crate::runtime::BatchResult>,
    page_len: usize,
    subscription: &str,
) -> Result<Vec<R>, crate::runtime::BatchResult> {
    match &verdict {
        Ok(replies) => {
            assert!(
                replies.len() == page_len,
                "subscriber '{subscription}' returned {} replies for a page of {page_len}",
                replies.len(),
            );
        }
        Err(crate::runtime::BatchResult::PerElement(settles)) => {
            assert!(
                settles.len() == page_len,
                "subscriber '{subscription}' returned {} per-element outcomes for a page of \
                 {page_len}",
                settles.len(),
            );
        }
        Err(crate::runtime::BatchResult::Uniform(_)) => {}
    }
    verdict
}

impl<T, R, S, H, Doc, Dest, Route, Attach> BatchPublishingCall<S>
    for Sealed<ReplyValue<HandleValue<Page<T>, R, (), (), S, H, Doc>, Dest, Route, Attach>>
where
    Self: BatchPublishingDef<Input = <Page<T> as Axis>::Kind, Injections = (), Reply = R>,
    [T]: Input<Axis = Page<T>>,
    T: Send + Sync + 'static,
    R: ReplyShape,
    S: Send + Sync,
    H: Handle<[T], R, (), (), S>,
{
    async fn call(
        &self,
        batch: &[T],
        _injections: &(),
        ctx: &mut Context<'_, (), S>,
    ) -> Result<Vec<R>, crate::runtime::BatchResult> {
        let verdict = self
            .0
            .value
            .body
            .handle(batch, &(), ctx)
            .await
            .into_verdict();
        page_reply_verdict(verdict, batch.len(), ctx.name())
    }
}

impl<Hd, P, R, S, H, Doc, Dest, Route, Attach> BatchPublishingCall<S>
    for Sealed<ReplyValue<HandleValue<PagePair<Hd, P>, R, (), (), S, H, Doc>, Dest, Route, Attach>>
where
    Self: BatchPublishingDef<Input = <PagePair<Hd, P> as Axis>::Kind, Injections = (), Reply = R>,
    [Message<Hd, P>]: Input<Axis = PagePair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    R: ReplyShape,
    S: Send + Sync,
    H: Handle<[Message<Hd, P>], R, (), (), S>,
{
    async fn call(
        &self,
        batch: &[Message<Hd, P>],
        _injections: &(),
        ctx: &mut Context<'_, (), S>,
    ) -> Result<Vec<R>, crate::runtime::BatchResult> {
        let verdict = self
            .0
            .value
            .body
            .handle(batch, &(), ctx)
            .await
            .into_verdict();
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

impl<V, Dest, Route, Attach, Src, St, DC> SplitAttach
    for SubscriberBuilder<Sealed<ReplyValue<V, Dest, Route, Attach>>, Src, St, DC>
{
    type Rest = SubscriberBuilder<Sealed<ReplyValue<V, Dest, Route, ()>>, Src, St, DC>;
    type Attach = Attach;

    fn split_attach(self) -> (Self::Rest, Attach) {
        self.split_def(|Sealed(def)| {
            let ReplyValue {
                value,
                dest,
                attach,
                _route: route,
            } = def;
            (
                Sealed(ReplyValue {
                    value,
                    dest,
                    attach: (),
                    _route: route,
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
