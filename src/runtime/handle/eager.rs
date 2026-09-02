//! The plain cells of the matrix: a sealed definition with no reply and no injections mounts
//! through the ordinary subscriber and batch machinery, with the body adapted to the dispatch
//! contracts here.

use std::marker::PhantomData;

use crate::runtime::batch::{BatchDef, BatchResult, RawSliceHandler, SliceHandler};
use crate::runtime::context::Context;
use crate::runtime::handler::{Handler, HandlerOutcome};
use crate::runtime::router::IncludeDef;
use crate::runtime::subscriber_def::SubscriberDef;
use crate::{Name, Unnamed};

use super::Handle;
use super::axis::{
    Axis, AxisDocs, Input, Message, Page, PagePair, PagedAxis, Payload, Solo, SoloAxis, SoloBytes,
    SoloPair,
};
use super::value::{HandleValue, Sealed};

/// The dispatch adapter of a single-delivery body: awaits the verdict and settles by it.
///
/// The state axis is not part of the type: the [`Handler`] impls quantify over it, so a body
/// whose `Handle` impl is generic over the state mounts on an app with any state, and one
/// naming a concrete state mounts only there.
pub struct SoloBody<A, C, H> {
    body: H,
    _axes: SoloAxes<A, C>,
}

/// The phantom carrying a solo adapter's axes.
type SoloAxes<A, C> = PhantomData<fn() -> (A, C)>;

impl<A, C, H> std::fmt::Debug for SoloBody<A, C, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SoloBody").finish_non_exhaustive()
    }
}

/// Settles by one solo verdict: `Ok` acks, `Err` is the outcome.
pub(super) fn settle_solo(verdict: Result<(), HandlerOutcome>) -> HandlerOutcome {
    match verdict {
        Ok(()) => HandlerOutcome::ack(),
        Err(outcome) => outcome,
    }
}

impl<T, C, S, H> Handler<T, C, S> for SoloBody<Solo<T>, C, H>
where
    T: Input<Axis = Solo<T>> + Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<T, (), (), C, S>,
{
    async fn handle(&self, msg: &T, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        settle_solo(self.body.handle(msg, &(), ctx).await)
    }
}

impl<C, S, H> Handler<[u8], C, S> for SoloBody<SoloBytes, C, H>
where
    C: Send + Sync,
    S: Send + Sync,
    H: for<'p> Handle<Payload<'p>, (), (), C, S>,
{
    async fn handle(&self, msg: &[u8], ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        let payload = Payload::new(msg);
        settle_solo(self.body.handle(&payload, &(), ctx).await)
    }
}

impl<Hd, P, C, S, H> Handler<Message<Hd, P>, C, S> for SoloBody<SoloPair<Hd, P>, C, H>
where
    Message<Hd, P>: Input<Axis = SoloPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<Message<Hd, P>, (), (), C, S>,
{
    async fn handle(&self, msg: &Message<Hd, P>, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        settle_solo(self.body.handle(msg, &(), ctx).await)
    }
}

impl<A, R, C, H, Doc> IncludeDef for Sealed<HandleValue<A, R, (), C, H, Doc>>
where
    A: Axis,
{
    type Form = A::EagerForm;
}

impl<A, C, H, Doc> SubscriberDef for Sealed<HandleValue<A, (), (), C, H, Doc>>
where
    A: SoloAxis,
    Doc: AxisDocs<A>,
{
    type Input = A::Kind;
    type Context = C;
    type Handler = SoloBody<A, C, H>;
    // The sealed value never builds a source: the settings builder wrapping it carries the real
    // one, and this placeholder is no `SubscriptionSource` at all, so a bare mount is a compile
    // error.
    type Source = Unnamed<Name>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.0.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.docs.message_description
    }

    fn into_handler(self) -> SoloBody<A, C, H> {
        SoloBody {
            body: self.0.body,
            _axes: PhantomData,
        }
    }
}

/// The dispatch adapter of a page body.
///
/// Awaits the verdict, checks the per-element contract, and settles the page by it. Carries
/// the [`batch`](crate::runtime::SubscriberBuilder::batch) cap, feeding an oversized page to
/// the body in chunks.
pub struct PageBody<A, H> {
    body: H,
    cap: Option<std::num::NonZeroUsize>,
    _axes: PhantomData<fn() -> A>,
}

impl<A, H> std::fmt::Debug for PageBody<A, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageBody").finish_non_exhaustive()
    }
}

/// Applies the page contract to one verdict: `Ok` acks the chunk, an `Err` vector must be
/// exactly chunk-length (a mismatch is a bug in the handler and panics under the subscriber's
/// panic policy).
pub(super) fn settle_page(
    verdict: Result<(), Vec<HandlerOutcome>>,
    chunk_len: usize,
    subscription: &str,
) -> BatchResult {
    match verdict {
        Ok(()) => BatchResult::Uniform(HandlerOutcome::ack()),
        Err(outcomes) => {
            assert!(
                outcomes.len() == chunk_len,
                "subscriber '{subscription}' returned {} per-element outcomes for a page of {}",
                outcomes.len(),
                chunk_len,
            );
            BatchResult::PerElement(outcomes)
        }
    }
}

/// Extends `settles` with one chunk's outcomes, one per element.
///
/// A uniform chunk outcome fans its status out per element; its one continuation rides the
/// chunk's last element, so it still runs after the whole chunk is settled.
fn extend_settles(settles: &mut Vec<HandlerOutcome>, outcome: BatchResult, chunk_len: usize) {
    match outcome {
        BatchResult::Uniform(uniform) => {
            if chunk_len == 0 {
                return;
            }
            let status = uniform.outcome();
            settles.extend(
                std::iter::repeat_with(|| HandlerOutcome::from(status)).take(chunk_len - 1),
            );
            settles.push(uniform);
        }
        BatchResult::PerElement(chunk) => settles.extend(chunk),
    }
}

impl<T, S, H> SliceHandler<T, S> for PageBody<Page<T>, H>
where
    [T]: Input<Axis = Page<T>>,
    T: Send + Sync + 'static,
    S: Send + Sync,
    H: Handle<[T], (), (), (), S>,
{
    async fn handle_slice(&self, batch: &[T], ctx: &mut Context<'_, (), S>) -> BatchResult {
        match self.cap {
            None => {
                let verdict = self.body.handle(batch, &(), ctx).await;
                settle_page(verdict, batch.len(), ctx.name())
            }
            Some(max) => {
                let mut settles = Vec::with_capacity(batch.len());
                for chunk in batch.chunks(max.get()) {
                    let verdict = self.body.handle(chunk, &(), ctx).await;
                    let outcome = settle_page(verdict, chunk.len(), ctx.name());
                    extend_settles(&mut settles, outcome, chunk.len());
                }
                BatchResult::PerElement(settles)
            }
        }
    }
}

impl<Hd, P, S, H> SliceHandler<Message<Hd, P>, S> for PageBody<PagePair<Hd, P>, H>
where
    [Message<Hd, P>]: Input<Axis = PagePair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: Send + Sync,
    H: Handle<[Message<Hd, P>], (), (), (), S>,
{
    async fn handle_slice(
        &self,
        batch: &[Message<Hd, P>],
        ctx: &mut Context<'_, (), S>,
    ) -> BatchResult {
        match self.cap {
            None => {
                let verdict = self.body.handle(batch, &(), ctx).await;
                settle_page(verdict, batch.len(), ctx.name())
            }
            Some(max) => {
                let mut settles = Vec::with_capacity(batch.len());
                for chunk in batch.chunks(max.get()) {
                    let verdict = self.body.handle(chunk, &(), ctx).await;
                    let outcome = settle_page(verdict, chunk.len(), ctx.name());
                    extend_settles(&mut settles, outcome, chunk.len());
                }
                BatchResult::PerElement(settles)
            }
        }
    }
}

impl<S, H> RawSliceHandler<S> for PageBody<super::axis::PageBytes, H>
where
    S: Send + Sync,
    H: for<'p> Handle<[Payload<'p>], (), (), (), S>,
{
    async fn handle_slice(
        &self,
        batch: &[Payload<'_>],
        ctx: &mut Context<'_, (), S>,
    ) -> BatchResult {
        // The dispatcher's page already carries the payload views, so nothing is rebuilt here
        // and the payload bytes stay in the broker's buffers.
        match self.cap {
            None => {
                let verdict = self.body.handle(batch, &(), ctx).await;
                settle_page(verdict, batch.len(), ctx.name())
            }
            Some(max) => {
                let mut settles = Vec::with_capacity(batch.len());
                for chunk in batch.chunks(max.get()) {
                    let verdict = self.body.handle(chunk, &(), ctx).await;
                    let outcome = settle_page(verdict, chunk.len(), ctx.name());
                    extend_settles(&mut settles, outcome, chunk.len());
                }
                BatchResult::PerElement(settles)
            }
        }
    }
}

impl<A, H, Doc> BatchDef for Sealed<HandleValue<A, (), (), (), H, Doc>>
where
    A: PagedAxis,
    Doc: AxisDocs<A>,
{
    type Input = A::Kind;
    type Handler = PageBody<A, H>;
    // See `SubscriberDef::Source` above: the builder carries the real source.
    type Source = Unnamed<Name>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.0.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.docs.message_description
    }

    fn into_handler(self) -> PageBody<A, H> {
        PageBody {
            body: self.0.body,
            cap: self.0.page_cap,
            _axes: PhantomData,
        }
    }
}
