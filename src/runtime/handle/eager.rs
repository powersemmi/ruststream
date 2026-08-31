//! The plain cells of the matrix: a sealed definition with no reply and no injections mounts
//! through the ordinary subscriber and batch machinery, with the body adapted to the dispatch
//! contracts here.

use std::marker::PhantomData;

use crate::runtime::batch::{BatchDef, BatchResult, RawSliceHandler, SliceHandler};
use crate::runtime::context::Context;
use crate::runtime::handler::{Handler, HandlerResult, Settle};
use crate::runtime::router::IncludeDef;
use crate::runtime::subscriber_def::SubscriberDef;
use crate::{Name, Unnamed};

use super::axis::{
    Axis, AxisDocs, Input, Message, Page, PagePair, PagedAxis, Payload, Solo, SoloAxis, SoloBytes,
    SoloPair,
};
use super::value::{HandleValue, Sealed};
use super::{Handle, IntoVerdict};

/// The dispatch adapter of a single-delivery body: awaits the verdict and settles by it.
pub struct SoloBody<A, C, S, H> {
    body: H,
    _axes: PhantomData<fn() -> (A, C, S)>,
}

impl<A, C, S, H> std::fmt::Debug for SoloBody<A, C, S, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SoloBody").finish_non_exhaustive()
    }
}

impl<T, C, S, H> Handler<T, C, S> for SoloBody<Solo<T>, C, S, H>
where
    T: Input<Axis = Solo<T>> + Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<T, (), (), C, S>,
{
    async fn handle(&self, msg: &T, ctx: &mut Context<'_, C, S>) -> Settle {
        match self.body.handle(msg, &(), ctx).await.into_verdict() {
            Ok(()) => HandlerResult::Ack.into(),
            Err(settle) => settle,
        }
    }
}

impl<C, S, H> Handler<[u8], C, S> for SoloBody<SoloBytes, C, S, H>
where
    C: Send + Sync,
    S: Send + Sync,
    H: for<'p> Handle<Payload<'p>, (), (), C, S>,
{
    async fn handle(&self, msg: &[u8], ctx: &mut Context<'_, C, S>) -> Settle {
        let payload = Payload::new(msg);
        match self.body.handle(&payload, &(), ctx).await.into_verdict() {
            Ok(()) => HandlerResult::Ack.into(),
            Err(settle) => settle,
        }
    }
}

impl<Hd, P, C, S, H> Handler<Message<Hd, P>, C, S> for SoloBody<SoloPair<Hd, P>, C, S, H>
where
    Message<Hd, P>: Input<Axis = SoloPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<Message<Hd, P>, (), (), C, S>,
{
    async fn handle(&self, msg: &Message<Hd, P>, ctx: &mut Context<'_, C, S>) -> Settle {
        match self.body.handle(msg, &(), ctx).await.into_verdict() {
            Ok(()) => HandlerResult::Ack.into(),
            Err(settle) => settle,
        }
    }
}

impl<A, R, O, C, S, H, Doc> IncludeDef for Sealed<HandleValue<A, R, O, C, S, H, Doc>>
where
    A: Axis,
{
    type Form = A::EagerForm;
}

impl<A, C, S, H, Doc> SubscriberDef for Sealed<HandleValue<A, (), (), C, S, H, Doc>>
where
    A: SoloAxis,
    Doc: AxisDocs<A>,
{
    type Input = A::Kind;
    type Context = C;
    type Handler = SoloBody<A, C, S, H>;
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
        Doc::payload_schema()
    }

    fn headers_schema(&self) -> Option<String> {
        Doc::headers_schema()
    }

    fn into_handler(self) -> SoloBody<A, C, S, H> {
        SoloBody {
            body: self.0.body,
            _axes: PhantomData,
        }
    }
}

/// The dispatch adapter of a page body: awaits the verdict, checks the per-element contract,
/// and settles the page by it. Carries the [`batch`](crate::runtime::SubscriberBuilder::batch)
/// cap, feeding an oversized page to the body in chunks.
pub struct PageBody<A, S, H> {
    body: H,
    cap: Option<std::num::NonZeroUsize>,
    _axes: PhantomData<fn() -> (A, S)>,
}

impl<A, S, H> std::fmt::Debug for PageBody<A, S, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageBody").finish_non_exhaustive()
    }
}

/// Applies the page contract to one normalized verdict: `Ok` acks the chunk, a per-element
/// vector must be exactly chunk-length (a mismatch is a bug in the handler and panics under the
/// subscriber's panic policy).
fn settle_page<R>(
    verdict: Result<Vec<R>, BatchResult>,
    chunk_len: usize,
    subscription: &str,
) -> BatchResult {
    match verdict {
        Ok(_) => BatchResult::Uniform(HandlerResult::Ack),
        Err(BatchResult::PerElement(settles)) => {
            assert!(
                settles.len() == chunk_len,
                "subscriber '{subscription}' returned {} per-element outcomes for a page of {}",
                settles.len(),
                chunk_len,
            );
            BatchResult::PerElement(settles)
        }
        Err(uniform) => uniform,
    }
}

/// Extends `settles` with one chunk's outcomes, one per element.
fn extend_settles(settles: &mut Vec<Settle>, outcome: BatchResult, chunk_len: usize) {
    match outcome {
        BatchResult::Uniform(result) => {
            settles.extend(std::iter::repeat_with(|| Settle::from(result)).take(chunk_len));
        }
        BatchResult::PerElement(chunk) => settles.extend(chunk),
    }
}

impl<T, S, H> SliceHandler<T, S> for PageBody<Page<T>, S, H>
where
    [T]: Input<Axis = Page<T>>,
    T: Send + Sync + 'static,
    S: Send + Sync,
    H: Handle<[T], (), (), (), S>,
{
    async fn handle_slice(&self, batch: &[T], ctx: &mut Context<'_, (), S>) -> BatchResult {
        match self.cap {
            None => {
                let verdict = self.body.handle(batch, &(), ctx).await.into_verdict();
                settle_page(verdict, batch.len(), ctx.name())
            }
            Some(max) => {
                let mut settles = Vec::with_capacity(batch.len());
                for chunk in batch.chunks(max.get()) {
                    let verdict = self.body.handle(chunk, &(), ctx).await.into_verdict();
                    let outcome = settle_page(verdict, chunk.len(), ctx.name());
                    extend_settles(&mut settles, outcome, chunk.len());
                }
                BatchResult::PerElement(settles)
            }
        }
    }
}

impl<Hd, P, S, H> SliceHandler<Message<Hd, P>, S> for PageBody<PagePair<Hd, P>, S, H>
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
                let verdict = self.body.handle(batch, &(), ctx).await.into_verdict();
                settle_page(verdict, batch.len(), ctx.name())
            }
            Some(max) => {
                let mut settles = Vec::with_capacity(batch.len());
                for chunk in batch.chunks(max.get()) {
                    let verdict = self.body.handle(chunk, &(), ctx).await.into_verdict();
                    let outcome = settle_page(verdict, chunk.len(), ctx.name());
                    extend_settles(&mut settles, outcome, chunk.len());
                }
                BatchResult::PerElement(settles)
            }
        }
    }
}

impl<S, H> RawSliceHandler<S> for PageBody<super::axis::PageBytes, S, H>
where
    S: Send + Sync,
    H: for<'p> Handle<[Payload<'p>], (), (), (), S>,
{
    async fn handle_slice(&self, batch: &[&[u8]], ctx: &mut Context<'_, (), S>) -> BatchResult {
        // One page-sized Vec of borrowing wrappers; the payload bytes themselves stay in the
        // broker's buffers.
        let payloads: Vec<Payload<'_>> = batch.iter().map(|bytes| Payload::new(bytes)).collect();
        match self.cap {
            None => {
                let verdict = self.body.handle(&payloads, &(), ctx).await.into_verdict();
                settle_page(verdict, payloads.len(), ctx.name())
            }
            Some(max) => {
                let mut settles = Vec::with_capacity(payloads.len());
                for chunk in payloads.chunks(max.get()) {
                    let verdict = self.body.handle(chunk, &(), ctx).await.into_verdict();
                    let outcome = settle_page(verdict, chunk.len(), ctx.name());
                    extend_settles(&mut settles, outcome, chunk.len());
                }
                BatchResult::PerElement(settles)
            }
        }
    }
}

impl<A, S, H, Doc> BatchDef for Sealed<HandleValue<A, (), (), (), S, H, Doc>>
where
    A: PagedAxis,
    Doc: AxisDocs<A>,
{
    type Input = A::Kind;
    type Handler = PageBody<A, S, H>;
    // See `SubscriberDef::Source` above: the builder carries the real source.
    type Source = Unnamed<Name>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.0.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        Doc::payload_schema()
    }

    fn headers_schema(&self) -> Option<String> {
        Doc::headers_schema()
    }

    fn into_handler(self) -> PageBody<A, S, H> {
        PageBody {
            body: self.0.body,
            cap: self.0.page_cap,
            _axes: PhantomData,
        }
    }
}
