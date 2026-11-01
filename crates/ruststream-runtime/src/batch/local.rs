//! The batch handler of the local family: what a runner whose deliveries stay on one thread
//! hands a batch to.

// Written for a runner that polls on one thread: the futures are `Send` exactly where the types
// they run are, and nothing here spawns them.
#![allow(clippy::future_not_send)]

use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::context::Context;
use crate::handle::Deserialized;
use crate::host::{HostDetach, PrepareKept, SendHost};
use crate::redelivery::RetryCopies;
use crate::{BuildBatchContext, LocalIncomingMessage};

use super::{BatchHandler, DeserializedBatch, LocalSliceHandler, TakenBatch};
use crate::handler::LocalCall;

/// The dispatch-side consumer of one batch, for a runner whose deliveries stay on the thread
/// that polls them: the [`BatchHandler`] of the local family, whose batch and future need not be
/// `Send`.
///
/// An `Arc` of every [`BatchHandler`] is one through a forwarding impl, so a runner that spawns
/// onto a multi-thread runtime shares its handler in an `Arc` and still proves the future it
/// spawns `Send`.
#[doc(hidden)]
pub trait LocalBatchHandler<Batch, Host, C = (), S = ()> {
    /// The buffer the decoded batch is built in. See [`BatchHandler::Scratch`].
    type Scratch: Default;

    /// The batch as this handler takes it. See [`BatchHandler::Taken`].
    type Taken: TakenBatch<C>;

    /// Takes one batch off the subscription.
    fn take(batch: Batch) -> Self::Taken;

    /// Consumes one batch, handing what its deliveries leave behind to `host`. See
    /// [`BatchHandler::handle_batch`].
    fn handle_batch(
        &self,
        batch: Self::Taken,
        scratch: &mut Self::Scratch,
        ctx: &mut Context<'_, C, S>,
        host: &Host,
    ) -> impl Future<Output = ()>;
}

// A forwarding call: the future is the `BatchHandler`'s own, so a runner that knows its handler
// is a `BatchHandler` proves the body's future `Send` through this impl.
impl<Batch, Host, C, S, H> LocalBatchHandler<Batch, Host, C, S> for Arc<H>
where
    H: BatchHandler<Batch, C, S>,
    Host: SendHost<C>,
{
    type Scratch = H::Scratch;
    type Taken = H::Taken;

    #[inline]
    fn take(batch: Batch) -> H::Taken {
        H::take(batch)
    }

    #[inline]
    fn handle_batch(
        &self,
        batch: H::Taken,
        scratch: &mut H::Scratch,
        ctx: &mut Context<'_, C, S>,
        host: &Host,
    ) -> impl Future<Output = ()> {
        <H as BatchHandler<Batch, C, S>>::handle_batch(self, batch, scratch, ctx, host)
    }
}

/// A batch adapter for a runner whose deliveries stay on one thread: the same body, reached
/// through [`LocalBatchHandler`], so the deliveries need not be `Send` or `Sync`.
#[doc(hidden)]
#[derive(Debug)]
pub struct LocalBatch<Adapter>(pub(crate) Adapter);

impl<Batch, Host, M, F, Inner, C, S> LocalBatchHandler<Batch, Host, C, S>
    for LocalBatch<DeserializedBatch<M, F, Inner>>
where
    Host: HostDetach<Kept: PrepareKept<C>, Retry: RetryCopies<C>>,
    Batch: IntoIterator<Item = M>,
    M: LocalIncomingMessage,
    F: Deserialized,
    Inner: for<'p> LocalSliceHandler<F::Output<'p>, C, S>,
    C: BuildBatchContext<M> + Send + Sync + 'static,
{
    // See the `BatchHandler` impl: the values borrow the batch, so no buffer outlives it.
    type Scratch = ();
    type Taken = Vec<M>;

    fn take(batch: Batch) -> Vec<M> {
        batch.into_iter().collect()
    }

    fn handle_batch(
        &self,
        batch: Vec<M>,
        scratch: &mut (),
        ctx: &mut Context<'_, C, S>,
        host: &Host,
    ) -> impl Future<Output = ()> {
        self.0
            .handle_local::<LocalCall, C, S, Host>(batch, scratch, ctx, host)
    }
}
