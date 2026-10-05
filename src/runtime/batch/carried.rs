//! The batch adapter of the carried lane: the broker's batch lends its values as one slice, and
//! the handler reads that slice where it lies.
//!
//! The batch stays whole while the body runs, because the slice is borrowed from it, so its
//! context is built from the batch itself and its deliveries are taken out of it only after the
//! body returned. The value at index `i` belongs to the `i`-th delivery; a delivery past the end
//! of the slice carries no value and settles by the decode-failure policy.

use std::any::type_name;
use std::fmt;
use std::marker::PhantomData;
#[cfg(feature = "testing")]
use std::sync::Arc;

use tracing::{error, warn};

use crate::{
    BatchSubscriber, BuildBatchContext, CarriesBatch, ConnectedBroker, IncomingMessage,
    SubscriptionSource,
};

use super::super::context::Context;
use super::super::dispatch::{Slot, settle_outcome};
use super::super::failure::FailurePolicy;
#[cfg(feature = "otel")]
use super::record_batch_size;
use super::{BatchHandler, LentValues, SliceHandler, TakenBatch, rejection, settle_lent_batch};

/// A subscription whose batches lend a slice of `T`: what a handler taking `&[T]` on the carried
/// lane asks of the subscription it mounts on.
///
/// It names the subscription rather than its batch type, so mounting such a handler on a
/// subscription whose batches lend nothing is an error about the subscription the mount site
/// wrote.
#[diagnostic::on_unimplemented(
    message = "the subscription `{Self}` does not lend batches of `{T}`",
    label = "a `&[{T}]` handler reads the values this subscription's batches hold",
    note = "mount it on a subscription whose batches implement `CarriesBatch<{T}>`"
)]
pub trait SourceCarriesBatch<T, Conn> {
    /// The subscription's batch type.
    type Batch;

    /// The values one batch lends.
    fn lend(batch: &Self::Batch) -> &[T];
}

// As for the single lane: the batch type's own `CarriesBatch` is the machinery of the check, and
// the trait's message names the subscription and the type.
#[diagnostic::do_not_recommend]
impl<T, Conn, Src> SourceCarriesBatch<T, Conn> for Src
where
    Conn: ConnectedBroker,
    Src: SubscriptionSource<Conn>,
    Src::Subscriber: BatchSubscriber,
    <Src::Subscriber as BatchSubscriber>::Batch: CarriesBatch<T>,
{
    type Batch = <Src::Subscriber as BatchSubscriber>::Batch;

    fn lend(batch: &Self::Batch) -> &[T] {
        batch.carried()
    }
}

/// The lane a carried batch rides: the subscription, the connected broker and the value type.
type Lane<Src, Conn, T> = PhantomData<fn() -> (Src, Conn, T)>;

/// The batch as the carried adapter takes it: whole, because its values are lent from it while
/// the body runs.
pub(crate) struct Lent<Src, Conn, Batch, T> {
    batch: Batch,
    _lane: Lane<Src, Conn, T>,
}

impl<Src, Conn, Batch, T, C> TakenBatch<C> for Lent<Src, Conn, Batch, T>
where
    Src: SourceCarriesBatch<T, Conn, Batch = Batch>,
    Batch: Send,
    T: Clone + Send + Sync + 'static,
    C: BuildBatchContext<Batch>,
{
    fn context(&self) -> Option<C> {
        Some(C::build(&self.batch))
    }

    // The deliveries are unread until the body returns, so a panic leaves the lent values to
    // stand for them.
    #[cfg(feature = "testing")]
    fn unsettled(&self) -> Vec<crate::testing::coordinator::Delivered> {
        Src::lend(&self.batch)
            .iter()
            .map(|value| crate::testing::coordinator::Delivered {
                raw: bytes::Bytes::new(),
                value: Some(Arc::new(value.clone())),
                settle: None,
            })
            .collect()
    }
}

/// The batch adapter of the carried lane, over the subscription `Src` of the connected broker
/// `Conn`: no codec runs and nothing is copied; the body reads the slice the broker's batch
/// holds.
pub struct CarriedBatch<Src, Conn, T, Inner> {
    inner: Inner,
    decode: FailurePolicy,
    _lane: Lane<Src, Conn, T>,
}

impl<Src, Conn, T, Inner> CarriedBatch<Src, Conn, T, Inner> {
    /// Builds the adapter over the batch handler, like
    /// [`DeserializedBatch::over`](super::DeserializedBatch::over).
    #[must_use]
    pub(crate) fn over(inner: Inner) -> Self {
        Self {
            inner,
            decode: FailurePolicy::Drop,
            _lane: PhantomData,
        }
    }

    /// Sets the policy applied to a delivery that carries no value.
    #[must_use]
    pub(crate) fn with_decode(mut self, decode: FailurePolicy) -> Self {
        self.decode = decode;
        self
    }
}

impl<Src, Conn, T, Inner> fmt::Debug for CarriedBatch<Src, Conn, T, Inner> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CarriedBatch")
            .field("decode", &self.decode)
            .finish_non_exhaustive()
    }
}

impl<Src, Conn, Batch, M, T, Inner, C, S> BatchHandler<Batch, C, S>
    for CarriedBatch<Src, Conn, T, Inner>
where
    Src: SourceCarriesBatch<T, Conn, Batch = Batch>,
    Batch: IntoIterator<Item = M> + Send,
    M: IncomingMessage,
    T: Clone + Send + Sync + 'static,
    Inner: SliceHandler<T, C, S>,
    C: BuildBatchContext<Batch> + BuildBatchContext<M> + Send + Sync + 'static,
    S: Send + Sync,
{
    type Scratch = ();
    type Taken = Lent<Src, Conn, Batch, T>;

    fn take(batch: Batch) -> Self::Taken {
        Lent {
            batch,
            _lane: PhantomData,
        }
    }

    async fn handle_batch(
        &self,
        taken: Self::Taken,
        _scratch: &mut (),
        ctx: &mut Context<'_, C, S>,
    ) {
        let subscription = ctx.subscription();
        let delivery = ctx.delivery();
        let Lent { batch, .. } = taken;
        let values = Src::lend(&batch);
        let lent = values.len();
        #[cfg(feature = "testing")]
        let recorded: LentValues = if delivery.hooks.coordinator().is_some() {
            values
                .iter()
                .map(|value| Arc::new(value.clone()) as crate::testing::coordinator::RecordedValue)
                .collect()
        } else {
            LentValues::new()
        };
        #[cfg(not(feature = "testing"))]
        let recorded: LentValues = ();
        // A batch that lends nothing never reaches the body, as a batch that decodes to nothing
        // never does on the other lanes.
        let result = if lent == 0 {
            None
        } else {
            #[cfg(feature = "otel")]
            record_batch_size(subscription, lent);
            Some(self.inner.handle_slice(values, ctx).await)
        };
        let mut deliveries: Vec<M> = batch.into_iter().collect();
        if deliveries.len() < lent {
            error!(
                target: "ruststream::dispatch",
                subscription = %subscription,
                message_type = type_name::<T>(),
                lent,
                delivered = deliveries.len(),
                "the batch lent more values than it delivered; the values past its last delivery \
                 have nothing to settle",
            );
        }
        // Deliveries past the end of the slice carry no value: they settle by the decode policy,
        // the way a single delivery without one does.
        let gone = if deliveries.len() > lent {
            deliveries.split_off(lent)
        } else {
            Vec::new()
        };
        for msg in gone {
            warn!(
                target: "ruststream::dispatch",
                subscription = %subscription,
                message_type = type_name::<T>(),
                "the delivery carries no value",
            );
            let outcome = rejection(
                &format_args!("the delivery carries no `{}`", type_name::<T>()),
                "batch delivery carries no value",
                self.decode,
                ctx,
            );
            settle_outcome(
                &mut Slot::new(msg),
                outcome,
                subscription,
                delivery,
                <C as BuildBatchContext<M>>::build as fn(&M) -> C,
            )
            .await;
        }
        if let Some(result) = result {
            settle_lent_batch(deliveries, result, subscription, delivery, recorded).await;
        }
    }
}
