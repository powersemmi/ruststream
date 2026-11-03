//! The plan of a batch registration on the embedded runner: one worker, and the store its elements'
//! retry copies wait in.
//!
//! A batch mount takes the steps of a single-message one that apply to it: `.retry_capacity::<CAP>()`
//! and `.encode_capacity::<N>()`. Its batches run one at a time, so `pool` and `keyed` have no plan
//! to ask for. A store holds a record for each element a batch retries at once, so the capacity of a
//! batch mount is at least the batch's size, and a smaller one does not compile.

// The batch loop holds deliveries that are not `Send`, on the app's one task.
#![allow(clippy::future_not_send)]

#[cfg(ruststream_alloc)]
use futures::FutureExt as _;
use futures::future::join;
use ruststream_core::{BatchSubscriber, MaybeSendSync};
#[cfg(not(ruststream_alloc))]
use ruststream_runtime::{BatchFan, BatchSize, Gathers};
use ruststream_runtime::{
    Detach, Halt, KeepCopies, Keeping, NoCopies, OneTaskBatchDispatch, OneTaskBatchRoute,
    PrepareCopy, RunsBatches, ShutdownWait,
};
#[cfg(not(ruststream_alloc))]
use typenum::{Cmp, Const, ToUInt, U};
use typenum::{Equal, Greater};

use crate::copies::CopyStore;
use crate::hooks::AppState;
#[cfg(not(ruststream_alloc))]
use crate::host::WorkerHost;
use crate::workers::{EmbeddedPlan, EncodeSize, NoRoom, OneWorker, Room};

/// Whether a store of `Self` records holds the retries of a batch of `Size`. Machinery; never
/// named directly.
#[doc(hidden)]
pub trait Holds<Size> {}

// A single delivery, and any batch of a build with `alloc`, whose size is no type.
impl<const CAPACITY: usize> Holds<()> for Room<CAPACITY> {}

#[cfg(not(ruststream_alloc))]
impl<const CAPACITY: usize, const SIZE: usize> Holds<BatchSize<SIZE>> for Room<CAPACITY>
where
    Const<CAPACITY>: ToUInt,
    Const<SIZE>: ToUInt,
    U<CAPACITY>: Cmp<U<SIZE>, Output: Enough<Self, BatchSize<SIZE>>>,
{
}

/// The comparison of a store's capacity with a batch's size, read off the type: the capacity is
/// not below the size. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "the store `{Store}` cannot hold the retries of a batch of `{Size}`",
    label = "`retry_capacity` is smaller than the batch",
    note = "a batch that retries whole needs one record per element: name a capacity of at least \
            the batch size"
)]
pub trait Enough<Store, Size> {}

impl<Store, Size> Enough<Store, Size> for Greater {}

impl<Store, Size> Enough<Store, Size> for Equal {}

/// What gathers the uniform outcomes of a batch of `Self`: a fan sized to the batch where the
/// build has no `alloc`. Machinery; never named directly.
#[doc(hidden)]
pub trait FanSize {
    /// The fan of a batch of this size.
    #[cfg(not(ruststream_alloc))]
    type Fan: Gathers;

    /// A fan with nothing to gather.
    #[cfg(not(ruststream_alloc))]
    fn fan() -> Self::Fan;
}

// A build with `alloc` shares the `.error` of a uniform outcome between its elements itself.
#[cfg(ruststream_alloc)]
impl FanSize for () {}

#[cfg(not(ruststream_alloc))]
impl<const SIZE: usize> FanSize for BatchSize<SIZE> {
    type Fan = BatchFan<SIZE>;

    fn fan() -> BatchFan<SIZE> {
        BatchFan::new()
    }
}

impl<Enc: EncodeSize, Size> OneTaskBatchDispatch for EmbeddedPlan<OneWorker, NoRoom, Enc, Size> {
    type Copies = NoCopies;

    #[cfg(ruststream_alloc)]
    // Not an `async fn`: a frame of its own would sit between the caller and the worker for
    // nothing.
    fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: OneTaskBatchRoute<Sub, Body, Cx>,
        _path: Option<Path>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Sub: BatchSubscriber,
        Body: RunsBatches<Sub::Batch, Cx, State>,
        Cx: MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
        Path: PrepareCopy<Cx> + MaybeSendSync,
    {
        route.run_batches(signal, state, host, Enc::room())
    }

    // The worker's own continuation slot lives in this frame, beside the loop.
    #[cfg(not(ruststream_alloc))]
    async fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: OneTaskBatchRoute<Sub, Body, Cx>,
        _path: Option<Path>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
    ) where
        Sub: BatchSubscriber,
        Body: RunsBatches<Sub::Batch, Cx, State>,
        Cx: MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
        Path: PrepareCopy<Cx> + MaybeSendSync,
    {
        let worker = WorkerHost::new(host);
        worker
            .around(route.run_batches(signal, state, &worker, Enc::room()))
            .await;
    }
}

impl<Enc: EncodeSize, const CAPACITY: usize, Size: FanSize> OneTaskBatchDispatch
    for EmbeddedPlan<OneWorker, Room<CAPACITY>, Enc, Size>
{
    type Copies = KeepCopies;

    async fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: OneTaskBatchRoute<Sub, Body, Cx>,
        path: Option<Path>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
    ) where
        Sub: BatchSubscriber,
        Body: RunsBatches<Sub::Batch, Cx, State>,
        Cx: MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
        Path: PrepareCopy<Cx> + MaybeSendSync,
    {
        let Some(path) = path else {
            // A registration the broker gave no copy path keeps nothing: the plain worker.
            self.without_room()
                .run(route, None::<Path>, signal, state, host)
                .await;
            return;
        };
        // The registration's own store: its copies wait in its records, and its driver
        // publishes the last of them after the batch loop has stopped. The loop, the worker's slot
        // and the driver are joined in this frame, so none of them adds a frame of its own.
        let store = CopyStore::<Path, CAPACITY>::new(path, &route.name).input(route.input);
        // The fan of the mount's uniform outcomes: the worker and the store report to it.
        #[cfg(not(ruststream_alloc))]
        let store = store.with_fan(Size::fan());
        let keeping = Keeping(host, &store);
        #[cfg(ruststream_alloc)]
        join(
            route
                .run_batches(signal, state, &keeping, Enc::room())
                .map(|()| store.close()),
            store.drive(host),
        )
        .await;
        #[cfg(not(ruststream_alloc))]
        {
            let worker = WorkerHost::new(&keeping);
            join(
                worker.around_then(
                    route.run_batches(signal, state, &worker, Enc::room()),
                    || {
                        store.close();
                    },
                ),
                store.drive(host),
            )
            .await;
        }
    }
}
