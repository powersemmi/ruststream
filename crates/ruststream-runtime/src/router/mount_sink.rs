//! What a mount pushes its registrations into: the runner's collector of subscription starters.

use alloc::sync::Arc;
use core::num::NonZeroUsize;

use crate::batch::BatchHandler;
use crate::dispatch::{Delivery, Workers};
use crate::failure::FailurePolicies;
use crate::handler::Handler;
use crate::lifecycle::BoxError;
use crate::metadata::HandlerMetadata;
use crate::middleware::BlanketLayer;
use crate::redelivery::{OpenedSubscription, RetryCopies, RetryRoute, ScopeDelivery};
use crate::{BatchSubscriber, Broker, BuildContext, Connected, Subscriber};

/// What opening one subscription at startup hands the runner: the subscriber, its handler and the
/// publish context its deliveries run under, which the runner keeps beside its own handle, and the
/// route its retry copies leave through.
#[doc(hidden)]
pub type Opened<Sub, Body, Cx, Retry> = (Sub, Body, Delivery<Cx>, Option<RetryRoute<Retry>>);

/// The collector a router's registrations are pushed into when it mounts.
///
/// The mount code is the runner's guest: it builds what opens a subscription (pair the reply
/// publisher, open the subscriber, resolve the injections, wrap the handler in the app's layers)
/// and hands it over; the runner keeps it until the app starts and then spawns the subscription's
/// loop on its own runtime. Each method is one shape of registration, and carries the bounds that
/// runner's spawn asks of it.
#[doc(hidden)]
pub trait MountSink<B: Broker, State> {
    /// A subscriber that is already open, dispatched one delivery at a time.
    fn push_handle<Sub, Body, Cx>(
        &mut self,
        subscriber: Sub,
        handler: Body,
        meta: HandlerMetadata,
        policies: FailurePolicies,
    ) where
        Sub: Subscriber + Send + 'static,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Body: Handler<Sub::Message, Cx, State> + 'static;

    /// A subscription `open` opens at startup against the connected broker, dispatched under the
    /// `workers` policy. `open` receives the connected form, the scope's publish context and the
    /// subscription's name, and hands back the handler before the app's layers wrap it: the
    /// runner applies `global` to it where it spawns the loop, because the wrapped handler's
    /// type borrows the layer it was applied through and cannot leave the future that holds it.
    // Each part is the registration's own; bundling them would hide that.
    #[allow(clippy::too_many_arguments)]
    fn push_workers<Open, Opening, Sub, Body, Cx, Global, Retry>(
        &mut self,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
        global: Global,
        open: Open,
    ) where
        Open: FnOnce(Arc<Connected<B>>, Arc<ScopeDelivery>, Arc<str>) -> Opening + Send + 'static,
        Opening: Future<Output = Result<Opened<Sub, Body, Cx, Retry>, BoxError>> + Send,
        Sub: Subscriber + Send + 'static,
        Sub::Message: Send + Sync + 'static,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        State: Send + Sync + 'static,
        Body: Handler<Sub::Message, Cx, State> + 'static,
        Global: BlanketLayer + Send + 'static,
        Retry: RetryCopies<Cx>;

    /// The batch counterpart of [`push_workers`](Self::push_workers): the loop drives
    /// [`BatchSubscriber::batches`] with `batch_size`.
    fn push_batch<Open, Opening, Sub, Body, Cx, Retry>(
        &mut self,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
        batch_size: NonZeroUsize,
        open: Open,
    ) where
        Open: FnOnce(Arc<Connected<B>>, Arc<ScopeDelivery>, Arc<str>) -> Opening + Send + 'static,
        Opening: Future<Output = Result<Opened<Sub, Body, Cx, Retry>, BoxError>> + Send,
        Sub: BatchSubscriber + Send + 'static,
        Sub::Message: Send + 'static,
        Sub::Batch: 'static,
        Cx: Send + Sync + 'static,
        Body: BatchHandler<Sub::Batch, Cx, State> + 'static,
        Retry: RetryCopies<Cx>;
    /// A subscription whose handler the mount built in full, layers included: the runner takes
    /// it now and shares it with the loop it spawns once `open` has opened the subscriber. The
    /// rest is [`push_workers`](Self::push_workers).
    fn push_mounted<Open, Opening, Sub, Body, Cx, Retry>(
        &mut self,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
        handler: Body,
        open: Open,
    ) where
        Open: FnOnce(Arc<Connected<B>>, Arc<ScopeDelivery>, Arc<str>) -> Opening + Send + 'static,
        Opening: Future<Output = Result<OpenedSubscription<Sub, Cx, Retry>, BoxError>> + Send,
        Sub: Subscriber + Send + 'static,
        Sub::Message: Send + Sync + 'static,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        State: Send + Sync + 'static,
        Body: Handler<Sub::Message, Cx, State> + 'static,
        Retry: RetryCopies<Cx>;

    /// The batch counterpart of [`push_mounted`](Self::push_mounted).
    fn push_batch_mounted<Open, Opening, Sub, Body, Cx, Retry>(
        &mut self,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
        batch_size: NonZeroUsize,
        handler: Body,
        open: Open,
    ) where
        Open: FnOnce(Arc<Connected<B>>, Arc<ScopeDelivery>, Arc<str>) -> Opening + Send + 'static,
        Opening: Future<Output = Result<OpenedSubscription<Sub, Cx, Retry>, BoxError>> + Send,
        Sub: BatchSubscriber + Send + 'static,
        Sub::Message: Send + 'static,
        Sub::Batch: 'static,
        Cx: Send + Sync + 'static,
        Body: BatchHandler<Sub::Batch, Cx, State> + 'static,
        Retry: RetryCopies<Cx>;
}
