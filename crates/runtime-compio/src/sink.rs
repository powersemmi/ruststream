//! The runtime collector routers and scopes mount into: type-erased starters plus metadata.

use std::{fmt, num::NonZeroUsize, sync::Arc};

use compio::runtime::JoinHandle;

use crate::Tasks;
use crate::app::Launch;
use crate::dispatch::{Hosted, spawn_batch_dispatch, spawn_dispatch, spawn_dispatch_workers};
use crate::{
    BatchHandler, BatchSubscriber, BlanketLayer, BoxError, Broker, BuildContext, Connected,
    DispatchFailure, FailurePolicies, Handler, HandlerMetadata, LocalBoxFuture, MountSink, NoRetry,
    Opened, OpenedSubscription, RetryCopies, ScopeDelivery, SendBroker, Subscriber, Workers,
    open_mounted_subscriber,
};

/// A deferred registration: given the broker's connected form (produced by
/// [`Broker::connect`](crate::Broker::connect) at startup), shared state, the scope's publish
/// context, the app's count of what deliveries leave behind and the launch (the stop signal, the cores'
/// mailboxes and the cores of this mount), it opens
/// the subscription and spawns the dispatch task. The source and handler are captured and
/// type-erased.
pub(crate) type BoundStarter<B, State> = Box<
    dyn FnOnce(
            Arc<Connected<B>>,
            Arc<State>,
            Arc<ScopeDelivery>,
            Tasks,
            Launch,
        ) -> LocalBoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// A starter with the concurrency its mount declared.
pub(crate) type Pushed<B, State> = (BoundStarter<B, State>, Workers);

/// The runtime collector a router mounts into: type-erased starters plus handler metadata.
///
/// `State` is the app's shared state type, threaded so a handler is only mounted on a sink whose state
/// type it matches.
///
/// Created and drained inside the application; a [`RouterDef`](crate::RouterDef) pushes
/// into it during [`include_router`](crate::BrokerScope::include_router). You do not
/// construct one directly.
pub struct CoreSink<B: Broker, State = ()> {
    starters: Vec<Pushed<B, State>>,
    handlers: Vec<HandlerMetadata>,
}

impl<B: Broker, State> fmt::Debug for CoreSink<B, State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoreSink")
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl<B: SendBroker, State: Send + Sync + 'static> CoreSink<B, State> {
    pub(crate) fn new() -> Self {
        Self {
            starters: Vec::new(),
            handlers: Vec::new(),
        }
    }

    pub(crate) fn into_parts(self) -> (Vec<Pushed<B, State>>, Vec<HandlerMetadata>) {
        (self.starters, self.handlers)
    }
}

impl<B: SendBroker, State: Send + Sync + 'static> MountSink<B, State> for CoreSink<B, State> {
    fn push_handle<Sub, Body, Cx>(
        &mut self,
        subscriber: Sub,
        handler: Body,
        meta: HandlerMetadata,
        policies: FailurePolicies,
    ) where
        Sub: Subscriber + Send + 'static,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Body: Handler<Sub::Message, Cx, State> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push((
            Box::new(move |_connected, state, scope, tasks, launch| {
                Box::pin(async move {
                    // No descriptor to ask where a copy of a delivery would go, and no mount chain
                    // that can bind one here, so this mount carries no retry path at all.
                    let hosted = Arc::new(Hosted::<Cx, NoRetry>::new(
                        open_mounted_subscriber(&scope, &name),
                        None,
                        tasks,
                    ));
                    let failure = DispatchFailure::new(policies, launch.stop.clone());
                    Ok(spawn_dispatch(
                        subscriber,
                        handler,
                        launch.stop,
                        name,
                        state,
                        hosted,
                        failure,
                    ))
                })
            }),
            Workers::sequential(),
        ));
        self.handlers.push(meta);
    }

    // See the trait: each part is the registration's own.
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
        Opening: Future<Output = Result<Opened<Sub, Body, Cx, Retry>, BoxError>>,
        Sub: Subscriber + Send + 'static,
        Sub::Message: Send + Sync + 'static,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        State: Send + Sync + 'static,
        Body: Handler<Sub::Message, Cx, State> + 'static,
        Global: BlanketLayer + Send + 'static,
        Retry: RetryCopies<Cx>,
    {
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push((
            Box::new(
                move |connected: Arc<Connected<B>>, state, scope, tasks, launch| {
                    Box::pin(async move {
                        let (subscriber, handler, delivery, retry) =
                            open(connected, scope, Arc::clone(&name)).await?;
                        // `BlanketLayer::apply` is an RPITIT whose hidden type borrows the layer, so
                        // the wrapped handler cannot leave this future: apply and spawn stay in one
                        // block.
                        let handler = global.apply::<Sub::Message, Cx, State, Body>(handler);
                        let failure = DispatchFailure::new(policies, launch.stop.clone());
                        Ok(spawn_dispatch_workers(
                            subscriber,
                            Arc::new(handler),
                            launch.stop,
                            name,
                            state,
                            Arc::new(Hosted::new(delivery, retry, tasks)),
                            failure,
                            workers,
                            &launch.fabric,
                            &launch.cores,
                        )?)
                    })
                },
            ),
            workers,
        ));
        self.handlers.push(meta);
    }

    fn push_batch<Open, Opening, Sub, Body, Cx, Retry>(
        &mut self,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
        batch_size: NonZeroUsize,
        open: Open,
    ) where
        Open: FnOnce(Arc<Connected<B>>, Arc<ScopeDelivery>, Arc<str>) -> Opening + Send + 'static,
        Opening: Future<Output = Result<Opened<Sub, Body, Cx, Retry>, BoxError>>,
        Sub: BatchSubscriber + Send + 'static,
        Sub::Message: Send + 'static,
        Sub::Batch: 'static,
        Cx: Send + Sync + 'static,
        Body: BatchHandler<Sub::Batch, Cx, State> + 'static,
        Retry: RetryCopies<Cx>,
    {
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push((
            Box::new(
                move |connected: Arc<Connected<B>>, state, scope, tasks, launch| {
                    Box::pin(async move {
                        let (subscriber, handler, delivery, retry) =
                            open(connected, scope, Arc::clone(&name)).await?;
                        let failure = DispatchFailure::new(policies, launch.stop.clone());
                        // Turbofish: the adapter handlers are generic over the batch context, so
                        // the pushed definition's own context names it.
                        Ok(spawn_batch_dispatch::<_, _, Cx, _, _>(
                            subscriber,
                            Arc::new(handler),
                            launch.stop,
                            name,
                            state,
                            Arc::new(Hosted::new(delivery, retry, tasks)),
                            failure,
                            workers,
                            batch_size,
                            &launch.fabric,
                            &launch.cores,
                        )?)
                    })
                },
            ),
            workers,
        ));
        self.handlers.push(meta);
    }

    // See the trait: each part is the registration's own.
    #[allow(clippy::too_many_arguments)]
    fn push_mounted<Open, Opening, Sub, Body, Cx, Retry>(
        &mut self,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
        handler: Body,
        open: Open,
    ) where
        Open: FnOnce(Arc<Connected<B>>, Arc<ScopeDelivery>, Arc<str>) -> Opening + Send + 'static,
        Opening: Future<Output = Result<OpenedSubscription<Sub, Cx, Retry>, BoxError>>,
        Sub: Subscriber + Send + 'static,
        Sub::Message: Send + Sync + 'static,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        State: Send + Sync + 'static,
        Body: Handler<Sub::Message, Cx, State> + 'static,
        Retry: RetryCopies<Cx>,
    {
        // Shared at mount, so starting the app allocates only what the subscription itself is.
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push((
            Box::new(
                move |connected: Arc<Connected<B>>, state, scope, tasks, launch| {
                    Box::pin(async move {
                        let (subscriber, delivery, retry) =
                            open(connected, scope, Arc::clone(&name)).await?;
                        let failure = DispatchFailure::new(policies, launch.stop.clone());
                        Ok(spawn_dispatch_workers(
                            subscriber,
                            handler,
                            launch.stop,
                            name,
                            state,
                            Arc::new(Hosted::new(delivery, retry, tasks)),
                            failure,
                            workers,
                            &launch.fabric,
                            &launch.cores,
                        )?)
                    })
                },
            ),
            workers,
        ));
        self.handlers.push(meta);
    }

    // See the trait: each part is the registration's own.
    #[allow(clippy::too_many_arguments)]
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
        Opening: Future<Output = Result<OpenedSubscription<Sub, Cx, Retry>, BoxError>>,
        Sub: BatchSubscriber + Send + 'static,
        Sub::Message: Send + 'static,
        Sub::Batch: 'static,
        Cx: Send + Sync + 'static,
        Body: BatchHandler<Sub::Batch, Cx, State> + 'static,
        Retry: RetryCopies<Cx>,
    {
        // See `push_mounted`.
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push((
            Box::new(
                move |connected: Arc<Connected<B>>, state, scope, tasks, launch| {
                    Box::pin(async move {
                        let (subscriber, delivery, retry) =
                            open(connected, scope, Arc::clone(&name)).await?;
                        let failure = DispatchFailure::new(policies, launch.stop.clone());
                        // Turbofish: see `push_batch`.
                        Ok(spawn_batch_dispatch::<_, _, Cx, _, _>(
                            subscriber,
                            handler,
                            launch.stop,
                            name,
                            state,
                            Arc::new(Hosted::new(delivery, retry, tasks)),
                            failure,
                            workers,
                            batch_size,
                            &launch.fabric,
                            &launch.cores,
                        )?)
                    })
                },
            ),
            workers,
        ));
        self.handlers.push(meta);
    }
}
