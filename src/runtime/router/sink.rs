//! The runtime collector routers and scopes mount into: type-erased starters plus metadata.

use std::{fmt, future::Future, sync::Arc};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{BatchSubscriber, Broker, Connected, Subscriber, SubscriptionSource};

use crate::runtime::batch::BatchHandler;
use crate::runtime::dispatch::{
    Delivery, Workers, spawn_batch_dispatch, spawn_dispatch, spawn_dispatch_workers,
};
use crate::runtime::failure::{DispatchFailure, ErrorShutdown, FailurePolicies};
use crate::runtime::handler::Handler;
use crate::runtime::lifecycle::{BoxError, BoxFuture};
use crate::runtime::metadata::HandlerMetadata;

use super::SourceMessage;

/// A deferred registration: given the broker's connected form (produced by
/// [`Broker::connect`](crate::Broker::connect) at startup), shared state, the per-scope publish
/// [`Delivery`] context, and the shutdown token, it opens the subscription and spawns the dispatch
/// task. The source and handler are captured and type-erased.
pub(crate) type BoundStarter<B, State> = Box<
    dyn FnOnce(
            Arc<Connected<B>>,
            Arc<State>,
            Arc<Delivery>,
            ErrorShutdown,
            CancellationToken,
        ) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// The runtime collector a router mounts into: type-erased starters plus handler metadata.
///
/// `State` is the app's shared state type, threaded so a handler is only mounted on a sink whose state
/// type it matches.
///
/// Created and drained inside the application; a [`RouterDef`](crate::runtime::RouterDef) pushes
/// into it during [`include_router`](crate::runtime::BrokerScope::include_router). You do not
/// construct one directly.
pub struct RouterSink<B: Broker, State = ()> {
    starters: Vec<BoundStarter<B, State>>,
    handlers: Vec<HandlerMetadata>,
}

impl<B: Broker, State> fmt::Debug for RouterSink<B, State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterSink")
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl<B: Broker + 'static, State: Send + Sync + 'static> RouterSink<B, State> {
    pub(crate) fn new() -> Self {
        Self {
            starters: Vec::new(),
            handlers: Vec::new(),
        }
    }

    /// Erases an already-created subscriber and its handler into a starter.
    pub(crate) fn push_handle<S, H, Cx>(
        &mut self,
        subscriber: S,
        handler: H,
        meta: HandlerMetadata,
        policies: FailurePolicies,
    ) where
        S: Subscriber + Send + 'static,
        Cx: crate::BuildContext<S::Message> + Send + 'static,
        H: Handler<S::Message, Cx, State> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |_connected, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_dispatch(
                        subscriber, handler, token, name, state, delivery, failure,
                    ))
                })
            },
        ));
        self.handlers.push(meta);
    }

    /// Erases a source and its batch handler into a starter driving
    /// [`BatchSubscriber::batches`]; the subscription opens against the connected broker.
    pub(crate) fn push_subscribe_batch<S, H>(
        &mut self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
    ) where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        SourceMessage<B, S>: Send + 'static,
        H: BatchHandler<SourceMessage<B, S>, State> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |connected: Arc<Connected<B>>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_batch_dispatch(
                        subscriber, handler, token, name, state, delivery, failure, workers,
                    ))
                })
            },
        ));
        self.handlers.push(meta);
    }

    /// Erases a source and its handler into a starter dispatching under the `workers` policy;
    /// the subscription opens against the connected broker.
    pub(crate) fn push_subscribe_workers<S, H, Cx>(
        &mut self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
    ) where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        SourceMessage<B, S>: Send + Sync + 'static,
        Cx: crate::BuildContext<SourceMessage<B, S>> + Send + 'static,
        H: Handler<SourceMessage<B, S>, Cx, State> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |connected: Arc<Connected<B>>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_dispatch_workers(
                        subscriber, handler, token, name, state, delivery, failure, workers,
                    ))
                })
            },
        ));
        self.handlers.push(meta);
    }

    /// Erases a source and its handler into a starter; the subscription opens against the
    /// connected broker.
    pub(crate) fn push_subscribe<S, H, Cx>(
        &mut self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
        policies: FailurePolicies,
    ) where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        Cx: crate::BuildContext<SourceMessage<B, S>> + Send + 'static,
        H: Handler<SourceMessage<B, S>, Cx, State> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |connected: Arc<Connected<B>>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_dispatch(
                        subscriber, handler, token, name, state, delivery, failure,
                    ))
                })
            },
        ));
        self.handlers.push(meta);
    }

    /// Pushes a fully custom starter, for the one mount the factory helpers cannot express:
    /// applying a [`BlanketLayer`](crate::runtime::BlanketLayer) inside the startup closure
    /// (its RPITIT return captures the layer borrow, so the applied handler cannot leave a
    /// factory closure).
    pub(crate) fn push_raw(&mut self, starter: BoundStarter<B, State>, meta: HandlerMetadata) {
        self.starters.push(starter);
        self.handlers.push(meta);
    }

    /// Erases a source plus an async handler factory into a starter under the `workers` policy.
    ///
    /// The factory runs at startup against the connected broker, for mounts whose handler can
    /// only exist then (pairing a publisher source first); the subscription, failure wiring and
    /// dispatch spawn stay here so every paired mount shares one shape.
    pub(crate) fn push_paired_workers<Source, MakeHandler, HandlerFut, NewHandler, HandlerCx>(
        &mut self,
        source: Source,
        make_handler: MakeHandler,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        SourceMessage<B, Source>: Send + Sync + 'static,
        MakeHandler: FnOnce(Arc<Connected<B>>) -> HandlerFut + Send + 'static,
        HandlerFut: Future<Output = Result<NewHandler, BoxError>> + Send,
        HandlerCx: crate::BuildContext<SourceMessage<B, Source>> + Send + 'static,
        NewHandler: Handler<SourceMessage<B, Source>, HandlerCx, State> + 'static,
    {
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |connected: Arc<Connected<B>>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let handler = make_handler(Arc::clone(&connected)).await?;
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_dispatch_workers(
                        subscriber,
                        Arc::new(handler),
                        token,
                        name,
                        state,
                        delivery,
                        failure,
                        workers,
                    ))
                })
            },
        ));
        self.handlers.push(meta);
    }

    /// Erases a source plus a handler factory over the opened subscriber into a starter under
    /// the `workers` policy.
    ///
    /// The counterpart of [`push_paired_workers`](Self::push_paired_workers) for handles minted
    /// off the live subscription (a seeker): the subscription opens first and the factory runs
    /// on the subscriber before the first delivery, so the handler holds a live handle by
    /// construction - a "not opened" state is never representable inside it.
    pub(crate) fn push_seek_workers<Source, MakeHandler, NewHandler, HandlerCx>(
        &mut self,
        source: Source,
        make_handler: MakeHandler,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        SourceMessage<B, Source>: Send + Sync + 'static,
        MakeHandler: FnOnce(&Source::Subscriber) -> NewHandler + Send + 'static,
        HandlerCx: crate::BuildContext<SourceMessage<B, Source>> + Send + 'static,
        NewHandler: Handler<SourceMessage<B, Source>, HandlerCx, State> + 'static,
    {
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |connected: Arc<Connected<B>>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let handler = make_handler(&subscriber);
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_dispatch_workers(
                        subscriber,
                        Arc::new(handler),
                        token,
                        name,
                        state,
                        delivery,
                        failure,
                        workers,
                    ))
                })
            },
        ));
        self.handlers.push(meta);
    }

    /// The batch counterpart of [`push_paired_workers`](Self::push_paired_workers).
    pub(crate) fn push_paired_batch<Source, MakeHandler, HandlerFut, NewHandler>(
        &mut self,
        source: Source,
        make_handler: MakeHandler,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        SourceMessage<B, Source>: Send + 'static,
        MakeHandler: FnOnce(Arc<Connected<B>>) -> HandlerFut + Send + 'static,
        HandlerFut: Future<Output = Result<NewHandler, BoxError>> + Send,
        NewHandler: BatchHandler<SourceMessage<B, Source>, State> + 'static,
    {
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |connected: Arc<Connected<B>>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let handler = make_handler(Arc::clone(&connected)).await?;
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_batch_dispatch(
                        subscriber,
                        Arc::new(handler),
                        token,
                        name,
                        state,
                        delivery,
                        failure,
                        workers,
                    ))
                })
            },
        ));
        self.handlers.push(meta);
    }

    pub(crate) fn into_parts(self) -> (Vec<BoundStarter<B, State>>, Vec<HandlerMetadata>) {
        (self.starters, self.handlers)
    }
}
