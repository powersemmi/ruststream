//! The runtime collector routers and scopes mount into: type-erased starters plus metadata.

use std::fmt;
use std::sync::Arc;

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

    /// Pushes a fully custom starter. Used by mounts that must do more at startup than open a
    /// subscription (pairing a publisher source before the handler can exist).
    pub(crate) fn push_raw(&mut self, starter: BoundStarter<B, State>, meta: HandlerMetadata) {
        self.starters.push(starter);
        self.handlers.push(meta);
    }

    pub(crate) fn into_parts(self) -> (Vec<BoundStarter<B, State>>, Vec<HandlerMetadata>) {
        (self.starters, self.handlers)
    }
}
