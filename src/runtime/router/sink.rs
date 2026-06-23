//! The runtime collector routers and scopes mount into: type-erased starters plus metadata.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{BatchSubscriber, Broker, Subscriber, SubscriptionSource};

use crate::runtime::batch::BatchHandler;
use crate::runtime::dispatch::{
    Delivery, Workers, spawn_batch_dispatch, spawn_dispatch, spawn_dispatch_workers,
};
use crate::runtime::failure::{DispatchFailure, ErrorShutdown, FailurePolicies};
use crate::runtime::handler::Handler;
use crate::runtime::lifecycle::{BoxError, BoxFuture};
use crate::runtime::metadata::HandlerMetadata;

use super::SourceMessage;

/// A deferred registration: given the broker (after connect), shared state, the per-scope publish
/// [`Delivery`] context, and the shutdown token, it opens the subscription and spawns the dispatch
/// task. The source and handler are captured and type-erased.
pub(crate) type BoundStarter<B, St> = Box<
    dyn FnOnce(
            Arc<B>,
            Arc<St>,
            Arc<Delivery>,
            ErrorShutdown,
            CancellationToken,
        ) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// The runtime collector a router mounts into: type-erased starters plus handler metadata. `St` is
/// the app's shared state type, threaded so a handler is only mounted on a sink whose state type it
/// matches.
///
/// Created and drained inside the application; a [`RouterDef`](crate::runtime::RouterDef) pushes
/// into it during [`include_router`](crate::runtime::BrokerScope::include_router). You do not
/// construct one directly.
pub struct RouterSink<B, St = ()> {
    starters: Vec<BoundStarter<B, St>>,
    handlers: Vec<HandlerMetadata>,
}

impl<B, St> std::fmt::Debug for RouterSink<B, St> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterSink")
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl<B: Broker + 'static, St: Send + Sync + 'static> RouterSink<B, St> {
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
        H: Handler<S::Message, Cx, St> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |_broker, state, delivery, shutdown, token| {
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
    /// [`BatchSubscriber::batches`]; the subscription opens after connect.
    pub(crate) fn push_subscribe_batch<S, H>(
        &mut self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        SourceMessage<B, S>: Send + 'static,
        H: BatchHandler<SourceMessage<B, S>> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |broker: Arc<B>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(broker.as_ref())
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
    /// the subscription opens after connect.
    pub(crate) fn push_subscribe_workers<S, H, Cx>(
        &mut self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
        policies: FailurePolicies,
        workers: Workers,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        SourceMessage<B, S>: Send + Sync + 'static,
        Cx: crate::BuildContext<SourceMessage<B, S>> + Send + 'static,
        H: Handler<SourceMessage<B, S>, Cx, St> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |broker: Arc<B>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(broker.as_ref())
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

    /// Erases a source and its handler into a starter; the subscription opens after connect.
    pub(crate) fn push_subscribe<S, H, Cx>(
        &mut self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
        policies: FailurePolicies,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        Cx: crate::BuildContext<SourceMessage<B, S>> + Send + 'static,
        H: Handler<SourceMessage<B, S>, Cx, St> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters.push(Box::new(
            move |broker: Arc<B>, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(broker.as_ref())
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

    pub(crate) fn into_parts(self) -> (Vec<BoundStarter<B, St>>, Vec<HandlerMetadata>) {
        (self.starters, self.handlers)
    }
}
