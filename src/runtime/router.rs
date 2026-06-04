//! [`Router`]: a broker-agnostic, lazily-bound group of handler registrations.
//!
//! A `Router` collects subscriber registrations without a live broker, so a set of handlers can be
//! defined in its own module and mounted later. Bind it to a broker by passing it to
//! [`BrokerScope::include_router`](super::BrokerScope::include_router) inside
//! [`RustStream::with_broker`](super::RustStream::with_broker). Nothing connects or subscribes
//! until the application runs.
//!
//! It is also the shared registration collector: a [`BrokerScope`](super::BrokerScope) is a
//! `Router` plus the broker it is bound to.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{Broker, Subscriber, SubscriptionSource};

use super::dispatch::spawn_dispatch;
use super::handler::Handler;
use super::lifecycle::{BoxError, BoxFuture};
use super::metadata::HandlerMetadata;

/// A deferred registration: given the broker (after connect) and the shutdown token, it opens the
/// subscription and spawns the dispatch task. The source and handler are captured and type-erased.
pub(crate) type BoundStarter<B> = Box<
    dyn FnOnce(Arc<B>, CancellationToken) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// A lazily-bound group of handler registrations, not yet attached to any broker.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # fn build() {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{HandlerMetadata, HandlerResult, Router};
/// use ruststream::Topic;
///
/// let mut router = Router::<MemoryBroker>::new();
/// router.subscribe(
///     Topic::new("events"),
///     |_msg: &_| async { HandlerResult::Ack },
///     HandlerMetadata::raw("events"),
/// );
/// // later: app.with_broker(broker, |b| b.include_router(router));
/// # }
/// ```
pub struct Router<B> {
    starters: Vec<BoundStarter<B>>,
    handlers: Vec<HandlerMetadata>,
}

impl<B> Default for Router<B> {
    fn default() -> Self {
        Self {
            starters: Vec::new(),
            handlers: Vec::new(),
        }
    }
}

impl<B> std::fmt::Debug for Router<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> Router<B> {
    /// Creates an empty router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attaches `handler` to an already-created `subscriber`.
    ///
    /// The subscriber is created up front (before connect). Use this for brokers whose
    /// subscription does not need a live connection, or when you already hold a subscriber.
    pub fn handle<S, H>(&mut self, subscriber: S, handler: H, meta: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message> + 'static,
    {
        let handler = Arc::new(handler);
        self.starters.push(Box::new(move |_broker, token| {
            Box::pin(async move { Ok(spawn_dispatch(subscriber, handler, token)) })
        }));
        self.handlers.push(meta);
    }

    /// Attaches `handler` to a subscription described by `source`.
    ///
    /// The subscription is opened when the application runs, after the broker is connected, so this
    /// is the path that works for brokers requiring a live connection to subscribe.
    pub fn subscribe<S, H>(&mut self, source: S, handler: H, meta: HandlerMetadata)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        H: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let handler = Arc::new(handler);
        self.starters.push(Box::new(move |broker: Arc<B>, token| {
            Box::pin(async move {
                let subscriber = source
                    .subscribe(broker.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(spawn_dispatch(subscriber, handler, token))
            })
        }));
        self.handlers.push(meta);
    }

    /// Merges another router's registrations into this one, preserving order.
    pub fn merge(&mut self, other: Self) {
        self.starters.extend(other.starters);
        self.handlers.extend(other.handlers);
    }

    /// Returns metadata for every registered handler, in registration order.
    #[must_use]
    pub fn handlers(&self) -> &[HandlerMetadata] {
        &self.handlers
    }

    pub(crate) fn into_parts(self) -> (Vec<BoundStarter<B>>, Vec<HandlerMetadata>) {
        (self.starters, self.handlers)
    }
}
