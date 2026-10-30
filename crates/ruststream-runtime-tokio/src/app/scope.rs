//! The per-broker handler registration scope.

use std::{cell::Cell, error::Error as StdError, fmt, sync::Arc};

use crate::{Broker, Connected, Subscriber};

use crate::FailurePolicies;
use crate::Handler;
use crate::HandlerMetadata;
use crate::PublishPolicy;
use crate::bindable::ConnectedSlot;
use crate::sink::RouterSink;
use crate::{BlanketLayer, Identity, Router};
use crate::{MountSink, RouterDef};
use crate::{PublishIdentity, PublishPipeline};

use super::include::ScopeRouter;
use super::{LifecycleHook, lifecycle_hooks::box_startup_publish};

/// What a scope registers before its mount chain, kept by the app for the length of one
/// `with_broker` call: the startup publishes and the registrations mounted at once.
///
/// The cells let the scope register through `&self`, so a `with_broker` closure takes the scope
/// by value without a `mut` binding and still ends in the chain it returns.
pub(super) struct ScopeParts<B: Broker, State> {
    pub(super) startup_hooks: Cell<Vec<LifecycleHook<State>>>,
    pub(super) sink: Cell<RouterSink<B, State>>,
}

impl<B: Broker + 'static, State: Send + Sync + 'static> ScopeParts<B, State> {
    pub(super) fn new() -> Self {
        Self {
            startup_hooks: Cell::new(Vec::new()),
            sink: Cell::new(RouterSink::new()),
        }
    }

    /// Runs `register` against the sink, holding it out of its cell for the call.
    fn with_sink(&self, register: impl FnOnce(&mut RouterSink<B, State>)) {
        let mut sink = self.sink.replace(RouterSink::new());
        register(&mut sink);
        self.sink.set(sink);
    }
}

/// A handler-registration scope bound to one broker.
///
/// Handed by value to the [`RustStream::with_broker`](crate::RustStream::with_broker) closure,
/// which returns the mount chain [`include`](Self::include) opens:
/// `|b| b.include(confirm).out_reply(Publish).include(audit)`. The chain carries every
/// registration's type, and the app mounts it, wrapped with its global middleware stack
/// `Layers`, once the closure returns; nothing starts before
/// [`RustStream::run`](crate::RustStream::run).
///
/// What the scope registers at once (a startup publish, a whole router) goes through `&self` and
/// hands back the scope's empty chain, so those calls come ahead of the chain the closure
/// returns, or end the closure.
pub struct BrokerScope<
    'scope,
    B: Broker,
    Layers = Identity,
    C = (),
    State = (),
    Pipeline = PublishIdentity,
> {
    pub(super) broker: &'scope B,
    /// The slot the runtime fills with this broker's connected form at startup; shared with
    /// every starter of this scope and with the [`Bound`](crate::Bound) tokens minted from it.
    pub(super) slot: &'scope ConnectedSlot<B>,
    pub(super) parts: &'scope ScopeParts<B, State>,
    pub(super) global: &'scope Layers,
    pub(super) pipeline: Pipeline,
    pub(super) codec: C,
}

impl<B: Broker + 'static, Layers, C, State, Pipeline>
    BrokerScope<'_, B, Layers, C, State, Pipeline>
{
    /// Returns the broker, for creating subscribers or publishers with its own API.
    #[must_use]
    pub const fn broker(&self) -> &B {
        self.broker
    }

    /// Registers a startup publish: once every broker is connected and the subscriptions are
    /// open, `source` is paired against this scope's broker and `hook` runs with the live
    /// publisher. The scope-side home of the first message (seeding reference data, announcing
    /// readiness): the pairing happens inside, so no token leaves the closure. A failing hook
    /// aborts startup, exactly like the app-level
    /// [`after_startup`](crate::RustStream::after_startup).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "services.started")]
    /// struct Started {
    ///     service: &'static str,
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn handle(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         // The rest of the system learns the service is up once its subscriptions are open.
    ///         b.after_startup(Publish, async move |publisher| {
    ///             publisher
    ///                 .message(&Started { service: "orders" })
    ///                 .publish()
    ///                 .await
    ///         });
    ///         b.include(handle)
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn after_startup<Source, Hook, Fut, E>(
        &self,
        source: Source,
        hook: Hook,
    ) -> ScopeRouter<B, C, Pipeline>
    where
        C: Clone,
        Pipeline: Clone,
        Source: PublishPolicy<Connected<B>> + Send + 'static,
        Source::Live: Send,
        Hook: FnOnce(Source::Live) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
        B: 'static,
    {
        let mut hooks = self.parts.startup_hooks.take();
        hooks.push(box_startup_publish::<B, State, Source, Hook, Fut, E>(
            Arc::clone(self.slot),
            source,
            hook,
        ));
        self.parts.startup_hooks.set(hooks);
        self.chain()
    }

    /// Attaches `handler` (wrapped with the app's global stack) to an already-created
    /// `subscriber`.
    ///
    /// Machinery, not the user path - see [`Router::handle`](crate::Router::handle);
    /// a service mounts definitions with [`include`](Self::include) and the value constructors.
    pub fn handle<S, H, Cx>(
        &self,
        subscriber: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> ScopeRouter<B, C, Pipeline>
    where
        C: Clone,
        Pipeline: Clone,
        S: Subscriber + Send + 'static,
        S::Message: Send + Sync + 'static,
        State: Send + Sync + 'static,
        Cx: crate::BuildContext<S::Message> + Send + Sync + 'static,
        H: Handler<S::Message, Cx, State> + 'static,
        Layers: BlanketLayer + Clone + Send + Sync + 'static,
    {
        let handler = self.global.apply::<S::Message, Cx, State, H>(handler);
        // A directly mounted subscriber names no descriptor, so no retry position can be bound
        // on it: nothing reports where a copy of one of its deliveries would be published.
        self.parts.with_sink(|sink| {
            sink.push_handle(subscriber, handler, meta, FailurePolicies::default());
        });
        self.chain()
    }

    /// Mounts every registration from `router` onto this broker, wrapping each handler with the
    /// app's global middleware stack.
    ///
    /// The router registers at once, ahead of the chain the closure returns, and the call hands
    /// back the scope's empty chain: the closure ends on it when the router is all the scope
    /// mounts (`|b| b.include_router(orders())`), or goes on with `.include(..)`.
    ///
    /// The app's global stack must be a [`BlanketLayer`] (it applies to handlers whose concrete
    /// types the router hides), which every bundled layer and any
    /// [`Stack`](crate::Stack) of them satisfies.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn handle(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[subscriber("audit")]
    /// async fn audit(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn orders() -> impl RouterDef<MemoryBroker> {
    ///     Router::new().include(handle)
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include_router(orders()).include(audit)
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn include_router<R>(&self, router: R) -> ScopeRouter<B, C, Pipeline>
    where
        R: RouterDef<B, State>,
        C: Clone,
        State: Send + Sync + 'static,
        Layers: BlanketLayer + Clone + Send + Sync + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
    {
        // A router hands its own publish path down, so what this scope names here is the
        // fallback for a bare registration list mounted directly.
        self.parts.with_sink(|sink| {
            router.mount(self.global, &self.pipeline, &PublishIdentity, sink);
        });
        self.chain()
    }

    /// The scope's empty mount chain, which a registration made through `&self` hands back so
    /// the closure can end on it or go on with `.include(..)`.
    pub(super) fn chain(&self) -> ScopeRouter<B, C, Pipeline>
    where
        C: Clone,
        Pipeline: Clone,
    {
        Router::for_scope(self.codec.clone(), self.pipeline.clone())
    }
}

impl<B: Broker, Layers, C, State, Pipeline> fmt::Debug
    for BrokerScope<'_, B, Layers, C, State, Pipeline>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerScope").finish_non_exhaustive()
    }
}
