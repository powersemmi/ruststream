//! The per-broker handler registration scope.

use std::{error::Error as StdError, fmt, marker::PhantomData, sync::Arc};

use crate::SendBroker;
use crate::{Broker, Connected, Subscriber};

use crate::FailurePolicies;
use crate::Handler;
use crate::HandlerMetadata;
use crate::PublishPolicy;
use crate::bindable::ConnectedSlot;
use crate::sink::RouterSink;
use crate::{BlanketLayer, Identity};
use crate::{BuildContext, MountSink, RouterDef};
use crate::{PublishIdentity, PublishPipeline};

use super::{LifecycleHook, lifecycle_hooks::box_startup_publish};

/// A handler-registration scope bound to one broker.
///
/// Handed by value to the [`RustStream::with_broker`](crate::RustStream::with_broker) closure,
/// which returns the mount chain: `|b| b.include(confirm).out_reply(Publish).include(audit)`.
/// The chain carries every registration's type, and the app mounts it, wrapped with its global
/// middleware stack `Layers`, once the closure returns; nothing starts before
/// [`RustStream::run`](crate::RustStream::run).
///
/// The scope is a chain itself. [`after_startup`](Self::after_startup) and
/// [`include_router`](Self::include_router) are steps that return the grown scope, `Pre` is what
/// they registered, and a scope with nothing registered is the empty chain: a broker the service
/// only publishes to is `|b| b`.
pub struct BrokerScope<
    B: Broker,
    Layers = Identity,
    C = (),
    State = (),
    Pipeline = PublishIdentity,
    Pre = (),
> {
    /// The slot the runtime fills with this broker's connected form at startup; shared with
    /// every starter of this scope and with the [`Bound`](crate::Bound) tokens minted from it.
    pub(super) slot: ConnectedSlot<B>,
    pub(super) pre: Pre,
    pub(super) pipeline: Pipeline,
    pub(super) codec: C,
    pub(super) _app: PhantomData<fn() -> (Layers, State)>,
}

/// A startup publish a scope registered: the hook, already paired against the scope's broker.
#[doc(hidden)]
// Machinery that holds a closure, a router or a handler; nothing prints it.
#[allow(missing_debug_implementations)]
pub struct ScopedStartup<State>(LifecycleHook<State>);

/// A whole router a scope mounted ahead of its chain.
#[doc(hidden)]
// Machinery that holds a closure, a router or a handler; nothing prints it.
#[allow(missing_debug_implementations)]
pub struct ScopedRouter<R>(R);

/// A subscriber a scope mounted by hand ahead of its chain, with its handler and metadata.
#[doc(hidden)]
// Machinery that holds a closure, a router or a handler; nothing prints it.
#[allow(missing_debug_implementations)]
pub struct ScopedHandle<S, H, Cx> {
    subscriber: S,
    handler: H,
    meta: HandlerMetadata,
    _context: PhantomData<fn() -> Cx>,
}

/// What a scope registered ahead of its chain, in the order it was written: registers it into the
/// app's sink and its startup hooks. Machinery; you never implement or name it.
#[doc(hidden)]
pub trait ScopeRegistrations<B: Broker, Layers, State, Pipeline> {
    /// Registers every entry, earliest first.
    fn register(
        self,
        global: &Layers,
        pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        hooks: &mut Vec<LifecycleHook<State>>,
    );
}

impl<B: Broker, Layers, State, Pipeline> ScopeRegistrations<B, Layers, State, Pipeline> for () {
    fn register(
        self,
        _global: &Layers,
        _pipeline: &Pipeline,
        _sink: &mut RouterSink<B, State>,
        _hooks: &mut Vec<LifecycleHook<State>>,
    ) {
    }
}

// The list grows at its head, so the tail holds the earlier entries and registers first.
impl<B, Layers, State, Pipeline, Entry, Tail> ScopeRegistrations<B, Layers, State, Pipeline>
    for (Entry, Tail)
where
    B: Broker,
    Entry: ScopeRegistrations<B, Layers, State, Pipeline>,
    Tail: ScopeRegistrations<B, Layers, State, Pipeline>,
{
    fn register(
        self,
        global: &Layers,
        pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        hooks: &mut Vec<LifecycleHook<State>>,
    ) {
        self.1.register(global, pipeline, sink, hooks);
        self.0.register(global, pipeline, sink, hooks);
    }
}

impl<B: Broker, Layers, State, Pipeline> ScopeRegistrations<B, Layers, State, Pipeline>
    for ScopedStartup<State>
{
    fn register(
        self,
        _global: &Layers,
        _pipeline: &Pipeline,
        _sink: &mut RouterSink<B, State>,
        hooks: &mut Vec<LifecycleHook<State>>,
    ) {
        hooks.push(self.0);
    }
}

impl<B, Layers, State, Pipeline, R> ScopeRegistrations<B, Layers, State, Pipeline>
    for ScopedRouter<R>
where
    B: SendBroker,
    R: RouterDef<B, State>,
    State: Send + Sync + 'static,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
{
    fn register(
        self,
        global: &Layers,
        pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        _hooks: &mut Vec<LifecycleHook<State>>,
    ) {
        // A router hands its own publish path down, so what the scope names here is the
        // fallback for a bare registration list mounted directly.
        self.0.mount(global, pipeline, &PublishIdentity, sink);
    }
}

impl<B, Layers, State, Pipeline, S, H, Cx> ScopeRegistrations<B, Layers, State, Pipeline>
    for ScopedHandle<S, H, Cx>
where
    B: SendBroker,
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    State: Send + Sync + 'static,
    Cx: BuildContext<S::Message> + Send + Sync + 'static,
    H: Handler<S::Message, Cx, State> + 'static,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
{
    fn register(
        self,
        global: &Layers,
        _pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        _hooks: &mut Vec<LifecycleHook<State>>,
    ) {
        let handler = global.apply::<S::Message, Cx, State, H>(self.handler);
        // A directly mounted subscriber names no descriptor, so no retry position can be bound
        // on it: nothing reports where a copy of one of its deliveries would be published.
        sink.push_handle(
            self.subscriber,
            handler,
            self.meta,
            FailurePolicies::default(),
        );
    }
}

impl<B: SendBroker, Layers, C, State, Pipeline, Pre>
    BrokerScope<B, Layers, C, State, Pipeline, Pre>
{
    /// Grows the scope by one registration, keeping everything else.
    fn grow<Entry>(self, entry: Entry) -> BrokerScope<B, Layers, C, State, Pipeline, (Entry, Pre)> {
        BrokerScope {
            slot: self.slot,
            pre: (entry, self.pre),
            pipeline: self.pipeline,
            codec: self.codec,
            _app: PhantomData,
        }
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
    ///         })
    ///         .include(handle)
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[allow(clippy::type_complexity)] // the grown scope's own type
    pub fn after_startup<Source, Hook, Fut, E>(
        self,
        source: Source,
        hook: Hook,
    ) -> BrokerScope<B, Layers, C, State, Pipeline, (ScopedStartup<State>, Pre)>
    where
        Source: PublishPolicy<Connected<B>> + Send + 'static,
        Source::Live: Send,
        Hook: FnOnce(Source::Live) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
    {
        let hook = box_startup_publish::<B, State, Source, Hook, Fut, E>(
            Arc::clone(&self.slot),
            source,
            hook,
        );
        self.grow(ScopedStartup(hook))
    }

    /// Attaches `handler` (wrapped with the app's global stack) to an already-created
    /// `subscriber`.
    ///
    /// Machinery, not the user path - see [`Router::handle`](crate::Router::handle);
    /// a service mounts definitions with [`include`](Self::include) and the value constructors.
    #[allow(clippy::type_complexity)] // the grown scope's own type
    pub fn handle<S, H, Cx>(
        self,
        subscriber: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> BrokerScope<B, Layers, C, State, Pipeline, (ScopedHandle<S, H, Cx>, Pre)>
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message, Cx, State> + 'static,
    {
        self.grow(ScopedHandle {
            subscriber,
            handler,
            meta,
            _context: PhantomData,
        })
    }

    /// Mounts every registration from `router` onto this broker, wrapping each handler with the
    /// app's global middleware stack.
    ///
    /// The router registers ahead of everything the chain mounts after it. The closure ends on it
    /// when the router is all the scope mounts (`|b| b.include_router(orders())`), or goes on
    /// with `.include(..)`.
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
    #[allow(clippy::type_complexity)] // the grown scope's own type
    pub fn include_router<R>(
        self,
        router: R,
    ) -> BrokerScope<B, Layers, C, State, Pipeline, (ScopedRouter<R>, Pre)>
    where
        R: RouterDef<B, State>,
    {
        self.grow(ScopedRouter(router))
    }
}

impl<B: Broker, Layers, C, State, Pipeline, Pre> fmt::Debug
    for BrokerScope<B, Layers, C, State, Pipeline, Pre>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerScope").finish_non_exhaustive()
    }
}
