//! [`Router`]: a broker-agnostic, statically-typed group of handler registrations.
//!
//! A `Router` collects subscriber registrations without a live broker, so a set of handlers can be
//! defined in its own module and mounted later. It is a consuming builder: each `include`/
//! `subscribe`/`handle` call takes the router by value and returns a new type carrying the added
//! registration, so the full registration list lives in the type. A builder function therefore
//! returns an opaque [`RouterDef`] rather than naming that type.
//!
//! Bind it to a broker by passing it to [`BrokerScope::include_router`](super::BrokerScope::include_router)
//! inside [`RustStream::with_broker`](super::RustStream::with_broker). Nothing connects or
//! subscribes until the application runs. Unlike a hand-rolled callback group, the app's global
//! [`layer`](super::RustStream::layer) stack DOES reach router handlers: each is wrapped with the
//! app's [`BlanketLayer`] global when the router is mounted.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::codec::Codec;
use crate::{Broker, Publisher, Subscriber, SubscriptionSource};

use super::context::State;
use super::dispatch::{Delivery, spawn_dispatch};
use super::handler::Handler;
use super::lifecycle::{BoxError, BoxFuture};
use super::metadata::HandlerMetadata;
use super::middleware::BlanketLayer;
use super::publish::{PublishLayer, PublishMiddleware, TypedPublisher};
use super::publishing::{PublishingDef, PublishingHandler};
use super::subscriber_def::SubscriberDef;
use super::typed::{Typed, typed};

/// A deferred registration: given the broker (after connect), shared state, the per-scope publish
/// [`Delivery`] context, and the shutdown token, it opens the subscription and spawns the dispatch
/// task. The source and handler are captured and type-erased.
pub(crate) type BoundStarter<B> = Box<
    dyn FnOnce(
            Arc<B>,
            Arc<State>,
            Arc<Delivery>,
            CancellationToken,
        ) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// The message a source's subscriber yields, for broker `B`. Tames the long projection in bounds
/// and return types.
type SourceMessage<B, S> = <<S as SubscriptionSource<B>>::Subscriber as Subscriber>::Message;

/// The router that mounting a [`SubscriberDef`] `D` (decoded with `C`) onto `R` produces. Names the
/// otherwise unwieldy builder return type.
type IncludedRouter<B, D, C, R> = Router<
    B,
    (
        SubscribeRoute<
            <D as SubscriberDef>::Source,
            Typed<
                SourceMessage<B, <D as SubscriberDef>::Source>,
                <D as SubscriberDef>::Input,
                C,
                <D as SubscriberDef>::Handler,
            >,
        >,
        R,
    ),
>;

/// The router that mounting a publishing [`PublishingDef`] `D` (replying through a `P`/`PC`/`PL`
/// publisher) onto `R` produces.
type PublishingRouter<B, D, P, PC, PL, R> = Router<
    B,
    (
        SubscribeRoute<<D as PublishingDef>::Source, PublishingHandler<D, PC, P, PC, PL>>,
        R,
    ),
>;

/// The runtime collector a router mounts into: type-erased starters plus handler metadata.
///
/// Created and drained inside the application; a [`RouterDef`] pushes into it during
/// [`include_router`](super::BrokerScope::include_router). You do not construct one directly.
pub struct RouterSink<B> {
    starters: Vec<BoundStarter<B>>,
    handlers: Vec<HandlerMetadata>,
}

impl<B> std::fmt::Debug for RouterSink<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterSink")
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> RouterSink<B> {
    pub(crate) fn new() -> Self {
        Self {
            starters: Vec::new(),
            handlers: Vec::new(),
        }
    }

    /// Erases an already-created subscriber and its handler into a starter.
    pub(crate) fn push_handle<S, H>(&mut self, subscriber: S, handler: H, meta: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters
            .push(Box::new(move |_broker, state, delivery, token| {
                Box::pin(async move {
                    Ok(spawn_dispatch(
                        subscriber, handler, token, name, state, delivery,
                    ))
                })
            }));
        self.handlers.push(meta);
    }

    /// Erases a source and its handler into a starter; the subscription opens after connect.
    pub(crate) fn push_subscribe<S, H>(&mut self, source: S, handler: H, meta: HandlerMetadata)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        H: Handler<SourceMessage<B, S>> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters
            .push(Box::new(move |broker: Arc<B>, state, delivery, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(broker.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    Ok(spawn_dispatch(
                        subscriber, handler, token, name, state, delivery,
                    ))
                })
            }));
        self.handlers.push(meta);
    }

    pub(crate) fn into_parts(self) -> (Vec<BoundStarter<B>>, Vec<HandlerMetadata>) {
        (self.starters, self.handlers)
    }
}

/// One subscription registration: a source plus the handler it dispatches to. An implementation
/// detail of [`Router`]'s registration list.
#[doc(hidden)]
#[derive(Debug)]
pub struct SubscribeRoute<S, H> {
    source: S,
    handler: H,
    meta: HandlerMetadata,
}

/// One registration bound to an already-created subscriber. An implementation detail of [`Router`].
#[doc(hidden)]
#[derive(Debug)]
pub struct HandleRoute<S, H> {
    subscriber: S,
    handler: H,
    meta: HandlerMetadata,
}

/// One mountable registration: applies the global blanket layer to its handler and registers it.
trait MountRoute<B> {
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>);
    fn collect(&self, out: &mut Vec<HandlerMetadata>);
}

impl<B, S, H> MountRoute<B> for SubscribeRoute<S, H>
where
    B: Broker + 'static,
    S: SubscriptionSource<B> + Send + 'static,
    S::Subscriber: Send + 'static,
    SourceMessage<B, S>: Send + Sync + 'static,
    H: Handler<SourceMessage<B, S>> + 'static,
{
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        let handler = global.apply::<SourceMessage<B, S>, H>(self.handler);
        sink.push_subscribe(self.source, handler, self.meta);
    }

    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<B, S, H> MountRoute<B> for HandleRoute<S, H>
where
    B: Broker + 'static,
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message> + 'static,
{
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        let handler = global.apply::<S::Message, H>(self.handler);
        sink.push_handle(self.subscriber, handler, self.meta);
    }

    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

/// A mountable group of handler registrations.
///
/// Mounting applies the app's global [`BlanketLayer`] to each handler and registers it, so the
/// app-wide [`layer`](super::RustStream::layer) stack reaches router handlers. Implemented by
/// [`Router`] and its internal registration list; you obtain one from a builder and pass it to
/// [`include_router`](super::BrokerScope::include_router). You do not implement it.
pub trait RouterDef<B> {
    /// Applies `global` to every registration and pushes it into `sink`. Called by `include_router`.
    #[doc(hidden)]
    fn mount<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>);

    /// Appends each registration's metadata, in registration order.
    #[doc(hidden)]
    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>);
}

impl<B: Broker + 'static> RouterDef<B> for () {
    fn mount<G: BlanketLayer>(self, _global: &G, _sink: &mut RouterSink<B>) {}
    fn collect_handlers(&self, _out: &mut Vec<HandlerMetadata>) {}
}

impl<B, Head, Tail> RouterDef<B> for (Head, Tail)
where
    B: Broker + 'static,
    Head: MountRoute<B>,
    Tail: RouterDef<B>,
{
    fn mount<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        // Registrations are prepended, so the tail holds the earlier ones; mount it first to keep
        // registration order.
        self.1.mount(global, sink);
        self.0.mount_one(global, sink);
    }

    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>) {
        self.1.collect_handlers(out);
        self.0.collect(out);
    }
}

/// A statically-typed, lazily-bound group of handler registrations, not attached to any broker.
///
/// Build it by chaining (each call consumes the router and returns a new type that carries the
/// added registration), then mount it with
/// [`include_router`](super::BrokerScope::include_router). The registration list `R` is an opaque
/// nested tuple, so a builder function returns `impl RouterDef<B>` instead of naming the type.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # fn build() {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{Context, HandlerMetadata, HandlerResult, Router, RouterDef};
/// use ruststream::Name;
///
/// fn routes() -> impl RouterDef<MemoryBroker> {
///     Router::<MemoryBroker>::new().subscribe(
///         Name::new("events"),
///         |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
///         HandlerMetadata::raw("events"),
///     )
/// }
/// // later: app.with_broker(broker, |b| b.include_router(routes()));
/// # }
/// ```
pub struct Router<B, R = ()> {
    routes: R,
    _broker: PhantomData<fn() -> B>,
}

impl<B: Broker + 'static> Default for Router<B, ()> {
    fn default() -> Self {
        Self {
            routes: (),
            _broker: PhantomData,
        }
    }
}

impl<B, R> std::fmt::Debug for Router<B, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router").finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> Router<B, ()> {
    /// Creates an empty router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl<B: Broker + 'static, R> Router<B, R> {
    /// Attaches `handler` to an already-created `subscriber`.
    ///
    /// The subscriber is created up front (before connect). Use this for brokers whose subscription
    /// does not need a live connection, or when you already hold a subscriber.
    pub fn handle<S, H>(
        self,
        subscriber: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> Router<B, (HandleRoute<S, H>, R)>
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message> + 'static,
    {
        Router {
            routes: (
                HandleRoute {
                    subscriber,
                    handler,
                    meta,
                },
                self.routes,
            ),
            _broker: PhantomData,
        }
    }

    /// Attaches `handler` to a subscription described by `source`.
    ///
    /// The subscription is opened when the application runs, after the broker is connected, so this
    /// is the path that works for brokers requiring a live connection to subscribe.
    pub fn subscribe<S, H>(
        self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> Router<B, (SubscribeRoute<S, H>, R)>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        H: Handler<SourceMessage<B, S>> + 'static,
    {
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler,
                    meta,
                },
                self.routes,
            ),
            _broker: PhantomData,
        }
    }

    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Name a codec explicitly with [`include_with`](Self::include_with). The router-level
    /// counterpart of [`BrokerScope::include`](super::BrokerScope::include).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include<D>(self, def: D) -> IncludedRouter<B, D, crate::codec::DefaultCodec, R>
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        self.include_with(def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// explicit `codec`.
    pub fn include_with<D, C>(self, def: D, codec: C) -> IncludedRouter<B, D, C, R>
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
    {
        let source = def.source();
        let mut meta = HandlerMetadata::typed::<D::Input>(source.name().to_owned());
        if let Some(description) = def.description() {
            meta = meta.with_description(description.to_owned());
        }
        if let Some(schema) = def.input_schema() {
            meta = meta.with_payload_schema(schema);
        }
        let handler = typed(codec, def.into_handler());
        self.subscribe(source, handler, meta)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the `publisher`'s own codec and sending the reply through it.
    ///
    /// Router handlers run with an empty dynamic publish pipeline - the app's
    /// [`publish_layer`](super::RustStream::publish_layer)s do not apply; the publisher's own static
    /// [`PublishLayer`] stack still does.
    pub fn include_publishing<D, P, PC, PL>(
        self,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, D, P, PC, PL, R>
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishLayer + 'static,
    {
        let source = def.source();
        let description = def.description().map(str::to_owned);
        let schema = def.input_schema();
        let mut meta = HandlerMetadata::typed::<D::Input>(source.name().to_owned())
            .with_output_type(std::any::type_name::<D::Reply>());
        if let Some(description) = description {
            meta = meta.with_description(description);
        }
        if let Some(schema) = schema {
            meta = meta.with_payload_schema(schema);
        }
        let codec = publisher.codec().clone();
        let pipeline: Arc<[Arc<dyn PublishMiddleware>]> = Arc::from([]);
        let handler = PublishingHandler {
            def,
            codec,
            publisher,
            pipeline,
        };
        self.subscribe(source, handler, meta)
    }
}

impl<B: Broker + 'static, R: RouterDef<B>> Router<B, R> {
    /// Returns metadata for every registered handler, in registration order.
    #[must_use]
    pub fn handlers(&self) -> Vec<HandlerMetadata> {
        let mut out = Vec::new();
        self.routes.collect_handlers(&mut out);
        out
    }
}

impl<B: Broker + 'static, R: RouterDef<B>> RouterDef<B> for Router<B, R> {
    fn mount<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        self.routes.mount(global, sink);
    }

    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}
