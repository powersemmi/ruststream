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
use super::publish::{PublishLayer, PublishMiddleware, TypedPublisher};
use super::publishing::{PublishingDef, PublishingHandler};
use super::subscriber_def::SubscriberDef;
use super::typed::typed;

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

/// A lazily-bound group of handler registrations, not yet attached to any broker.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # fn build() {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{Context, HandlerMetadata, HandlerResult, Router};
/// use ruststream::Name;
///
/// let mut router = Router::<MemoryBroker>::new();
/// router.subscribe(
///     Name::new("events"),
///     |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
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

    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Name a codec explicitly with [`with_codec`](Self::with_codec). The router-level counterpart
    /// of [`BrokerScope::include`](super::BrokerScope::include): collect macro handlers in a
    /// standalone module, then mount the whole group with
    /// [`include_router`](super::BrokerScope::include_router).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include<D>(&mut self, def: D)
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        self.mount(def, crate::codec::DefaultCodec::default());
    }

    fn mount<D, C>(&mut self, def: D, codec: C)
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
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
        self.subscribe(source, handler, meta);
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the `publisher`'s own codec and sending the reply through it.
    ///
    /// The common case where input and reply share a format: name the codec once, on the
    /// `publisher`. Override the decode codec with [`with_codec`](Self::with_codec). Router handlers
    /// run with an empty dynamic publish pipeline - the app's
    /// [`publish_layer`](super::RustStream::publish_layer)s do not apply; the publisher's own static
    /// [`PublishLayer`] stack still does.
    pub fn include_publishing<D, P, PC, PL>(&mut self, def: D, publisher: TypedPublisher<P, PC, PL>)
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishLayer + 'static,
    {
        let codec = publisher.codec().clone();
        self.mount_publishing(def, codec, publisher);
    }

    fn mount_publishing<D, C, P, PC, PL>(
        &mut self,
        def: D,
        codec: C,
        publisher: TypedPublisher<P, PC, PL>,
    ) where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
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
        let pipeline: Arc<[Arc<dyn PublishMiddleware>]> = Arc::from([]);
        let handler = PublishingHandler {
            def,
            codec,
            publisher,
            pipeline,
        };
        self.subscribe(source, handler, meta);
    }

    /// Returns a view of this router that decodes with `codec` instead of the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// `router.with_codec(CborCodec).include(def)` is the explicit-codec form of
    /// [`include`](Self::include). The view's
    /// [`include_publishing`](RouterCodec::include_publishing) overrides the decode codec; the reply
    /// still uses the publisher's own.
    pub fn with_codec<C>(&mut self, codec: C) -> RouterCodec<'_, B, C>
    where
        C: Codec + Clone + 'static,
    {
        RouterCodec {
            router: self,
            codec,
        }
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

/// A codec-bound view of a [`Router`], returned by [`Router::with_codec`].
///
/// Its [`include`](Self::include) / [`include_publishing`](Self::include_publishing) decode handler
/// input with the chosen codec instead of the [`DefaultCodec`](crate::codec::DefaultCodec).
pub struct RouterCodec<'a, B, C> {
    router: &'a mut Router<B>,
    codec: C,
}

impl<B, C> std::fmt::Debug for RouterCodec<'_, B, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterCodec").finish_non_exhaustive()
    }
}

impl<B: Broker + 'static, C: Codec + Clone + 'static> RouterCodec<'_, B, C> {
    /// Mounts `def` on its own source, decoding its input with this view's codec.
    pub fn include<D>(&mut self, def: D)
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        self.router.mount(def, self.codec.clone());
    }

    /// Mounts a publishing `def` on its own source, decoding its input with this view's codec; the
    /// reply uses the `publisher`'s own codec.
    pub fn include_publishing<D, P, PC, PL>(&mut self, def: D, publisher: TypedPublisher<P, PC, PL>)
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
    {
        self.router
            .mount_publishing(def, self.codec.clone(), publisher);
    }
}
