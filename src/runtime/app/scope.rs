//! The per-broker handler registration scope and its shared mount tails.

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Publisher, Subscriber, SubscriptionSource};

use crate::runtime::batch::{BatchDef, batch_metadata, typed_batch};
use crate::runtime::batch_publishing::{
    BatchPublishingDef, BatchPublishingHandler, batch_publishing_metadata,
};
use crate::runtime::dispatch::Publishers;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{BlanketLayer, Identity, Layer};
use crate::runtime::publish::{PublishLayer, PublishMiddleware, ReplyPublisher, TypedPublisher};
use crate::runtime::publisher_registry::ErasedPublisher;
use crate::runtime::publishing::{PublishingDef, PublishingHandler, publishing_metadata};
use crate::runtime::router::{RouterDef, RouterSink};
use crate::runtime::subscriber_def::{SubscriberDef, subscriber_metadata};
use crate::runtime::typed::{Typed, typed};

/// A handler-registration scope bound to one broker.
///
/// Handed to the [`RustStream::with_broker`](crate::runtime::RustStream::with_broker) closure. It
/// is a [`Router`](crate::runtime::Router) plus the broker it is bound to and the global middleware
/// stack `L`; registrations are collected and started later, in
/// [`RustStream::run`](crate::runtime::RustStream::run). Each handler registered here is wrapped
/// with `L` before it is stored.
pub struct BrokerScope<B, L = Identity, C = ()> {
    pub(super) broker: Arc<B>,
    pub(super) sink: RouterSink<B>,
    pub(super) publishers: Publishers,
    pub(super) pipeline: Arc<[Arc<dyn PublishMiddleware>]>,
    pub(super) retry_publisher: Option<Arc<dyn ErasedPublisher>>,
    pub(super) global: L,
    pub(super) codec: C,
}

impl<B: Broker + 'static, L, C> BrokerScope<B, L, C> {
    /// Returns the broker, for creating subscribers or publishers with its own API.
    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
    }

    /// Resolves a named publisher registered with
    /// [`RustStream::publisher`](crate::runtime::RustStream::publisher), to capture in a handler
    /// and publish to.
    #[must_use]
    pub fn publisher(&self, name: &str) -> Option<Arc<dyn ErasedPublisher>> {
        self.publishers.get(name).cloned()
    }

    /// Wires a publisher for the broker-agnostic `retry_after` fallback on this scope.
    ///
    /// When a handler returns [`HandlerResult::retry_after`](crate::runtime::HandlerResult::retry_after)
    /// (or a delivery is `nack_after`-ed) on a broker that does not natively support delayed
    /// redelivery, the runtime re-publishes the message to its own source subject after the delay,
    /// through `publisher`, with the
    /// [`RETRY_COUNT_HEADER`](crate::runtime::RETRY_COUNT_HEADER) incremented. Pass a publisher
    /// bound to the same broker (`b.broker().publisher()`); a publish to the source subject then
    /// reaches this scope's own subscriptions.
    ///
    /// Brokers with native delayed redelivery do not need this: the runtime uses their
    /// [`nack_after`](crate::IncomingMessage::nack_after) instead. Without it, a `retry_after` on a
    /// non-native broker degrades to an immediate requeue (with a warning).
    ///
    /// # Cancel safety
    ///
    /// The fallback's deferred re-publish is at-most-once over the delay window: see
    /// [`HandlerResult::retry_after`](crate::runtime::HandlerResult::retry_after).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::BrokerScope;
    /// use ruststream::{Broker, Publisher};
    ///
    /// // Wire a deferred-retry publisher bound to the same broker as the scope.
    /// fn configure<B, P>(scope: &mut BrokerScope<B>, retry_publisher: P)
    /// where
    ///     B: Broker + 'static,
    ///     P: Publisher + 'static,
    /// {
    ///     scope.retry_via(retry_publisher);
    /// }
    /// ```
    pub fn retry_via<P>(&mut self, publisher: P)
    where
        P: Publisher + 'static,
    {
        self.retry_publisher = Some(Arc::new(publisher));
    }

    /// Attaches `handler` (wrapped with the global stack) to an already-created `subscriber`.
    ///
    /// See [`Router::handle`](crate::runtime::Router::handle).
    pub fn handle<S, H, Cx>(&mut self, subscriber: S, handler: H, meta: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        Cx: crate::BuildContext<S::Message> + Send + 'static,
        H: Handler<S::Message, Cx> + 'static,
        L: Layer<H>,
        L::Handler: Handler<S::Message, Cx> + 'static,
    {
        let handler = self.global.layer(handler);
        self.sink
            .push_handle(subscriber, handler, meta, FailurePolicies::default());
    }

    /// Attaches `handler` (wrapped with the global stack) to a subscription described by `source`.
    ///
    /// See [`Router::subscribe`](crate::runtime::Router::subscribe).
    pub fn subscribe<S, H>(&mut self, source: S, handler: H, meta: HandlerMetadata)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        H: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
        L: Layer<H>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let handler = self.global.layer(handler);
        self.sink
            .push_subscribe(source, handler, meta, FailurePolicies::default());
    }

    /// Mounts every registration from `router` onto this broker, wrapping each handler with the
    /// app's global middleware stack.
    ///
    /// Unlike a hand-rolled handler group, a [`Router`](crate::runtime::Router) composes with the
    /// app's [`layer`](crate::runtime::RustStream::layer): the global stack must be a
    /// [`BlanketLayer`] (it applies to handlers whose concrete types the router hides), which every
    /// bundled layer and any [`Stack`](crate::runtime::Stack) of them satisfies.
    pub fn include_router<R>(&mut self, router: R)
    where
        R: RouterDef<B>,
        L: BlanketLayer,
    {
        router.mount(&self.global, &mut self.sink);
    }
}

impl<B: Broker + 'static, L, SC> BrokerScope<B, L, SC> {
    /// Mounts a definition on `source`, decoding with `codec`. The shared tail of the
    /// `include` / `include_on` forms.
    pub(super) fn mount_subscriber<S, D, C>(&mut self, source: S, def: D, codec: C)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
        L: Layer<Typed<<S::Subscriber as Subscriber>::Message, D::Input, C, D::Handler>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let meta = subscriber_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = self
            .global
            .layer(typed(codec, def.into_handler()).on_decode_failure(policies.decode));
        self.sink
            .push_subscribe_workers(source, handler, meta, policies, workers);
    }

    /// Mounts a batch definition on `source`, decoding each element with `codec`. The shared
    /// tail of the `include_batch` / `include_batch_on` forms. Batch handlers are not wrapped by
    /// the global stack: per-message layers cannot wrap a whole-batch handler.
    pub(super) fn mount_batch<S, D, C>(&mut self, source: S, def: D, codec: C)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = typed_batch(codec, def.into_handler()).with_decode(policies.decode);
        self.sink
            .push_subscribe_batch(source, handler, meta, policies, workers);
    }

    /// Mounts a batch publishing definition on `source`, decoding each element with `codec` and
    /// publishing the replies through `publisher`. The shared tail of the
    /// `include_batch_publishing` / `include_batch_publishing_on` forms.
    pub(super) fn mount_batch_publishing<S, D, C, RP>(
        &mut self,
        source: S,
        def: D,
        codec: C,
        publisher: RP,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        RP: ReplyPublisher + 'static,
    {
        let meta = batch_publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = BatchPublishingHandler {
            def,
            codec,
            publisher,
            pipeline: self.pipeline.clone(),
            decode: policies.decode,
        };
        self.sink
            .push_subscribe_batch(source, handler, meta, policies, workers);
    }

    /// Mounts a publishing definition on `source`, decoding with `codec` and replying through
    /// `publisher`. The shared tail of the `include_publishing` / `include_publishing_on` forms.
    pub(super) fn mount_publishing<S, D, C, P, PC, PL>(
        &mut self,
        source: S,
        def: D,
        codec: C,
        publisher: TypedPublisher<P, PC, PL>,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
        L: Layer<PublishingHandler<D, C, P, PC, PL>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = self.global.layer(PublishingHandler {
            def,
            codec,
            publisher,
            pipeline: self.pipeline.clone(),
            decode: policies.decode,
        });
        self.sink
            .push_subscribe_workers(source, handler, meta, policies, workers);
    }
}

impl<B, L, C> std::fmt::Debug for BrokerScope<B, L, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerScope")
            .field("sink", &self.sink)
            .finish_non_exhaustive()
    }
}
