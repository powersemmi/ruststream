//! The per-broker handler registration scope and its shared mount tails.

use std::{error::Error as StdError, fmt, future::Future, sync::Arc};

use serde::Serialize;

use crate::{BatchSubscriber, Broker, Connected, Publisher, Subscriber, SubscriptionSource};

use crate::PublishPolicy;
use crate::runtime::batch::{
    BatchDef, BatchWithHeadersDef, TypedBatch, TypedBatchWithHeaders, batch_metadata,
};
use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectHandler, batch_inject_metadata};
use crate::runtime::batch_publishing::{
    BatchPublishingCall, BatchPublishingHandler, batch_publishing_metadata,
};
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::inject::{FromStartup, InjectCall, InjectHandler, inject_metadata};
use crate::runtime::input::{DecodeWith, InputKind};
use crate::runtime::lifecycle::{BoxError, ConnectedSlot};
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{BlanketLayer, Identity, Layer};
use crate::runtime::publish::{PublishIdentity, PublishPipeline, ReplyPublisher};
use crate::runtime::publisher_registry::ErasedPublisher;
use crate::runtime::publishing::{
    PublishingCall, PublishingHandler, ReplySink, publishing_metadata,
};
use crate::runtime::router::{RouterDef, RouterSink};
use crate::runtime::subscriber_def::{SubscriberDef, subscriber_metadata};
use crate::runtime::typed::Typed;

use super::include::MountCodec;
use super::{LifecycleHook, lifecycle_hooks::box_startup_publish};

/// A handler-registration scope bound to one broker.
///
/// Handed to the [`RustStream::with_broker`](crate::runtime::RustStream::with_broker) closure. It
/// is a [`Router`](crate::runtime::Router) plus the broker it is bound to and the global middleware
/// stack `Layers`; registrations are collected and started later, in
/// [`RustStream::run`](crate::runtime::RustStream::run). Each handler registered here is wrapped
/// with `Layers` before it is stored.
pub struct BrokerScope<B: Broker, Layers = Identity, C = (), State = (), Pipeline = PublishIdentity>
{
    pub(super) broker: B,
    /// The slot the runtime fills with this broker's connected form at startup; shared with
    /// every starter of this scope and with the [`Bound`] tokens minted here.
    pub(super) slot: ConnectedSlot<B>,
    /// Startup publishes registered on this scope: paired against the broker and run with the
    /// app-level `after_startup` hooks, in registration order.
    pub(super) startup_hooks: Vec<LifecycleHook<State>>,
    pub(super) sink: RouterSink<B, State>,
    pub(super) pipeline: Pipeline,
    pub(super) retry_publisher: Option<Arc<dyn ErasedPublisher>>,
    pub(super) global: Layers,
    pub(super) codec: C,
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Returns the broker, for creating subscribers or publishers with its own API.
    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
    }

    /// Registers a startup publish: once every broker is connected and the subscriptions are
    /// open, `source` is paired against this scope's broker and `hook` runs with the live
    /// publisher. The scope-side home of the first message (seeding reference data, announcing
    /// readiness): the pairing happens inside, so no token leaves the closure. A failing hook
    /// aborts startup, exactly like the app-level
    /// [`after_startup`](crate::runtime::RustStream::after_startup).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # fn demo() {
    /// use ruststream::memory::{MemoryBroker, MemoryPublish};
    /// use ruststream::runtime::{AppInfo, RustStream};
    /// use ruststream::{OutgoingMessage, Publisher};
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| {
    ///         b.after_startup(MemoryPublish, async move |publisher| {
    ///             let msg = OutgoingMessage::new("announcements", b"up".as_slice());
    ///             publisher.publish(msg).await
    ///         });
    ///     });
    /// # let _ = app;
    /// # }
    /// ```
    pub fn after_startup<Source, Hook, Fut, E>(&mut self, source: Source, hook: Hook)
    where
        Source: PublishPolicy<Connected<B>> + Send + 'static,
        Source::Live: Send,
        Hook: FnOnce(Source::Live) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
        B: 'static,
    {
        self.startup_hooks
            .push(box_startup_publish::<B, State, Source, Hook, Fut, E>(
                Arc::clone(&self.slot),
                source,
                hook,
            ));
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
        State: Send + Sync + 'static,
        Cx: crate::BuildContext<S::Message> + Send + 'static,
        H: Handler<S::Message, Cx, State> + 'static,
        Layers: Layer<H>,
        Layers::Handler: Handler<S::Message, Cx, State> + 'static,
    {
        let handler = self.global.layer(handler);
        self.sink
            .push_handle(subscriber, handler, meta, FailurePolicies::default());
    }

    /// Attaches `handler` (wrapped with the global stack) to a subscription described by `source`.
    ///
    /// See [`Router::subscribe`](crate::runtime::Router::subscribe).
    pub fn subscribe<S, H, Cx>(&mut self, source: S, handler: H, meta: HandlerMetadata)
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        State: Send + Sync + 'static,
        Cx: crate::BuildContext<<S::Subscriber as Subscriber>::Message> + Send + 'static,
        H: Handler<<S::Subscriber as Subscriber>::Message, Cx, State> + 'static,
        Layers: Layer<H>,
        Layers::Handler: Handler<<S::Subscriber as Subscriber>::Message, Cx, State> + 'static,
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
        R: RouterDef<B, State>,
        State: Send + Sync + 'static,
        Layers: BlanketLayer + Clone + Send + Sync + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
    {
        router.mount(&self.global, &self.pipeline, &mut self.sink);
    }
}

impl<B: Broker + 'static, Layers, SC, State, Pipeline> BrokerScope<B, Layers, SC, State, Pipeline> {
    /// Mounts a definition on `source`, decoding with `codec`. The shared tail of the plain and
    /// raw `include` forms.
    pub(super) fn mount_subscriber<Source, Def, DecodeCodec>(
        &mut self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        <Source::Subscriber as Subscriber>::Message: 'static,
        Def: SubscriberDef,
        Def::Input: DecodeWith<DecodeCodec>,
        Def::Context:
            crate::BuildContext<<Source::Subscriber as Subscriber>::Message> + Send + 'static,
        Def::Handler: 'static,
        DecodeCodec: Send + Sync + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<
            Typed<
                <Source::Subscriber as Subscriber>::Message,
                Def::Input,
                DecodeCodec,
                Def::Handler,
            >,
        >,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
    {
        let meta = subscriber_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = self
            .global
            .layer(Typed::over(codec, def.into_handler()).on_decode_failure(policies.decode));
        self.sink
            .push_subscribe_workers(source, handler, meta, policies, workers);
    }

    /// Mounts a batch definition on `source`, decoding each element with `codec`. The shared
    /// tail of the `include_batch` forms. Batch handlers are not wrapped by the global stack:
    /// per-message layers cannot wrap a whole-batch handler.
    pub(super) fn mount_batch<Source, Def, DecodeCodec>(
        &mut self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchDef,
        Def::Input: DecodeWith<DecodeCodec>,
        Def::Handler:
            crate::runtime::SliceHandler<<Def::Input as InputKind>::Owned, State> + 'static,
        DecodeCodec: Send + Sync + 'static,
        State: Send + Sync + 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        // The handler bound alone cannot pin the kind (two kinds may share an owned type), so
        // the adapter names the def's input kind explicitly.
        let handler = TypedBatch::<_, Def::Input, _, _>::over(codec, def.into_handler())
            .with_decode(policies.decode);
        self.sink
            .push_subscribe_batch(source, handler, meta, policies, workers);
    }

    /// Mounts a batch definition whose handler also reads a typed header contract per element.
    /// The adapter parses the contract next to the payload decode, so both failures follow the
    /// one decode policy and the handler's two slices stay aligned.
    pub(super) fn mount_batch_with_headers<Source, Def, DecodeCodec>(
        &mut self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchWithHeadersDef,
        Def::Input: DecodeWith<DecodeCodec>,
        Def::Handler: crate::runtime::SliceHandlerWithHeaders<
                <Def::Input as InputKind>::Owned,
                Def::Headers,
                State,
            > + 'static,
        DecodeCodec: Send + Sync + 'static,
        State: Send + Sync + 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = TypedBatchWithHeaders::<_, Def::Input, _, Def::Headers, _>::over(
            codec,
            def.into_handler(),
        )
        .with_decode(policies.decode);
        self.sink
            .push_subscribe_batch(source, handler, meta, policies, workers);
    }

    /// Mounts a publishing definition whose reply publisher is a policy source, paired by the
    /// runtime after connect. Decode uses the scope codec; how the reply leaves (encoded
    /// through a typed stack, or byte-for-byte through a bare publisher) is the source's live
    /// form, per its [`ReplySink`] wiring.
    pub(super) fn mount_publishing_source<Source, Def, ReplySource, OutExtra>(
        &mut self,
        source: Source,
        def: Def,
        reply: ReplySource,
        extra: OutExtra,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Sync + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + Sync + 'static,
        Def: PublishingCall<State> + 'static,
        Def::Input: DecodeWith<SC::Codec>,
        Def::Injections: FromStartup<B, Source::Subscriber, OutExtra> + Send + Sync + 'static,
        Def::Reply: Send + Sync + 'static,
        Def::Context: crate::BuildContext<<Source::Subscriber as Subscriber>::Message>
            + Send
            + Sync
            + 'static,
        ReplySource: PublishPolicy<Connected<B>> + Send + 'static,
        ReplySource::Live: ReplySink<Def::Reply, Def::Context, Pipeline> + 'static,
        OutExtra: Send + Sync + 'static,
        SC: MountCodec,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<PublishingHandler<Def, SC::Codec, ReplySource::Live, Pipeline>>
            + Clone
            + Send
            + 'static,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
        B::Connected: 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let codec = self.codec.mount_codec();
        let pipeline = self.pipeline.clone();
        let global = self.global.clone();
        // The injected primitive: the reply source pairs against the connected broker and the
        // startup injections resolve against the opened subscriber, both before the first
        // delivery.
        self.sink.push_injected_workers(
            source,
            async move |connected: Arc<Connected<B>>, subscriber| {
                let publisher = reply
                    .pair(connected.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                let handler = global.layer(PublishingHandler {
                    def,
                    codec,
                    publisher,
                    pipeline,
                    injections,
                    decode: policies.decode,
                });
                Ok((subscriber, handler))
            },
            meta,
            policies,
            workers,
        );
    }

    /// Mounts an injected definition: its startup injections (an attached publish policy
    /// pairing into an `Out` parameter, the subscription's own seeker for a `Seek` parameter)
    /// resolve right after the subscription opens, before the first delivery, so the handler
    /// holds live handles by construction. Decode uses the scope codec.
    pub(super) fn mount_inject<Source, Def, Extra>(
        &mut self,
        source: Source,
        def: Def,
        extra: Extra,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Sync + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + Sync + 'static,
        Def: InjectCall<State> + 'static,
        Def::Input: DecodeWith<SC::Codec>,
        Def::Context: crate::BuildContext<<Source::Subscriber as Subscriber>::Message>
            + Send
            + Sync
            + 'static,
        Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
        Extra: Send + Sync + 'static,
        SC: MountCodec,
        State: Send + Sync + 'static,
        Layers: Layer<InjectHandler<Def, SC::Codec>> + Clone + Send + 'static,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
        B::Connected: 'static,
    {
        let meta = inject_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let codec = self.codec.mount_codec();
        let global = self.global.clone();
        self.sink.push_injected_workers(
            source,
            async move |connected: Arc<Connected<B>>, subscriber| {
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                let handler = global.layer(InjectHandler {
                    def,
                    codec,
                    injections,
                    decode: policies.decode,
                });
                Ok((subscriber, handler))
            },
            meta,
            policies,
            workers,
        );
    }

    /// Mounts an injected batch definition on `source`: the subscription opens first, then the
    /// injections resolve against it (pairing the attached publish policy, minting a seeker)
    /// and the handler is built with them, so every injected handle is live by construction.
    /// The batch counterpart of [`mount_inject`](Self::mount_inject); batch handlers are not
    /// wrapped by the global stack (the documented middleware exception).
    pub(super) fn mount_batch_inject<Source, Def, Extra>(
        &mut self,
        source: Source,
        def: Def,
        extra: Extra,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Sync + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + 'static,
        Def: BatchInjectCall<State> + 'static,
        Def::Input: DecodeWith<SC::Codec>,
        Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
        Extra: Send + Sync + 'static,
        SC: MountCodec,
        State: Send + Sync + 'static,
        B::Connected: 'static,
    {
        let meta = batch_inject_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let codec = self.codec.mount_codec();
        self.sink.push_injected_batch(
            source,
            async move |connected: Arc<Connected<B>>, subscriber| {
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                let handler = BatchInjectHandler {
                    def,
                    codec,
                    injections,
                    decode: policies.decode,
                };
                Ok((subscriber, handler))
            },
            meta,
            policies,
            workers,
        );
    }

    /// Mounts a batch publishing definition whose reply publisher is a policy source, paired by
    /// the runtime after connect; its startup injections resolve against the opened subscriber
    /// in the same factory. Decode uses the scope codec.
    pub(super) fn mount_batch_publishing_source<Source, Def, ReplySource, BatchReply, OutExtra>(
        &mut self,
        source: Source,
        def: Def,
        reply: ReplySource,
        extra: OutExtra,
    ) where
        // The subscription side: batches open against the connected form.
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Sync + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + 'static,
        Def: BatchPublishingCall<State> + 'static,
        Def::Input: DecodeWith<SC::Codec>,
        Def::Injections: FromStartup<B, Source::Subscriber, OutExtra> + Send + Sync + 'static,
        Def::Reply: Serialize + Send + Sync + 'static,
        // The reply side: the source pairs at startup into a batch reply wiring (plain or
        // transactional).
        ReplySource: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
        BatchReply: ReplyPublisher + 'static,
        OutExtra: Send + Sync + 'static,
        SC: MountCodec,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
        B::Connected: 'static,
    {
        let meta = batch_publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let codec = self.codec.mount_codec();
        let pipeline = self.pipeline.clone();
        self.sink.push_injected_batch(
            source,
            async move |connected: Arc<Connected<B>>, subscriber| {
                let publisher = reply
                    .pair(connected.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                let handler = BatchPublishingHandler {
                    def,
                    codec,
                    publisher,
                    pipeline,
                    injections,
                    decode: policies.decode,
                };
                Ok((subscriber, handler))
            },
            meta,
            policies,
            workers,
        );
    }
}

impl<B: Broker, Layers, C, State, Pipeline> fmt::Debug
    for BrokerScope<B, Layers, C, State, Pipeline>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerScope")
            .field("sink", &self.sink)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::pin::pin;

    use futures::StreamExt as _;

    use crate::memory::MemoryBroker;
    use crate::runtime::publisher_registry::ErasedPublisher;
    use crate::runtime::{AppInfo, RustStream};
    use crate::{IncomingMessage, Subscriber};

    use super::Arc;

    /// The deferred-retry fallback is only reachable through a broker without native delayed
    /// redelivery (the in-memory one has it), so what the scope owes is the wiring: the publisher
    /// handed to `retry_via` is held erased and still reaches the broker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_via_holds_a_live_erased_publisher() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("retry.fallback");
        let publisher = broker.publisher();

        let mut fallback: Option<Arc<dyn ErasedPublisher>> = None;
        let _app = RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(broker, |b| {
            assert!(
                b.retry_publisher.is_none(),
                "a fresh scope has no fallback publisher",
            );
            b.retry_via(publisher);
            fallback = b.retry_publisher.clone();
        });

        let fallback = fallback.expect("retry_via must wire the deferred-retry publisher");
        fallback
            .publish_bytes("retry.fallback", b"deferred")
            .await
            .expect("the erased fallback publish failed");

        let mut stream = pin!(subscriber.stream());
        let msg = stream
            .next()
            .await
            .expect("the fallback publish must reach the broker")
            .expect("delivery");
        assert_eq!(msg.payload(), b"deferred");
    }

    #[test]
    fn scope_debug_reports_its_registrations() {
        let _app =
            RustStream::new(AppInfo::new("dbg", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
                let rendered = format!("{b:?}");
                assert!(rendered.starts_with("BrokerScope"), "{rendered}");
                assert!(rendered.contains("sink"), "{rendered}");
            });
    }
}
