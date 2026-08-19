//! The deferred reply-publishing routes: encoded, byte-for-byte, and batch.
//!
//! Unlike [`SubscribeRoute`](super::routes::SubscribeRoute) these store the pieces of their
//! handler rather than a built one: the app's publish pipeline is only known at mount time and
//! the live reply publisher only exists once the broker connects, so mounting captures the
//! pieces in a starter that pairs the publisher, resolves the startup injections and builds the
//! handler at startup. A router-mounted publishing handler therefore picks up the app-wide
//! [`publish_layer`](crate::runtime::RustStream::publish_layer) chain.

use std::fmt;
use std::sync::Arc;

use serde::Serialize;

use crate::codec::Codec;
use crate::{
    BatchSubscriber, Broker, BuildContext, Connected, PublishPolicy, Publisher, SubscriptionSource,
};

use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingHandler};
use crate::runtime::dispatch::{Workers, spawn_dispatch_workers};
use crate::runtime::failure::{DispatchFailure, FailurePolicies};
use crate::runtime::inject::FromStartup;
use crate::runtime::input::DecodeWith;
use crate::runtime::lifecycle::BoxError;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::BlanketLayer;
use crate::runtime::publish::{PublishPipeline, PublishTransform, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::{PublishingCall, PublishingHandler};

use super::SourceMessage;
use super::routes::{MountRoute, RouteMeta};
use super::sink::RouterSink;

/// One reply-publishing registration whose reply travels the encoded wiring: a
/// [`TypedPublisher`] stack naming the reply codec and transforms. An implementation detail of
/// [`Router`](crate::runtime::Router)'s registration list.
///
/// `Extra` is the include-site attachment the definition's startup injections resolve against:
/// the unit padding when the handler declares none beyond its reply, the bound slot sources when
/// it carries [`Out`](crate::runtime::Out) parameters.
#[doc(hidden)]
pub struct PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) publisher: ReplySource,
    pub(super) extra: Extra,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// One reply-publishing registration whose reply leaves byte-for-byte through a bare publisher
/// (the `publish_raw("dest")` form). See [`PublishingRoute`]; the split is what lets each route
/// name the wiring its replies travel, which a route must do because the app's publish pipeline
/// is not known when the registration is made.
#[doc(hidden)]
pub struct RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) publisher: ReplySource,
    pub(super) extra: Extra,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// One batch reply-publishing registration, deferred (see [`PublishingRoute`]). An
/// implementation detail of [`Router`](crate::runtime::Router)'s registration list.
#[doc(hidden)]
pub struct BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) publisher: ReplySource,
    pub(super) extra: Extra,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// Renders the deferred routes by the registration they carry: they hold no built handler to
/// print, so without the metadata a router's registration list would be anonymous.
macro_rules! debug_by_metadata {
    ($($route:ident),+ $(,)?) => {$(
        impl<Source, Def, DecodeCodec, ReplySource, Extra> fmt::Debug
            for $route<Source, Def, DecodeCodec, ReplySource, Extra>
        {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($route))
                    .field("meta", &self.meta)
                    .finish_non_exhaustive()
            }
        }

        impl<Source, Def, DecodeCodec, ReplySource, Extra> RouteMeta
            for $route<Source, Def, DecodeCodec, ReplySource, Extra>
        {
            fn collect(&self, out: &mut Vec<HandlerMetadata>) {
                out.push(self.meta.clone());
            }
        }
    )+};
}

debug_by_metadata!(PublishingRoute, RawReplyRoute, BatchPublishingRoute);

impl<B, Source, Def, DecodeCodec, ReplySource, Extra, State, Leaf, ReplyCodec, Transforms>
    MountRoute<B, State> for PublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>
where
    B: Broker + 'static,
    // The subscription side: the source opens against the connected form, and the definition's
    // handler runs over the messages it yields.
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Sync + Send + 'static,
    SourceMessage<B, Source>: Send + Sync + 'static,
    State: Send + Sync + 'static,
    Def: PublishingCall<State> + 'static,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Def::Context: BuildContext<SourceMessage<B, Source>> + Send + Sync + 'static,
    DecodeCodec: Codec + Send + 'static,
    Extra: Send + Sync + 'static,
    // The reply side: a policy paired at startup into an encoded typed stack. Naming the live
    // form structurally is what lets the route stay independent of the app's publish pipeline,
    // which it learns only here.
    ReplySource: PublishPolicy<Connected<B>, Live = TypedPublisher<Leaf, ReplyCodec, Transforms>>
        + Send
        + 'static,
    Leaf: Publisher + 'static,
    ReplyCodec: Codec + Send + Sync + 'static,
    Transforms: PublishTransform<Def::Context> + Send + Sync + 'static,
{
    fn mount_one<G, PP>(self, global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            meta,
            policies,
            workers,
        } = self;
        // The apply-and-push tail: `BlanketLayer::apply` is an RPITIT whose hidden type cannot
        // be named, so the wrapped handler cannot leave the startup factory; apply and spawn
        // stay in one block instead. The scope's own tail names its `Layers::Handler` and uses
        // the factory helper.
        let global = global.clone();
        let pipeline = pipeline.clone();
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        sink.push_raw(
            Box::new(move |connected, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let publisher = publisher
                        .pair(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let injections =
                        Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                            .await
                            .map_err(|e| Box::new(e) as BoxError)?;
                    let handler = global.apply::<SourceMessage<B, Source>, Def::Context, State, _>(
                        PublishingHandler {
                            def,
                            codec,
                            publisher,
                            pipeline,
                            injections,
                            decode: policies.decode,
                        },
                    );
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
            }),
            meta,
        );
    }
}

impl<B, Source, Def, DecodeCodec, ReplySource, Extra, State, Live> MountRoute<B, State>
    for RawReplyRoute<Source, Def, DecodeCodec, ReplySource, Extra>
where
    B: Broker + 'static,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Sync + Send + 'static,
    SourceMessage<B, Source>: Send + Sync + 'static,
    State: Send + Sync + 'static,
    Def: PublishingCall<State> + 'static,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
    Def::Reply: AsRef<[u8]> + Send + Sync + 'static,
    Def::Context: BuildContext<SourceMessage<B, Source>> + Send + Sync + 'static,
    DecodeCodec: Codec + Send + 'static,
    Extra: Send + Sync + 'static,
    // The reply side: the policy pairs into a bare publisher, and the reply bytes go out as-is.
    ReplySource: PublishPolicy<Connected<B>, Live = Live> + Send + 'static,
    Live: Publisher + Send + Sync + 'static,
{
    fn mount_one<G, PP>(self, global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            meta,
            policies,
            workers,
        } = self;
        // See the encoded route's tail for why apply and spawn stay in one block. The pipeline
        // travels along for shape, though a byte-for-byte reply never runs it.
        let global = global.clone();
        let pipeline = pipeline.clone();
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        sink.push_raw(
            Box::new(move |connected, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let publisher = publisher
                        .pair(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let injections =
                        Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                            .await
                            .map_err(|e| Box::new(e) as BoxError)?;
                    let handler = global.apply::<SourceMessage<B, Source>, Def::Context, State, _>(
                        PublishingHandler {
                            def,
                            codec,
                            publisher,
                            pipeline,
                            injections,
                            decode: policies.decode,
                        },
                    );
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
            }),
            meta,
        );
    }
}

impl<B, Source, Def, DecodeCodec, ReplySource, Extra, BatchReply, State> MountRoute<B, State>
    for BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource, Extra>
where
    B: Broker + 'static,
    // The subscription side: batches open against the connected form.
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: BatchSubscriber + Sync + Send + 'static,
    SourceMessage<B, Source>: Send + 'static,
    State: Send + Sync + 'static,
    Def: BatchPublishingCall<State> + 'static,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    DecodeCodec: Send + Sync + 'static,
    Extra: Send + Sync + 'static,
    // The reply side: the source pairs at startup into a batch reply wiring (plain or
    // transactional).
    ReplySource: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
    BatchReply: ReplyPublisher + 'static,
{
    fn mount_one<G, PP>(self, _global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // Batch handlers are not wrapped by the per-message global stack, but they do pick up the
        // app's publish pipeline for their replies, so this whole mount is shared with the scope
        // rather than split per surface.
        let pipeline = pipeline.clone();
        let Self {
            source,
            def,
            codec,
            publisher,
            extra,
            meta,
            policies,
            workers,
        } = self;
        sink.push_injected_batch(
            source,
            async move |connected: Arc<Connected<B>>, subscriber| {
                let publisher = publisher
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

#[cfg(all(test, feature = "memory", feature = "json"))]
mod tests {
    use super::*;
    use crate::Name;
    use crate::codec::JsonCodec;
    use crate::memory::MemoryPublish;

    /// The deferred routes hold no built handler to print, so their `Debug` has to identify the
    /// registration by its metadata; without that a router's registration list is anonymous.
    #[test]
    fn deferred_route_debug_names_the_registration() {
        let publishing = PublishingRoute {
            source: Name::new("orders"),
            def: (),
            codec: JsonCodec,
            publisher: TypedPublisher::with_codec(MemoryPublish, JsonCodec),
            extra: ((),),
            meta: HandlerMetadata::raw("orders"),
            policies: FailurePolicies::default(),
            workers: Workers::sequential(),
        };
        let rendered = format!("{publishing:?}");
        assert!(rendered.contains("PublishingRoute"), "{rendered}");
        assert!(rendered.contains("orders"), "{rendered}");

        let raw_reply = RawReplyRoute {
            source: Name::new("raw-orders"),
            def: (),
            codec: JsonCodec,
            publisher: MemoryPublish,
            extra: ((),),
            meta: HandlerMetadata::raw("raw-orders"),
            policies: FailurePolicies::default(),
            workers: Workers::sequential(),
        };
        let rendered = format!("{raw_reply:?}");
        assert!(rendered.contains("RawReplyRoute"), "{rendered}");
        assert!(rendered.contains("raw-orders"), "{rendered}");

        let batch_publishing = BatchPublishingRoute {
            source: Name::new("bulk-orders"),
            def: (),
            codec: JsonCodec,
            publisher: TypedPublisher::with_codec(MemoryPublish, JsonCodec),
            extra: ((),),
            meta: HandlerMetadata::raw("bulk-orders"),
            policies: FailurePolicies::default(),
            workers: Workers::sequential(),
        };
        let rendered = format!("{batch_publishing:?}");
        assert!(rendered.contains("BatchPublishingRoute"), "{rendered}");
        assert!(rendered.contains("bulk-orders"), "{rendered}");
    }
}
