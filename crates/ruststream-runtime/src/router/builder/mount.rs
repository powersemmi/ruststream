//! Mounting one handler definition on its source: the `include` forms' shared tails, each
//! building the route its form dispatches through.

use alloc::borrow::ToOwned;
use core::marker::PhantomData;

use crate::{BatchSubscriber, Broker, Connected, LocalBatchSubscriber, SubscriptionSource};

use crate::batch::{BatchDef, CarriedBatch, DeserializedBatch, TypedBatch, batch_metadata};
use crate::batch_inject::{BatchInjectDef, batch_inject_metadata};
use crate::batch_publishing::{BatchPublishingDef, batch_publishing_metadata};
use crate::inject::{InjectDef, inject_metadata};
use crate::input::{Carried, DecodeWith, Materialize, Provided};
use crate::publishing::{PublishingDef, publishing_metadata};
use crate::settings::BatchSized;
use crate::subscriber_def::{SubscriberDef, subscriber_metadata};
use crate::typed::Typed;

use crate::router::routes::{BatchRoute, SubscribeRoute};
use crate::router::routes_inject::{BatchInjectRoute, InjectRoute};
use crate::router::routes_publish::{BatchPublishingRoute, PublishingRoute, RawReplyRoute};
use crate::router::{
    BatchInjectedRouter, BatchPublishingRouter, IncludedBatchRouter, IncludedCarriedBatchRouter,
    IncludedRawBatchRouter, IncludedRouter, InjectedRouter, LocalSourceMessage, PublishingRouter,
    RawReplyRouter,
};

use super::Router;

impl<B: Broker + 'static, Routes, RouteCodec, RouteLayers, RoutePipe>
    Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>
{
    /// Mounts a definition on `source`, decoding with `codec`. The shared tail of the plain and
    /// raw `include` forms.
    pub(crate) fn mount_subscriber<Source, Def, DecodeCodec>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
    ) -> IncludedRouter<B, Source, Def, DecodeCodec, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        Source: SubscriptionSource<Connected<B>> + 'static,
        Def: SubscriberDef,
        Def::Input: Materialize<DecodeCodec, LocalSourceMessage<B, Source>>,
        Def::Handler: 'static,
        DecodeCodec: Send + Sync + 'static,
    {
        let meta = subscriber_metadata(source.name().to_owned(), &def)
            .materialized_with::<Def::Input, DecodeCodec, LocalSourceMessage<B, Source>>()
            .describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = Typed::over(codec, def.into_handler()).on_decode_failure(policies.decode);
        self.with_route(SubscribeRoute {
            source,
            handler,
            meta,
            policies,
            workers,
            _context: PhantomData,
        })
    }

    /// Mounts a batch definition on `source`, decoding with `codec`. The shared tail of the
    /// plain batch `include` form.
    pub(crate) fn mount_batch<Source, Def, DecodeCodec>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
    ) -> IncludedBatchRouter<B, Source, Def, DecodeCodec, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        B: Broker,
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchDef + BatchSized,
        Def::Input: DecodeWith<DecodeCodec>,
        Def::Handler: 'static,
        DecodeCodec: Send + Sync + 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def)
            .decoded_with::<Def::Input, DecodeCodec>()
            .describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        // The handler bound alone cannot pin the kind, so the adapter names the def's input
        // kind explicitly.
        let handler = TypedBatch::<_, Def::Input, _, _>::over(codec, def.into_handler())
            .with_decode(policies.decode);
        self.with_route(BatchRoute {
            source,
            handler,
            meta,
            policies,
            workers,
            batch_size,
            _context: PhantomData,
        })
    }

    /// Mounts a self-deserializing batch definition on `source`: each element constructs itself
    /// from its delivery's payload, so no codec takes part.
    pub(in crate::router) fn mount_raw_batch<Source, Def, F>(
        self,
        source: Source,
        def: Def,
    ) -> IncludedRawBatchRouter<B, Source, Def, F, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        Source: SubscriptionSource<Connected<B>> + 'static,
        Source::Subscriber: LocalBatchSubscriber,
        Def: BatchDef<Input = Provided<F>> + BatchSized,
        Def::Handler: 'static,
    {
        // The self-deserializing batch mounts with no codec, so the media type it would report
        // is the one nothing produced.
        let meta =
            batch_metadata(source.name().to_owned(), &def).describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        let handler =
            DeserializedBatch::<_, F, _>::over(def.into_handler()).with_decode(policies.decode);
        self.with_route(BatchRoute {
            source,
            handler,
            meta,
            policies,
            workers,
            batch_size,
            _context: PhantomData,
        })
    }

    /// Mounts a carried batch definition on `source`: the subscription's batches lend the values
    /// of `T` the body reads, so no codec takes part.
    pub(in crate::router) fn mount_carried_batch<Source, Def, T>(
        self,
        source: Source,
        def: Def,
    ) -> IncludedCarriedBatchRouter<B, Source, Def, T, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        B: Broker,
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchDef<Input = Carried<T>> + BatchSized,
        Def::Handler: 'static,
        T: Clone + Send + Sync + 'static,
    {
        // No codec reads the values, so the media type it would report is the one nothing
        // produced.
        let meta =
            batch_metadata(source.name().to_owned(), &def).describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        let handler = CarriedBatch::<Source, Connected<B>, T, _>::over(def.into_handler())
            .with_decode(policies.decode);
        self.with_route(BatchRoute {
            source,
            handler,
            meta,
            policies,
            workers,
            batch_size,
            _context: PhantomData,
        })
    }

    /// Mounts an injected definition on `source`: its startup injections (an attached publish
    /// policy pairing into an `Out` parameter) resolve right after the subscription opens, so
    /// the handler holds live handles by construction. The tail of the `Out` form.
    pub(in crate::router) fn mount_inject<Source, Def, DecodeCodec, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        extra: Extra,
    ) -> InjectedRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + 'static,
        Def: InjectDef + 'static,
        Def::Input: Materialize<DecodeCodec, LocalSourceMessage<B, Source>>,
        DecodeCodec: 'static,
    {
        let meta = inject_metadata(source.name().to_owned(), &def)
            .materialized_with::<Def::Input, DecodeCodec, LocalSourceMessage<B, Source>>()
            .describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        self.with_route(InjectRoute {
            source,
            def,
            codec,
            extra,
            meta,
            policies,
            workers,
        })
    }

    /// The batch counterpart of [`mount_inject`](Self::mount_inject): the tail of the
    /// `BatchOut` form.
    pub(in crate::router) fn mount_batch_inject<Source, Def, DecodeCodec, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        extra: Extra,
    ) -> BatchInjectedRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        B: Broker,
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchInjectDef + BatchSized + 'static,
        Def::Input: DecodeWith<DecodeCodec>,
        DecodeCodec: Send + Sync + 'static,
    {
        let meta = batch_inject_metadata(source.name().to_owned(), &def)
            .decoded_with::<Def::Input, DecodeCodec>()
            .describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        self.with_route(BatchInjectRoute {
            source,
            def,
            codec,
            extra,
            meta,
            policies,
            workers,
            batch_size,
        })
    }

    /// Mounts a batch publishing definition on `source`, decoding with `codec` and replying
    /// through the `publisher` policy, paired by the runtime after connect. The shared tail of
    /// the `BatchPublishing` and `BatchPublishingOut` forms.
    pub(in crate::router) fn mount_batch_publishing_source<
        Source,
        Def,
        DecodeCodec,
        ReplySource,
        Extra,
    >(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        publisher: ReplySource,
        extra: Extra,
    ) -> BatchPublishingRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        ReplySource,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        B: Broker,
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchPublishingDef + BatchSized + 'static,
        Def::Input: DecodeWith<DecodeCodec>,
        DecodeCodec: Send + Sync + 'static,
        ReplySource: 'static,
    {
        let meta = batch_publishing_metadata(source.name().to_owned(), &def)
            .decoded_with::<Def::Input, DecodeCodec>()
            .describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        // Defer building the handler: the app's publish pipeline is only known at mount time and
        // the live reply publisher only exists once the broker connects, so mounting captures the
        // pieces in a starter that pairs and builds at startup (see `BatchPublishingRoute`),
        // letting a router-mounted batch publishing handler pick up the app-wide `publish_layer`
        // chain.
        self.with_route(BatchPublishingRoute {
            source,
            def,
            codec,
            publisher,
            extra,
            meta,
            policies,
            workers,
            batch_size,
        })
    }

    /// Mounts a publishing definition on `source` whose reply travels the encoded wiring: the
    /// shared tail of the `Publishing` and `PublishingOut` forms. See
    /// [`mount_batch_publishing_source`](Self::mount_batch_publishing_source) for why the
    /// handler is deferred.
    pub(in crate::router) fn mount_publishing_source<Source, Def, DecodeCodec, ReplySource, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        publisher: ReplySource,
        extra: Extra,
    ) -> PublishingRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        ReplySource,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + 'static,
        Def: PublishingDef + 'static,
        Def::Input: Materialize<DecodeCodec, LocalSourceMessage<B, Source>>,
        // Not `Codec`: `Materialize` already carries what the input asks of it, and a byte
        // input asks for nothing - the route is built with `()` there.
        DecodeCodec: 'static,
        ReplySource: 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def)
            .materialized_with::<Def::Input, DecodeCodec, LocalSourceMessage<B, Source>>()
            .describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        self.with_route(PublishingRoute {
            source,
            def,
            codec,
            publisher,
            extra,
            meta,
            policies,
            workers,
        })
    }

    /// The byte-reply counterpart of
    /// [`mount_publishing_source`](Self::mount_publishing_source): the reply bytes go out as-is
    /// through a bare publisher. The shared tail of the `RawReply` and `RawReplyOut` forms.
    pub(in crate::router) fn mount_raw_reply_source<Source, Def, DecodeCodec, ReplySource, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        publisher: ReplySource,
        extra: Extra,
    ) -> RawReplyRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        ReplySource,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + 'static,
        Def: PublishingDef + 'static,
        Def::Input: Materialize<DecodeCodec, LocalSourceMessage<B, Source>>,
        // Not `Codec`: `Materialize` already carries what the input asks of it, and a byte
        // input asks for nothing - the route is built with `()` there.
        DecodeCodec: 'static,
        ReplySource: 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def)
            .materialized_with::<Def::Input, DecodeCodec, LocalSourceMessage<B, Source>>()
            .describing::<Connected<B>, _>(&source);
        let policies = def.failure_policies();
        let workers = def.workers();
        self.with_route(RawReplyRoute {
            source,
            def,
            codec,
            publisher,
            extra,
            meta,
            policies,
            workers,
        })
    }
}
