//! The `include` family on [`Router`]: mounting macro-generated definitions, in the
//! default-codec form (`RouteCodec = ()`) and the chain-codec form (`RouteCodec: Codec`).

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Connected, SubscriptionSource};

use crate::runtime::batch::{BatchDef, BatchWithHeadersDef, SliceHandler};
use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::input::{DecodeWith, RawBytes};
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::publish::{PublishTransform, ReplyWiring, TypedPublisher};
use crate::runtime::publishing::PublishingDef;
use crate::runtime::subscriber_def::SubscriberDef;

use super::builder::Router;
use super::{
    BatchPublishingRouter, IncludedBatchRouter, IncludedBatchWithHeadersRouter, IncludedRouter,
    PublishingRouter, SubscribedBatchRouter,
};

impl<B: Broker + 'static, Routes, RouteLayers> Router<B, Routes, (), RouteLayers> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Name a codec for the chain with [`with_codec`](Self::with_codec). The router-level
    /// counterpart of [`BrokerScope::include`](crate::runtime::BrokerScope::include).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include<Def>(
        self,
        def: Def,
    ) -> IncludedRouter<B, Def::Source, Def, crate::codec::DefaultCodec, (), RouteLayers, Routes>
    where
        Def: SubscriberDef,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        Def::Input: DecodeWith<crate::codec::DefaultCodec>,
        Def::Handler: 'static,
    {
        let source = def.source();
        self.mount_subscriber(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`
    /// (overriding the macro's own source), decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Useful to retarget a handler - e.g. mount it on an in-memory source in tests, or a
    /// different broker descriptor per deployment. The subscription name in metadata comes from
    /// `source`. The router-level counterpart of
    /// [`BrokerScope::include_on`](crate::runtime::BrokerScope::include_on).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_on<Source, Def>(
        self,
        source: Source,
        def: Def,
    ) -> IncludedRouter<B, Source, Def, crate::codec::DefaultCodec, (), RouteLayers, Routes>
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        Def: SubscriberDef,
        Def::Input: DecodeWith<crate::codec::DefaultCodec>,
        Def::Handler: 'static,
    {
        self.mount_subscriber(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on its own source, decoding each
    /// element with the [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// The source's subscriber must implement [`BatchSubscriber`] - natively, or through the
    /// [`Buffered`](crate::Buffered) adapter. Router and app middleware wrap per-message handlers
    /// and do not apply to batch registrations.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch<Def>(
        self,
        def: Def,
    ) -> IncludedBatchRouter<B, Def::Source, Def, crate::codec::DefaultCodec, (), RouteLayers, Routes>
    where
        Def: BatchDef,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        Def::Input: DecodeWith<crate::codec::DefaultCodec>,
        Def::Handler: 'static,
    {
        let source = def.source();
        self.mount_batch(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on an explicit subscription
    /// `source` (overriding the macro's own source), decoding each element with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch_on<S, Def>(
        self,
        source: S,
        def: Def,
    ) -> IncludedBatchRouter<B, S, Def, crate::codec::DefaultCodec, (), RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchDef,
        Def::Input: DecodeWith<crate::codec::DefaultCodec>,
        Def::Handler: 'static,
    {
        self.mount_batch(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition whose handler also reads a typed
    /// header contract per element (`FromHeaders<Vec<H>>`), decoding each element with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// The contracts are parsed next to the payload decode, so an element failing either step is
    /// settled by the definition's decode policy and never reaches the handler.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch_with_headers<Def>(
        self,
        def: Def,
    ) -> IncludedBatchWithHeadersRouter<
        B,
        Def::Source,
        Def,
        crate::codec::DefaultCodec,
        (),
        RouteLayers,
        Routes,
    >
    where
        Def: BatchWithHeadersDef,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        Def::Input: DecodeWith<crate::codec::DefaultCodec>,
        Def::Handler: 'static,
    {
        let source = def.source();
        self.mount_batch_with_headers(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a header-reading batch definition on an explicit subscription `source` (overriding
    /// the macro's own source), decoding each element with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch_with_headers_on<S, Def>(
        self,
        source: S,
        def: Def,
    ) -> IncludedBatchWithHeadersRouter<
        B,
        S,
        Def,
        crate::codec::DefaultCodec,
        (),
        RouteLayers,
        Routes,
    >
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchWithHeadersDef,
        Def::Input: DecodeWith<crate::codec::DefaultCodec>,
        Def::Handler: 'static,
    {
        self.mount_batch_with_headers(source, def, crate::codec::DefaultCodec::default())
    }

    /// Attaches a slice handler to a batch subscription described by `source`, decoding each
    /// element with the [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// The functional-path counterpart of [`include_batch`](Self::include_batch): `handler` is
    /// any [`SliceHandler`](crate::runtime::SliceHandler), typically a closure
    /// `|batch: &[T], ctx: &mut Context| async { .. }`. The source's subscriber must implement
    /// [`BatchSubscriber`] - natively, or through the [`Buffered`](crate::Buffered) adapter.
    /// Set the dispatch concurrency with [`workers`](Router::workers) on the returned router.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn subscribe_batch<S, T, H>(
        self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> SubscribedBatchRouter<B, S, T, crate::codec::DefaultCodec, H, (), RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        T: DeserializeOwned + Send + Sync + 'static,
        H: SliceHandler<T> + 'static,
    {
        self.push_batch_route(source, handler, crate::codec::DefaultCodec::default(), meta)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on its own
    /// source, decoding each element with the `publisher`'s own codec and publishing the replies
    /// through it.
    ///
    /// `publisher` is either a plain [`TypedPublisher`] (each reply published independently) or
    /// a [`Transactional`](crate::runtime::Transactional) one (the batch's replies inside one
    /// transaction). The mounted handler joins the app's publish pipeline at mount time, like
    /// [`include_publishing`](Self::include_publishing).
    pub fn include_batch_publishing<Def, RP>(
        self,
        def: Def,
        publisher: RP,
    ) -> BatchPublishingRouter<B, Def::Source, Def, RP::Codec, RP, (), RouteLayers, Routes>
    where
        Def: BatchPublishingDef + 'static,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        Def::Input: DecodeWith<RP::Codec>,
        Def::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyWiring + 'static,
    {
        let codec = publisher.decode_codec().clone();
        let source = def.source();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding each element with the `publisher`'s own codec.
    pub fn include_batch_publishing_on<S, Def, RP>(
        self,
        source: S,
        def: Def,
        publisher: RP,
    ) -> BatchPublishingRouter<B, S, Def, RP::Codec, RP, (), RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchPublishingDef + 'static,
        Def::Input: DecodeWith<RP::Codec>,
        Def::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyWiring + 'static,
    {
        let codec = publisher.decode_codec().clone();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the `publisher`'s own codec and sending the reply through it.
    ///
    /// The mounted handler joins the app's publish pipeline at mount time: the app-wide
    /// [`publish_layer`](crate::runtime::RustStream::publish_layer)s wrap each reply, and the
    /// publisher's own static [`PublishTransform`] stack runs closest to the value.
    pub fn include_publishing<Def, Leaf, ReplyCodec, Transforms>(
        self,
        def: Def,
        publisher: TypedPublisher<Leaf, ReplyCodec, Transforms>,
    ) -> PublishingRouter<
        B,
        Def::Source,
        Def,
        ReplyCodec,
        Leaf,
        ReplyCodec,
        Transforms,
        (),
        RouteLayers,
        Routes,
    >
    where
        Def: PublishingDef + 'static,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        Def::Input: DecodeWith<ReplyCodec>,
        Def::Reply: Serialize + Send + Sync + 'static,
        Leaf: 'static,
        ReplyCodec: Codec + Clone + 'static,
        Transforms: PublishTransform<Def::Context> + 'static,
    {
        let codec = publisher.codec().clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding its input with the `publisher`'s own codec.
    pub fn include_publishing_on<Source, Def, Leaf, ReplyCodec, Transforms>(
        self,
        source: Source,
        def: Def,
        publisher: TypedPublisher<Leaf, ReplyCodec, Transforms>,
    ) -> PublishingRouter<
        B,
        Source,
        Def,
        ReplyCodec,
        Leaf,
        ReplyCodec,
        Transforms,
        (),
        RouteLayers,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        Def: PublishingDef + 'static,
        Def::Input: DecodeWith<ReplyCodec>,
        Def::Reply: Serialize + Send + Sync + 'static,
        Leaf: 'static,
        ReplyCodec: Codec + Clone + 'static,
        Transforms: PublishTransform<Def::Context> + 'static,
    {
        let codec = publisher.codec().clone();
        self.mount_publishing(source, def, codec, publisher)
    }
}

impl<B: Broker + 'static, Routes, RouteCodec, RouteLayers>
    Router<B, Routes, RouteCodec, RouteLayers>
{
    /// Mounts a `#[subscriber(.., raw)]`-generated definition on its own source.
    ///
    /// No codec is involved: the byte input kind decodes with `()`, so this form exists on every
    /// chain regardless of [`with_codec`](Self::with_codec) (which raw mounts ignore). The
    /// router-level counterpart of mounting a raw definition with
    /// [`BrokerScope::include`](crate::runtime::BrokerScope::include).
    #[must_use]
    pub fn include_raw<Def>(
        self,
        def: Def,
    ) -> IncludedRouter<B, Def::Source, Def, (), RouteCodec, RouteLayers, Routes>
    where
        Def: SubscriberDef<Input = RawBytes>,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        Def::Handler: 'static,
    {
        let source = def.source();
        self.mount_subscriber(source, def, ())
    }
}

impl<B: Broker + 'static, Routes, RouteCodec: Codec + Clone + 'static, RouteLayers>
    Router<B, Routes, RouteCodec, RouteLayers>
{
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include<Def>(
        self,
        def: Def,
    ) -> IncludedRouter<B, Def::Source, Def, RouteCodec, RouteCodec, RouteLayers, Routes>
    where
        Def: SubscriberDef,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Handler: 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_subscriber(source, def, codec)
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`, decoding
    /// its input with the chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include_on<Source, Def>(
        self,
        source: Source,
        def: Def,
    ) -> IncludedRouter<B, Source, Def, RouteCodec, RouteCodec, RouteLayers, Routes>
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        Def: SubscriberDef,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Handler: 'static,
    {
        let codec = self.codec.clone();
        self.mount_subscriber(source, def, codec)
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on its own source, decoding each
    /// element with the chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include_batch<Def>(
        self,
        def: Def,
    ) -> IncludedBatchRouter<B, Def::Source, Def, RouteCodec, RouteCodec, RouteLayers, Routes>
    where
        Def: BatchDef,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Handler: 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch(source, def, codec)
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on an explicit subscription
    /// `source`, decoding each element with the chain's codec (set by
    /// [`with_codec`](Self::with_codec)).
    pub fn include_batch_on<S, Def>(
        self,
        source: S,
        def: Def,
    ) -> IncludedBatchRouter<B, S, Def, RouteCodec, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchDef,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Handler: 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch(source, def, codec)
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition whose handler also reads a typed
    /// header contract per element (`FromHeaders<Vec<H>>`), decoding each element with the
    /// chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include_batch_with_headers<Def>(
        self,
        def: Def,
    ) -> IncludedBatchWithHeadersRouter<
        B,
        Def::Source,
        Def,
        RouteCodec,
        RouteCodec,
        RouteLayers,
        Routes,
    >
    where
        Def: BatchWithHeadersDef,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Handler: 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch_with_headers(source, def, codec)
    }

    /// Mounts a header-reading batch definition on an explicit subscription `source`, decoding
    /// each element with the chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include_batch_with_headers_on<S, Def>(
        self,
        source: S,
        def: Def,
    ) -> IncludedBatchWithHeadersRouter<B, S, Def, RouteCodec, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchWithHeadersDef,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Handler: 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch_with_headers(source, def, codec)
    }

    /// Attaches a slice handler to a batch subscription described by `source`, decoding each
    /// element with the chain's codec (set by [`with_codec`](Self::with_codec)).
    ///
    /// See the default-codec form for details on the handler shape.
    pub fn subscribe_batch<S, T, H>(
        self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> SubscribedBatchRouter<B, S, T, RouteCodec, H, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        T: DeserializeOwned + Send + Sync + 'static,
        H: SliceHandler<T> + 'static,
    {
        let codec = self.codec.clone();
        self.push_batch_route(source, handler, codec, meta)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on its own
    /// source, decoding each element with the chain's codec and publishing the replies through
    /// `publisher`.
    pub fn include_batch_publishing<Def, RP>(
        self,
        def: Def,
        publisher: RP,
    ) -> BatchPublishingRouter<B, Def::Source, Def, RouteCodec, RP, RouteCodec, RouteLayers, Routes>
    where
        Def: BatchPublishingDef + 'static,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Reply: Serialize + Send + Sync + 'static,
        RP: 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding each element with the chain's codec.
    pub fn include_batch_publishing_on<S, Def, RP>(
        self,
        source: S,
        def: Def,
        publisher: RP,
    ) -> BatchPublishingRouter<B, S, Def, RouteCodec, RP, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchPublishingDef + 'static,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Reply: Serialize + Send + Sync + 'static,
        RP: 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the chain's codec and replying through `publisher`.
    pub fn include_publishing<Def, Leaf, ReplyCodec, Transforms>(
        self,
        def: Def,
        publisher: TypedPublisher<Leaf, ReplyCodec, Transforms>,
    ) -> PublishingRouter<
        B,
        Def::Source,
        Def,
        RouteCodec,
        Leaf,
        ReplyCodec,
        Transforms,
        RouteCodec,
        RouteLayers,
        Routes,
    >
    where
        Def: PublishingDef + 'static,
        Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Reply: Serialize + Send + Sync + 'static,
        Leaf: 'static,
        ReplyCodec: Codec + 'static,
        Transforms: PublishTransform<Def::Context> + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding its input with the chain's codec.
    pub fn include_publishing_on<Source, Def, Leaf, ReplyCodec, Transforms>(
        self,
        source: Source,
        def: Def,
        publisher: TypedPublisher<Leaf, ReplyCodec, Transforms>,
    ) -> PublishingRouter<
        B,
        Source,
        Def,
        RouteCodec,
        Leaf,
        ReplyCodec,
        Transforms,
        RouteCodec,
        RouteLayers,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        Def: PublishingDef + 'static,
        Def::Input: DecodeWith<RouteCodec>,
        Def::Reply: Serialize + Send + Sync + 'static,
        Leaf: 'static,
        ReplyCodec: Codec + 'static,
        Transforms: PublishTransform<Def::Context> + 'static,
    {
        let codec = self.codec.clone();
        self.mount_publishing(source, def, codec, publisher)
    }
}
