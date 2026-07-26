//! The `include` family on [`Router`]: mounting macro-generated definitions, in the
//! default-codec form (`C = ()`) and the chain-codec form (`C: Codec`).

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Connected, Publisher, SubscriptionSource};

use crate::runtime::batch::{BatchDef, SliceHandler};
use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::publish::{PublishTransform, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::PublishingDef;
use crate::runtime::subscriber_def::SubscriberDef;

use super::builder::Router;
use super::{
    BatchPublishingRouter, IncludedBatchRouter, IncludedRouter, PublishingRouter,
    SubscribedBatchRouter,
};

impl<B: Broker + 'static, R, RL> Router<B, R, (), RL> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Name a codec for the chain with [`with_codec`](Self::with_codec). The router-level
    /// counterpart of [`BrokerScope::include`](crate::runtime::BrokerScope::include).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include<D>(
        self,
        def: D,
    ) -> IncludedRouter<B, D::Source, D, crate::codec::DefaultCodec, (), RL, R>
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
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
    pub fn include_on<S, D>(
        self,
        source: S,
        def: D,
    ) -> IncludedRouter<B, S, D, crate::codec::DefaultCodec, (), RL, R>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
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
    pub fn include_batch<D>(
        self,
        def: D,
    ) -> IncludedBatchRouter<B, D::Source, D, crate::codec::DefaultCodec, (), RL, R>
    where
        D: BatchDef,
        D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <D::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let source = def.source();
        self.mount_batch(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on an explicit subscription
    /// `source` (overriding the macro's own source), decoding each element with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch_on<S, D>(
        self,
        source: S,
        def: D,
    ) -> IncludedBatchRouter<B, S, D, crate::codec::DefaultCodec, (), RL, R>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        self.mount_batch(source, def, crate::codec::DefaultCodec::default())
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
    ) -> SubscribedBatchRouter<B, S, T, crate::codec::DefaultCodec, H, (), RL, R>
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
    pub fn include_batch_publishing<D, RP>(
        self,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, D::Source, D, RP::Codec, RP, (), RL, R>
    where
        D: BatchPublishingDef + 'static,
        D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <D::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
        RP::Codec: Clone + 'static,
    {
        let codec = publisher.reply_codec().clone();
        let source = def.source();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding each element with the `publisher`'s own codec.
    pub fn include_batch_publishing_on<S, D, RP>(
        self,
        source: S,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, S, D, RP::Codec, RP, (), RL, R>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
        RP::Codec: Clone + 'static,
    {
        let codec = publisher.reply_codec().clone();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the `publisher`'s own codec and sending the reply through it.
    ///
    /// The mounted handler joins the app's publish pipeline at mount time: the app-wide
    /// [`publish_layer`](crate::runtime::RustStream::publish_layer)s wrap each reply, and the
    /// publisher's own static [`PublishTransform`] stack runs closest to the value.
    pub fn include_publishing<D, P, PC, PL>(
        self,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, D::Source, D, PC, P, PC, PL, (), RL, R>
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishTransform<D::Context> + 'static,
    {
        let codec = publisher.codec().clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding its input with the `publisher`'s own codec.
    pub fn include_publishing_on<S, D, P, PC, PL>(
        self,
        source: S,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, S, D, PC, P, PC, PL, (), RL, R>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishTransform<D::Context> + 'static,
    {
        let codec = publisher.codec().clone();
        self.mount_publishing(source, def, codec, publisher)
    }
}

impl<B: Broker + 'static, R, C: Codec + Clone + 'static, RL> Router<B, R, C, RL> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include<D>(self, def: D) -> IncludedRouter<B, D::Source, D, C, C, RL, R>
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_subscriber(source, def, codec)
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`, decoding
    /// its input with the chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include_on<S, D>(self, source: S, def: D) -> IncludedRouter<B, S, D, C, C, RL, R>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        self.mount_subscriber(source, def, codec)
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on its own source, decoding each
    /// element with the chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include_batch<D>(self, def: D) -> IncludedBatchRouter<B, D::Source, D, C, C, RL, R>
    where
        D: BatchDef,
        D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <D::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch(source, def, codec)
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on an explicit subscription
    /// `source`, decoding each element with the chain's codec (set by
    /// [`with_codec`](Self::with_codec)).
    pub fn include_batch_on<S, D>(
        self,
        source: S,
        def: D,
    ) -> IncludedBatchRouter<B, S, D, C, C, RL, R>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch(source, def, codec)
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
    ) -> SubscribedBatchRouter<B, S, T, C, H, C, RL, R>
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
    pub fn include_batch_publishing<D, RP>(
        self,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, D::Source, D, C, RP, C, RL, R>
    where
        D: BatchPublishingDef + 'static,
        D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <D::Source as SubscriptionSource<Connected<B>>>::Subscriber:
            BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding each element with the chain's codec.
    pub fn include_batch_publishing_on<S, D, RP>(
        self,
        source: S,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, S, D, C, RP, C, RL, R>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the chain's codec and replying through `publisher`.
    pub fn include_publishing<D, P, PC, PL>(
        self,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, D::Source, D, C, P, PC, PL, C, RL, R>
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
        <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishTransform<D::Context> + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding its input with the chain's codec.
    pub fn include_publishing_on<S, D, P, PC, PL>(
        self,
        source: S,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, S, D, C, P, PC, PL, C, RL, R>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishTransform<D::Context> + 'static,
    {
        let codec = self.codec.clone();
        self.mount_publishing(source, def, codec, publisher)
    }
}
