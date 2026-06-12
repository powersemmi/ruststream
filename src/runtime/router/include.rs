//! The `include` family on [`Router`]: mounting macro-generated definitions, in the
//! default-codec form (`C = ()`) and the chain-codec form (`C: Codec`).

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Publisher, SubscriptionSource};

use crate::runtime::batch::BatchDef;
use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::publish::{PublishLayer, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::PublishingDef;
use crate::runtime::subscriber_def::SubscriberDef;

use super::builder::Router;
use super::{BatchPublishingRouter, IncludedBatchRouter, IncludedRouter, PublishingRouter};

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
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
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
        S: SubscriptionSource<B> + Send + 'static,
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
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
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
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        self.mount_batch(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on its own
    /// source, decoding each element with the `publisher`'s own codec and publishing the replies
    /// through it.
    ///
    /// `publisher` is either a plain [`TypedPublisher`] (each reply published independently) or
    /// a [`Transactional`](crate::runtime::Transactional) one (the batch's replies inside one
    /// transaction). Router handlers run with an empty dynamic publish pipeline, like
    /// [`include_publishing`](Self::include_publishing).
    pub fn include_batch_publishing<D, RP>(
        self,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, D::Source, D, RP::Codec, RP, (), RL, R>
    where
        D: BatchPublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
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
        S: SubscriptionSource<B> + Send + 'static,
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
    /// Router handlers run with an empty dynamic publish pipeline - the app's
    /// [`publish_layer`](crate::runtime::RustStream::publish_layer)s do not apply; the publisher's
    /// own static [`PublishLayer`] stack still does.
    pub fn include_publishing<D, P, PC, PL>(
        self,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, D::Source, D, PC, P, PC, PL, (), RL, R>
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
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishLayer + 'static,
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
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
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
        S: SubscriptionSource<B> + Send + 'static,
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
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
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
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch(source, def, codec)
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
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
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
        S: SubscriptionSource<B> + Send + 'static,
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
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
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
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
    {
        let codec = self.codec.clone();
        self.mount_publishing(source, def, codec, publisher)
    }
}
