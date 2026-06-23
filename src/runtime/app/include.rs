//! The `include` family on [`BrokerScope`]: mounting macro-generated definitions, in the
//! default-codec form (`C = ()`) and the scope-codec form (`C: Codec`).

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Publisher, Subscriber, SubscriptionSource};

use crate::runtime::batch::BatchDef;
use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::handler::Handler;
use crate::runtime::middleware::Layer;
use crate::runtime::publish::{PublishLayer, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::{PublishingDef, PublishingHandler};
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;

use super::scope::BrokerScope;

impl<B: Broker + 'static, L, St> BrokerScope<B, L, (), St> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec) and wrapping the handler with the global stack.
    ///
    /// Name a codec by setting a scope default with
    /// [`with_broker_codec`](crate::runtime::RustStream::with_broker_codec), or per handler by
    /// mounting through a codec-carrying router
    /// ([`Router::with_codec`](crate::runtime::Router::with_codec) +
    /// [`include_router`](Self::include_router)). The source comes from the macro: a [`Name`] for
    /// `#[subscriber("topic")]` (the broker must implement [`Subscribe`]) or a broker descriptor for
    /// `#[subscriber(RedisStream::new(..))]`.
    ///
    /// [`Name`]: crate::Name
    /// [`Subscribe`]: crate::Subscribe
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include<D>(&mut self, def: D)
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        D::Context: crate::BuildContext<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
            > + Send
            + 'static,
        St: Send + Sync + 'static,
        L: Layer<
            Typed<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
                D::Input,
                crate::codec::DefaultCodec,
                D::Handler,
            >,
        >,
        L::Handler: Handler<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
                D::Context,
                St,
            > + 'static,
    {
        let source = def.source();
        self.mount_subscriber(source, def, crate::codec::DefaultCodec::default());
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`, decoding
    /// its input with the [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// To decode with another codec, set a scope default with
    /// [`with_broker_codec`](crate::runtime::RustStream::with_broker_codec) or mount through a
    /// codec-carrying router ([`Router::with_codec`](crate::runtime::Router::with_codec)).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_on<S, D>(&mut self, source: S, def: D)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        D::Context: crate::BuildContext<<S::Subscriber as Subscriber>::Message> + Send + 'static,
        St: Send + Sync + 'static,
        L: Layer<
            Typed<
                <S::Subscriber as Subscriber>::Message,
                D::Input,
                crate::codec::DefaultCodec,
                D::Handler,
            >,
        >,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message, D::Context, St> + 'static,
    {
        self.mount_subscriber(source, def, crate::codec::DefaultCodec::default());
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on its own source, decoding each
    /// element with the [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// The source's subscriber must implement [`BatchSubscriber`] - natively, or through the
    /// [`Buffered`](crate::Buffered) adapter. The handler consumes the whole decoded batch as a
    /// slice and its [`HandlerResult`](crate::runtime::HandlerResult) settles every message in the
    /// batch; elements that fail to decode are nacked individually and never reach the handler.
    ///
    /// App-global middleware ([`RustStream::layer`](crate::runtime::RustStream::layer)) wraps
    /// per-message handlers and does not apply to batch registrations.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch<D>(&mut self, def: D)
    where
        D: BatchDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: crate::runtime::SliceHandler<D::Input, St> + 'static,
        St: Send + Sync + 'static,
    {
        let source = def.source();
        self.mount_batch(source, def, crate::codec::DefaultCodec::default());
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on an explicit subscription
    /// `source` (overriding the macro's own source), decoding each element with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch_on<S, D>(&mut self, source: S, def: D)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: crate::runtime::SliceHandler<D::Input, St> + 'static,
        St: Send + Sync + 'static,
    {
        self.mount_batch(source, def, crate::codec::DefaultCodec::default());
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on its own
    /// source, decoding each element with the `publisher`'s own codec and publishing the replies
    /// through it.
    ///
    /// `publisher` is either a plain [`TypedPublisher`] (each reply published independently) or
    /// a [`Transactional`](crate::runtime::Transactional) one built with
    /// [`TypedPublisher::transactional`]: per batch, the runtime begins a transaction, publishes
    /// every reply, commits, then acks the batch; any failure aborts and the batch is retried.
    pub fn include_batch_publishing<D, RP>(&mut self, def: D, publisher: RP)
    where
        D: BatchPublishingDef<State = St> + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
        RP::Codec: Clone + 'static,
        St: Send + Sync + 'static,
    {
        let codec = publisher.reply_codec().clone();
        let source = def.source();
        self.mount_batch_publishing(source, def, codec, publisher);
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding each element with the `publisher`'s own codec.
    pub fn include_batch_publishing_on<S, D, RP>(&mut self, source: S, def: D, publisher: RP)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef<State = St> + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
        RP::Codec: Clone + 'static,
        St: Send + Sync + 'static,
    {
        let codec = publisher.reply_codec().clone();
        self.mount_batch_publishing(source, def, codec, publisher);
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the `publisher`'s own codec and replying through it.
    ///
    /// Name the codec once, on the `publisher`. Override the decode codec by setting a scope
    /// default with [`with_broker_codec`](crate::runtime::RustStream::with_broker_codec).
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
        St: Send + Sync + 'static,
        L: Layer<PublishingHandler<D, PC, P, PC, PL>>,
        L::Handler: Handler<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
                (),
                St,
            > + 'static,
    {
        let codec = publisher.codec().clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher);
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding its input with the `publisher`'s own codec.
    pub fn include_publishing_on<S, D, P, PC, PL>(
        &mut self,
        source: S,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishLayer + 'static,
        St: Send + Sync + 'static,
        L: Layer<PublishingHandler<D, PC, P, PC, PL>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message, (), St> + 'static,
    {
        let codec = publisher.codec().clone();
        self.mount_publishing(source, def, codec, publisher);
    }
}

impl<B: Broker + 'static, L, C: Codec + Clone + 'static, St> BrokerScope<B, L, C, St> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// scope's default codec (set by
    /// [`with_broker_codec`](crate::runtime::RustStream::with_broker_codec)).
    pub fn include<D>(&mut self, def: D)
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        D::Context: crate::BuildContext<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
            > + Send
            + 'static,
        St: Send + Sync + 'static,
        L: Layer<
            Typed<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
                D::Input,
                C,
                D::Handler,
            >,
        >,
        L::Handler: Handler<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
                D::Context,
                St,
            > + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_subscriber(source, def, codec);
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`, decoding
    /// its input with the scope's default codec.
    pub fn include_on<S, D>(&mut self, source: S, def: D)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        D::Context: crate::BuildContext<<S::Subscriber as Subscriber>::Message> + Send + 'static,
        St: Send + Sync + 'static,
        L: Layer<Typed<<S::Subscriber as Subscriber>::Message, D::Input, C, D::Handler>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message, D::Context, St> + 'static,
    {
        let codec = self.codec.clone();
        self.mount_subscriber(source, def, codec);
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on its own source, decoding each
    /// element with the scope's default codec (set by
    /// [`with_broker_codec`](crate::runtime::RustStream::with_broker_codec)).
    pub fn include_batch<D>(&mut self, def: D)
    where
        D: BatchDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: crate::runtime::SliceHandler<D::Input, St> + 'static,
        St: Send + Sync + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch(source, def, codec);
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on an explicit subscription
    /// `source`, decoding each element with the scope's default codec.
    pub fn include_batch_on<S, D>(&mut self, source: S, def: D)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: crate::runtime::SliceHandler<D::Input, St> + 'static,
        St: Send + Sync + 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch(source, def, codec);
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on its own
    /// source, decoding each element with the scope's default codec and publishing the replies
    /// through `publisher`.
    pub fn include_batch_publishing<D, RP>(&mut self, def: D, publisher: RP)
    where
        D: BatchPublishingDef<State = St> + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
        St: Send + Sync + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch_publishing(source, def, codec, publisher);
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding each element with the scope's default codec.
    pub fn include_batch_publishing_on<S, D, RP>(&mut self, source: S, def: D, publisher: RP)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef<State = St> + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
        St: Send + Sync + 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch_publishing(source, def, codec, publisher);
    }

    /// Mounts a `#[subscriber(.., publish)]`-generated definition on its own source, decoding its
    /// input with the scope's default codec and sending the reply through `publisher`.
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
        St: Send + Sync + 'static,
        L: Layer<PublishingHandler<D, C, P, PC, PL>>,
        L::Handler: Handler<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
                (),
                St,
            > + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher);
    }

    /// Mounts a `#[subscriber(.., publish)]`-generated definition on an explicit subscription
    /// `source`, decoding its input with the scope's default codec.
    pub fn include_publishing_on<S, D, P, PC, PL>(
        &mut self,
        source: S,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
        St: Send + Sync + 'static,
        L: Layer<PublishingHandler<D, C, P, PC, PL>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message, (), St> + 'static,
    {
        let codec = self.codec.clone();
        self.mount_publishing(source, def, codec, publisher);
    }
}
