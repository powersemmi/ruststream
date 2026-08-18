//! Router mounts for the forms that need no attachment: plain, raw, attachment-free injections
//! and the two batch shapes.
//!
//! Each is a source-override mount ([`RouterMountOn`]); the definition's own source flows into
//! it through the [`RouterMount`](super::RouterMount) impls in
//! [`mount`](super::mount), so both entry points share one body.

use crate::{BatchSubscriber, Broker, Connected, SubscriptionSource};

use crate::runtime::batch::{BatchDef, BatchWithHeadersDef};
use crate::runtime::batch_inject::BatchInjectDef;
use crate::runtime::inject::InjectDef;
use crate::runtime::input::{DecodeWith, RawBytes};
use crate::runtime::subscriber_def::SubscriberDef;

use super::builder::Router;
use super::mount::{MountCodec, RouterMountOn};
use super::{
    BatchInjectedRouter, IncludedBatchRouter, IncludedBatchWithHeadersRouter, IncludedRouter,
    InjectedRouter, forms,
};

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::Subscribing
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Send + 'static,
    Def: SubscriberDef,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Def::Handler: 'static,
{
    type Out = IncludedRouter<B, Source, Def, RouteCodec::Codec, RouteCodec, RouteLayers, Routes>;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        let codec = router.codec.mount_codec();
        router.mount_subscriber(source, def, codec)
    }
}

// The byte input kind decodes with `()`, so the chain's codec parameter is left unconstrained
// and a raw mount works without any codec feature enabled.
impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::RawSubscribing
where
    B: Broker + 'static,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Send + 'static,
    Def: SubscriberDef<Input = RawBytes>,
    Def::Handler: 'static,
{
    type Out = IncludedRouter<B, Source, Def, (), RouteCodec, RouteLayers, Routes>;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        router.mount_subscriber(source, def, ())
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::Seek
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Send + 'static,
    Def: InjectDef + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
{
    type Out =
        InjectedRouter<B, Source, Def, RouteCodec::Codec, ((),), RouteCodec, RouteLayers, Routes>;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        let codec = router.codec.mount_codec();
        // A `Seek` parameter resolves off the subscription itself, so its attachment is the unit
        // padding rather than anything named at the include site.
        router.mount_inject(source, def, codec, ((),))
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::Batch
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: BatchSubscriber + Send + 'static,
    Def: BatchDef,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Def::Handler: 'static,
{
    type Out =
        IncludedBatchRouter<B, Source, Def, RouteCodec::Codec, RouteCodec, RouteLayers, Routes>;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        let codec = router.codec.mount_codec();
        router.mount_batch(source, def, codec)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::BatchWithHeaders
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: BatchSubscriber + Send + 'static,
    Def: BatchWithHeadersDef,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Def::Handler: 'static,
{
    type Out = IncludedBatchWithHeadersRouter<
        B,
        Source,
        Def,
        RouteCodec::Codec,
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        let codec = router.codec.mount_codec();
        router.mount_batch_with_headers(source, def, codec)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Source, Def>
    RouterMountOn<B, Routes, RouteCodec, RouteLayers, Source, Def> for forms::BatchSeek
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: BatchSubscriber + Send + 'static,
    Def: BatchInjectDef + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
{
    type Out = BatchInjectedRouter<
        B,
        Source,
        Def,
        RouteCodec::Codec,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out {
        let codec = router.codec.mount_codec();
        router.mount_batch_inject(source, def, codec, ((),))
    }
}
