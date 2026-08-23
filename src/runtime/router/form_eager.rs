//! Router mounts for the forms that need no attachment: plain, raw, attachment-free injections
//! and the two batch shapes.
//!
//! Each resolves the subscription source from the definition itself, which is where a source
//! belongs: `#[subscriber(..)]` takes the broker's own source expression, builder chain included.

use crate::{BatchSubscriber, Broker, Connected, SubscriptionSource};

use crate::runtime::SourceSubscriber;
use crate::runtime::batch::{BatchDef, BatchWithHeadersDef};
use crate::runtime::batch_inject::BatchInjectDef;
use crate::runtime::inject::InjectDef;
use crate::runtime::input::{DecodeWith, RawBytes};
use crate::runtime::subscriber_def::SubscriberDef;

use super::builder::Router;
use super::mount::{MountCodec, RouterMount};
use super::{
    BatchInjectedRouter, IncludedBatchRouter, IncludedBatchWithHeadersRouter,
    IncludedRawBatchRouter, IncludedRouter, InjectedRouter, forms,
};

impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::Subscribing
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Def: SubscriberDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Def::Handler: 'static,
{
    type Out =
        IncludedRouter<B, Def::Source, Def, RouteCodec::Codec, RouteCodec, RouteLayers, Routes>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let codec = router.codec.mount_codec();
        let source = def.source();
        router.mount_subscriber(source, def, codec)
    }
}

// The byte input kind decodes with `()`, so the chain's codec parameter is left unconstrained
// and a raw mount works without any codec feature enabled.
impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::RawSubscribing
where
    B: Broker + 'static,
    Def: SubscriberDef<Input = RawBytes>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Handler: 'static,
{
    type Out = IncludedRouter<B, Def::Source, Def, (), RouteCodec, RouteLayers, Routes>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let source = def.source();
        router.mount_subscriber(source, def, ())
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::Seek
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Def: InjectDef + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
{
    type Out = InjectedRouter<
        B,
        Def::Source,
        Def,
        RouteCodec::Codec,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let codec = router.codec.mount_codec();
        let source = def.source();
        // A `Seek` parameter resolves off the subscription itself, so its attachment is the unit
        // padding rather than anything named at the include site.
        router.mount_inject(source, def, codec, ((),))
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::Batch
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Def: BatchDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Def::Handler: 'static,
{
    type Out = IncludedBatchRouter<
        B,
        Def::Source,
        Def,
        RouteCodec::Codec,
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let codec = router.codec.mount_codec();
        let source = def.source();
        router.mount_batch(source, def, codec)
    }
}

// A raw batch decodes nothing, so the chain's codec parameter stays unconstrained here too.
impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::RawBatch
where
    B: Broker + 'static,
    Def: BatchDef<Input = RawBytes>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Handler: 'static,
{
    type Out = IncludedRawBatchRouter<B, Def::Source, Def, RouteCodec, RouteLayers, Routes>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let source = def.source();
        router.mount_raw_batch(source, def)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::BatchWithHeaders
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Def: BatchWithHeadersDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
    Def::Handler: 'static,
{
    type Out = IncludedBatchWithHeadersRouter<
        B,
        Def::Source,
        Def,
        RouteCodec::Codec,
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let codec = router.codec.mount_codec();
        let source = def.source();
        router.mount_batch_with_headers(source, def, codec)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def> RouterMount<B, Routes, RouteCodec, RouteLayers, Def>
    for forms::BatchSeek
where
    B: Broker + 'static,
    RouteCodec: MountCodec,
    Def: BatchInjectDef + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<RouteCodec::Codec>,
{
    type Out = BatchInjectedRouter<
        B,
        Def::Source,
        Def,
        RouteCodec::Codec,
        ((),),
        RouteCodec,
        RouteLayers,
        Routes,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let codec = router.codec.mount_codec();
        let source = def.source();
        router.mount_batch_inject(source, def, codec, ((),))
    }
}
