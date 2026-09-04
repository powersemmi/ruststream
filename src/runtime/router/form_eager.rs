//! Router mounts for the forms that need no attachment: plain, raw, and the two batch shapes.
//!
//! Each resolves the subscription source from the definition itself, which is where a source
//! belongs, and the decode codec from the definition's settings against the chain (the
//! [`codec`](crate::runtime::SubscriberBuilder::codec) override wins, else the chain's codec or
//! the default applies).

use crate::{BatchSubscriber, Broker, Connected, SubscriptionSource};

use crate::runtime::SourceSubscriber;
use crate::runtime::batch::BatchDef;
use crate::runtime::input::{DecodeWith, Provided};
use crate::runtime::settings::{BatchSized, DefMountCodec, MountsWith};
use crate::runtime::subscriber_def::SubscriberDef;

use super::builder::Router;
use super::mount::RouterMount;
use super::{IncludedBatchRouter, IncludedRawBatchRouter, IncludedRouter, forms};

impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def> for forms::Subscribing
where
    B: Broker + 'static,
    Def: SubscriberDef + MountsWith<<Def as SubscriberDef>::Input, RouteCodec>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as SubscriberDef>::Input, RouteCodec>>,
    Def::Handler: 'static,
{
    type Out = IncludedRouter<
        B,
        Def::Source,
        Def,
        DefMountCodec<Def, <Def as SubscriberDef>::Input, RouteCodec>,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let codec = def.mounted_codec(&router.codec);
        let source = def.source();
        router.mount_subscriber(source, def, codec)
    }
}

// The self-deserializing input kind decodes with `()`, so the chain's codec parameter is left
// unconstrained and the mount works without any codec feature enabled.
impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, F>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def>
    for forms::RawSubscribing
where
    B: Broker + 'static,
    Def: SubscriberDef<Input = Provided<F>>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Handler: 'static,
    F: Send + Sync + 'static,
{
    type Out = IncludedRouter<B, Def::Source, Def, (), RouteCodec, RouteLayers, RoutePipe, Routes>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let source = def.source();
        router.mount_subscriber(source, def, ())
    }
}

impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def> for forms::Batch
where
    B: Broker + 'static,
    Def: BatchDef + BatchSized + MountsWith<<Def as BatchDef>::Input, RouteCodec>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as BatchDef>::Input, RouteCodec>>,
    Def::Handler: 'static,
{
    type Out = IncludedBatchRouter<
        B,
        Def::Source,
        Def,
        DefMountCodec<Def, <Def as BatchDef>::Input, RouteCodec>,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let codec = def.mounted_codec(&router.codec);
        let source = def.source();
        router.mount_batch(source, def, codec)
    }
}

// A self-deserializing batch decodes nothing, so the chain's codec parameter stays
// unconstrained here too.
impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, F>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def> for forms::RawBatch
where
    B: Broker + 'static,
    Def: BatchDef<Input = Provided<F>> + BatchSized,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Handler: 'static,
    F: Send + Sync + 'static,
{
    type Out =
        IncludedRawBatchRouter<B, Def::Source, Def, F, RouteCodec, RouteLayers, RoutePipe, Routes>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let source = def.source();
        router.mount_raw_batch(source, def)
    }
}
