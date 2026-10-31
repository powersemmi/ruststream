//! Router mounts for the forms that need no attachment: plain, raw, and the two batch shapes.
//!
//! Each resolves the subscription source from the definition itself, which is where a source
//! belongs, and the decode codec from the definition's settings against the chain (the
//! [`codec`](crate::SubscriberBuilder::codec) override wins, else the chain's codec or
//! the default applies).

use crate::{BatchSubscriber, Broker, Connected, SendSource, SubscriptionSource};

use crate::batch::{BatchDef, SourceCarriesBatch};
use crate::input::{Carried, DecodeWith, Materialize, Provided};
use crate::publish::PublishPipeline;
use crate::settings::{BatchSized, DefMountCodec, MountsWith};
use crate::subscriber_def::SubscriberDef;
use crate::{SourceMessage, SourceSubscriber};

use super::builder::Router;
use super::head::{PoisonAt, ScopeOut};
use super::mount::RouterMount;
use super::{
    IncludedBatchRouter, IncludedCarriedBatchRouter, IncludedRawBatchRouter, IncludedRouter, forms,
};

/// What a definition's single-delivery input decodes with when it mounts on a router of broker
/// `B` with the surface codec `RouteCodec`.
type SoloDecoder<Def, B, RouteCodec> =
    DefMountCodec<Def, <Def as SubscriberDef>::Input, RouteCodec, Connected<B>>;

/// The witness a single-delivery mount reads its output through: the copies its source declares,
/// which exist only where the source opens a subscription on `B`.
type SourceCopies<B, Def> =
    <<Def as SubscriberDef>::Source as SubscriptionSource<Connected<B>>>::Copies;

/// The witness a batch mount reads its output through: the batch its subscription delivers,
/// which exists only where the subscription delivers batches.
type SourceBatchOf<B, Source> = <SourceSubscriber<B, Source> as BatchSubscriber>::Batch;

/// The witness a carried batch mount reads its output through: the value its subscription's
/// batches lend, which exists only where they lend `T`.
type LentItem<B, Source, T> = <Source as SourceCarriesBatch<T, Connected<B>>>::Item;

/// What a plain single-delivery mount grows the router into.
type SoloRouter<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def> = IncludedRouter<
    B,
    <Def as SubscriberDef>::Source,
    Def,
    SoloDecoder<Def, B, RouteCodec>,
    RouteCodec,
    RouteLayers,
    RoutePipe,
    Routes,
>;

/// What a decoded batch mount grows the router into.
type BatchRouter<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def> = IncludedBatchRouter<
    B,
    <Def as BatchDef>::Source,
    Def,
    DefMountCodec<Def, <Def as BatchDef>::Input, RouteCodec, Connected<B>>,
    RouteCodec,
    RouteLayers,
    RoutePipe,
    Routes,
>;

impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def> for forms::Subscribing
where
    B: Broker + 'static,
    Def: SubscriberDef + MountsWith<<Def as SubscriberDef>::Input, RouteCodec, Connected<B>>,
    Def::Source: SendSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Input: Materialize<SoloDecoder<Def, B, RouteCodec>, SourceMessage<B, Def::Source>>,
    Def::Handler: 'static,
    RoutePipe: PublishPipeline,
    Routes: PoisonAt,
{
    // Read through the source's own projection: on a scope's chain, a source that opens no
    // subscription here leaves the output with no type, so the error is reported once, here.
    type Out = ScopeOut<
        Routes,
        SourceCopies<B, Def>,
        SoloRouter<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def>,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let codec = def.mounted_codec(&router.codec);
        let source = def.source();
        Routes::poison(router.mount_subscriber(source, def, codec))
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
    Def::Source: SendSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: Send + 'static,
    Def::Handler: 'static,
    F: Send + Sync + 'static,
    RoutePipe: PublishPipeline,
    Routes: PoisonAt,
{
    type Out = ScopeOut<
        Routes,
        SourceCopies<B, Def>,
        IncludedRouter<B, Def::Source, Def, (), RouteCodec, RouteLayers, RoutePipe, Routes>,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let source = def.source();
        Routes::poison(router.mount_subscriber(source, def, ()))
    }
}

impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def> for forms::Batch
where
    B: Broker + 'static,
    Def: BatchDef + BatchSized + MountsWith<<Def as BatchDef>::Input, RouteCodec, Connected<B>>,
    Def::Source: SendSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as BatchDef>::Input, RouteCodec, Connected<B>>>,
    Def::Handler: 'static,
    Routes: PoisonAt,
{
    // Read through the batching projection, so a subscription that delivers no batches fails at
    // the mount alone (see `forms::Subscribing`).
    type Out = ScopeOut<
        Routes,
        SourceBatchOf<B, Def::Source>,
        BatchRouter<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def>,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let codec = def.mounted_codec(&router.codec);
        let source = def.source();
        Routes::poison(router.mount_batch(source, def, codec))
    }
}

// A self-deserializing batch decodes nothing, so the chain's codec parameter stays
// unconstrained here too.
impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, F>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def> for forms::RawBatch
where
    B: Broker + 'static,
    Def: BatchDef<Input = Provided<F>> + BatchSized,
    Def::Source: SendSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Handler: 'static,
    F: Send + Sync + 'static,
    RoutePipe: PublishPipeline,
    Routes: PoisonAt,
{
    type Out = ScopeOut<
        Routes,
        SourceBatchOf<B, Def::Source>,
        IncludedRawBatchRouter<B, Def::Source, Def, F, RouteCodec, RouteLayers, RoutePipe, Routes>,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let source = def.source();
        Routes::poison(router.mount_raw_batch(source, def))
    }
}

// A carried batch decodes nothing: the subscription's batches lend their values, so no codec is
// resolved, and the subscription is checked for the values the body reads.
impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, T>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def> for forms::CarriedBatch
where
    B: Broker + 'static,
    Def: BatchDef<Input = Carried<T>> + BatchSized,
    Def::Source: SendSource<Connected<B>> + SourceCarriesBatch<T, Connected<B>> + Send + 'static,
    SourceSubscriber<B, Def::Source>: BatchSubscriber + Send + 'static,
    Def::Handler: 'static,
    T: Clone + Send + Sync + 'static,
    Routes: PoisonAt,
{
    // Read through the lent value's projection, so a subscription whose batches lend nothing
    // fails at the mount alone (see `forms::Subscribing`).
    type Out = ScopeOut<
        Routes,
        LentItem<B, Def::Source, T>,
        IncludedCarriedBatchRouter<
            B,
            Def::Source,
            Def,
            T,
            RouteCodec,
            RouteLayers,
            RoutePipe,
            Routes,
        >,
    >;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>) -> Self::Out {
        let source = def.source();
        Routes::poison(router.mount_carried_batch(source, def))
    }
}
