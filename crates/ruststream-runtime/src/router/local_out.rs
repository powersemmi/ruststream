//! A local router's mount for the `Out` form, and the commit its chain resolves through: the
//! [`form_out`](super::form_out) twin for a chain started with [`Router::local`].
//!
//! The chain is the same `RouterWith`, so `.out(marker, policy)` and `.codec(..)` read as they do
//! on the multi-thread runner; each slot pairs against a broker of the local family into a
//! [`LocalSlot`](crate::LocalSlot) when the subscription opens.

use crate::codec::Codec;
use crate::inject::InjectDef;
use crate::input::Materialize;
use crate::publish::{LowerOutTransforms, NarrowToUse, SlotStackUse};
use crate::settings::{DefMountCodec, MountsWith};
use crate::slot::{
    InitSlots, IntoSlotSource, LocalBindSlots, LocalHasSlots, NoReply, OutAttachment, OutSlot,
    SlotCodec, SlotPolicy, WithSource,
};
use crate::{
    LocalBroker, LocalConnected, LocalPublishPolicy, LocalSourceMessage, LocalSubscriptionSource,
};

use super::builder::Router;
use super::builders::{RouterCommit, RouterOut, RouterWith};
use super::local::LocalPipeline;
use super::mount::{InjectMount, MountCodec, RouterMount};
use super::{InjectedRouter, forms};

impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def> for forms::Out
where
    B: LocalBroker + 'static,
    Def: LocalHasSlots,
    Def::Markers: InitSlots,
{
    type Out = RouterOut<
        InjectMount,
        Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
        Def,
        <<Def as LocalHasSlots>::Markers as InitSlots>::Init,
    >;

    fn begin(
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
    ) -> Self::Out {
        RouterWith::new(
            def,
            (
                NoReply,
                <<Def as LocalHasSlots>::Markers as InitSlots>::init(),
            ),
            router,
        )
    }
}

/// The pipeline one slot's `.transform(..)` steps lower onto.
type SlotPipeline<Layers, Pipe> = <Layers as LowerOutTransforms<Pipe>>::Out;

/// The live publisher of the local family one slot's policy pairs into.
type SlotLive<Policy, B> = <Policy as LocalPublishPolicy<LocalConnected<B>>>::Live;

/// What the bound definition's input decodes with on a local router of broker `B`.
type BoundDecoder<Def, Input, RouteCodec, B> =
    DefMountCodec<Def, Input, RouteCodec, LocalConnected<B>>;

/// The bound-source tuple element of one slot, as on the multi-thread chain.
macro_rules! local_slot_source {
    ($attach:ident, $layers:ident, $enc:ident, $surface:ty, $pipe:ty) => {
        (
            SlotPolicy<$layers, $attach, SlotLive<$attach, B>>,
            <$enc as SlotCodec<$surface>>::Codec,
            SlotPipeline<$layers, $pipe>,
        )
    };
}

macro_rules! local_inject_out_commit {
    ($(($($marker:ident / $attach:ident / $layers:ident / $enc:ident),+))+) => {$(
        // A multi-thread chain's commit error lists the commits it could have used; the local
        // chain's twins are not among them.
        #[diagnostic::do_not_recommend]
        impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def, Bound, Extra,
             $($marker, $attach, $layers, $enc),+>
            RouterCommit<
                InjectMount,
                Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
                Def,
            >
            for (NoReply, ($(WithSource<OutAttachment<$marker, $attach, $layers, $enc>>,)+))
        where
            B: LocalBroker + 'static,
            RouteCodec: MountCodec,
            Pipe: Clone,
            $(
                $marker: OutSlot,
                $enc: SlotCodec<RouteCodec::Codec, Codec: Codec>,
                $attach: LocalPublishPolicy<LocalConnected<B>>,
                $layers: LowerOutTransforms<LocalPipeline<Pipe>>
                    + SlotStackUse<SlotLive<$attach, B>>,
                <$layers as SlotStackUse<SlotLive<$attach, B>>>::Destination: NarrowToUse<$attach>,
            )+
            Def: LocalBindSlots<
                LocalConnected<B>,
                ($(local_slot_source!(
                    $attach, $layers, $enc, RouteCodec::Codec, LocalPipeline<Pipe>
                ),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as InjectDef>::Input, RouteCodec, LocalConnected<B>>,
            Bound: InjectDef + 'static,
            Bound::Source: LocalSubscriptionSource<LocalConnected<B>> + 'static,
            Bound::Input: Materialize<
                BoundDecoder<Def, <Bound as InjectDef>::Input, RouteCodec, B>,
                LocalSourceMessage<B, Bound::Source>,
            >,
        {
            type Out = InjectedRouter<
                B,
                Bound::Source,
                Bound,
                BoundDecoder<Def, <Bound as InjectDef>::Input, RouteCodec, B>,
                Extra,
                RouteCodec,
                RouteLayers,
                LocalPipeline<Pipe>,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
            ) -> Self::Out {
                #[allow(non_snake_case)]
                let (_no_reply, ($($attach,)+)) = self;
                // The slots encode with the surface's codec unless one named its own; only the
                // decode side honours the definition's `.codec(..)` override.
                let codec = router.codec.mount_codec();
                let pipeline = router.pipeline.clone();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind_local(($(
                    $attach.into_source().wire(codec.clone(), pipeline.clone()),
                )+));
                let source = def.source();
                router.mount_inject(source, def, decode, extra)
            }
        }
    )+};
}

local_inject_out_commit! {
    (M0 / A0 / L0 / E0)
    (M0 / A0 / L0 / E0, M1 / A1 / L1 / E1)
    (M0 / A0 / L0 / E0, M1 / A1 / L1 / E1, M2 / A2 / L2 / E2)
}
