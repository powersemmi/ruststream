//! A local router's mounts for the `Out` forms of a single message and of a batch, and the commits
//! their chains resolve through: the [`form_out`](super::form_out) forms for a chain started with
//! [`Router::local`].
//!
//! The chain is the same `RouterWith`, so `.out(marker, policy)` and `.codec(..)` read as they do
//! on the multi-thread runner; each slot pairs against a broker of the local family into a
//! [`Slot`](crate::Slot) when the subscription opens.

use ruststream_core::MaybeSend;

use crate::batch_inject::BatchInjectDef;
use crate::inject::InjectDef;
use crate::input::{DecodeWith, Materialize};
#[cfg(not(any(feature = "json", feature = "cbor", feature = "msgpack")))]
use crate::publish::UnnamedCodec;
use crate::publish::{LowerOutTransforms, NarrowToUse, SlotStackUse};
use crate::settings::{BatchSized, DefMountCodec, MountsWith};
use crate::slot::{
    BindSlots, HasSlots, InitSlots, IntoSlotSource, NoReply, OutAttachment, OutSlot, SlotCodec,
    SlotPolicy, WithSource,
};
use crate::{
    BatchSubscriber, Broker, Connected, LocalSourceMessage, PublishPolicy, SourceSubscriber,
    SubscriptionSource,
};

use super::builder::Router;
use super::builders::{RouterCommit, RouterOut, RouterWith};
use super::local::LocalPipeline;
use super::mount::{BatchInjectMount, InjectMount, MountCodec, RouterMount};
use super::{BatchInjectedRouter, InjectedRouter, forms};

/// Implements [`RouterMount`] for a slot form on a local router: the chain starts with every
/// position unbound and no reply position, since a handler on the embedded runner that takes `Out`
/// slots does not reply.
macro_rules! local_out_form {
    ($($form:ty => $mount:ty),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def>
            RouterMount<Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def>
            for $form
        where
            B: Broker + 'static,
            Def: HasSlots,
            Def::Markers: InitSlots,
        {
            type Out = RouterOut<
                $mount,
                Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
                Def,
                <<Def as HasSlots>::Markers as InitSlots>::Init,
            >;

            fn begin(
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
            ) -> Self::Out {
                RouterWith::new(
                    def,
                    (NoReply, <<Def as HasSlots>::Markers as InitSlots>::init()),
                    router,
                )
            }
        }
    )+};
}

local_out_form! {
    forms::Out => InjectMount,
    forms::BatchOut => BatchInjectMount,
}

/// The codec a local chain's slots encode with when none names its own: the surface's own, the
/// crate default for a surface that names none, and no codec at all in a build without one, where
/// a slot publishes the types that carry their own bytes.
#[doc(hidden)]
pub trait SlotSurface {
    /// The resolved surface codec.
    type Codec: Clone;

    /// Produces it, once per registration.
    fn slot_codec(&self) -> Self::Codec;
}

impl<Surface: MountCodec> SlotSurface for Surface {
    type Codec = Surface::Codec;

    fn slot_codec(&self) -> Self::Codec {
        self.mount_codec()
    }
}

// A device build often has no serde codec: its slots publish what carries its own bytes, and a
// value that needs encoding is refused where it is published, naming the missing codec.
#[cfg(not(any(feature = "json", feature = "cbor", feature = "msgpack")))]
impl SlotSurface for () {
    type Codec = UnnamedCodec;

    fn slot_codec(&self) -> UnnamedCodec {
        UnnamedCodec::new()
    }
}

/// The pipeline one slot's `.transform(..)` steps lower onto.
type SlotPipeline<Layers, Pipe> = <Layers as LowerOutTransforms<Pipe>>::Out;

/// The live publisher of the local family one slot's policy pairs into.
type SlotLive<Policy, B> = <Policy as PublishPolicy<Connected<B>>>::Live;

/// What the bound definition's input decodes with on a local router of broker `B`.
type BoundDecoder<Def, Input, RouteCodec, B> = DefMountCodec<Def, Input, RouteCodec, Connected<B>>;

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
        // chain's commits are not among them.
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
            B: Broker + 'static,
            RouteCodec: SlotSurface,
            Pipe: Clone,
            $(
                $marker: OutSlot,
                $enc: SlotCodec<RouteCodec::Codec>,
                $attach: PublishPolicy<Connected<B>>,
                $layers: LowerOutTransforms<LocalPipeline<Pipe>>
                    + SlotStackUse<SlotLive<$attach, B>>,
                <$layers as SlotStackUse<SlotLive<$attach, B>>>::Destination: NarrowToUse<$attach>,
            )+
            Def: BindSlots<
                Connected<B>,
                ($(local_slot_source!(
                    $attach, $layers, $enc, RouteCodec::Codec, LocalPipeline<Pipe>
                ),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as InjectDef>::Input, RouteCodec, Connected<B>>,
            Bound: InjectDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + 'static,
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
                let codec = router.codec.slot_codec();
                let pipeline = router.pipeline.clone();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), pipeline.clone()),
                )+));
                let source = def.source();
                router.mount_inject(source, def, decode, extra)
            }
        }

        #[diagnostic::do_not_recommend]
        impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def, Bound, Extra,
             $($marker, $attach, $layers, $enc),+>
            RouterCommit<
                BatchInjectMount,
                Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
                Def,
            >
            for (NoReply, ($(WithSource<OutAttachment<$marker, $attach, $layers, $enc>>,)+))
        where
            B: Broker + 'static,
            RouteCodec: SlotSurface,
            Pipe: Clone,
            $(
                $marker: OutSlot,
                $enc: SlotCodec<RouteCodec::Codec>,
                $attach: PublishPolicy<Connected<B>>,
                $layers: LowerOutTransforms<LocalPipeline<Pipe>>
                    + SlotStackUse<SlotLive<$attach, B>>,
                <$layers as SlotStackUse<SlotLive<$attach, B>>>::Destination: NarrowToUse<$attach>,
            )+
            Def: BindSlots<
                Connected<B>,
                ($(local_slot_source!(
                    $attach, $layers, $enc, RouteCodec::Codec, LocalPipeline<Pipe>
                ),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as BatchInjectDef>::Input, RouteCodec, Connected<B>>,
            Bound: BatchInjectDef + BatchSized + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + MaybeSend + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + MaybeSend + 'static,
            Bound::Input:
                DecodeWith<BoundDecoder<Def, <Bound as BatchInjectDef>::Input, RouteCodec, B>>,
        {
            type Out = BatchInjectedRouter<
                B,
                Bound::Source,
                Bound,
                BoundDecoder<Def, <Bound as BatchInjectDef>::Input, RouteCodec, B>,
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
                // As the single-message commit: the surface's codec for the slots, the
                // definition's override for the decode.
                let codec = router.codec.slot_codec();
                let pipeline = router.pipeline.clone();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), pipeline.clone()),
                )+));
                let source = def.source();
                router.mount_batch_inject(source, def, decode, extra)
            }
        }
    )+};
}

local_inject_out_commit! {
    (M0 / A0 / L0 / E0)
    (M0 / A0 / L0 / E0, M1 / A1 / L1 / E1)
    (M0 / A0 / L0 / E0, M1 / A1 / L1 / E1, M2 / A2 / L2 / E2)
}
