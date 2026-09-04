//! Router mounts for the forms carrying [`Out`](crate::runtime::Out) slots, and the commits their
//! chains resolve through.
//!
//! The attachment is the reply position next to a positional slot tuple, one element per marker,
//! starting all-unbound. Each `.out(marker, policy)` binds one position and `.build()` commits;
//! the commit impls exist only for fully-bound tuples, so a forgotten binding is a compile error
//! naming the slot. A handler with a single unnamed `Out` parameter names
//! [`DefaultSlot`](crate::runtime::DefaultSlot), the implicit marker of that position.
//!
//! Like every other form, the subscription source comes from the definition - here from the one
//! the bound slots instantiate, which is why the commit resolves it rather than the entry point.
//!
//! A slot's publish pipeline is fixed here, at the registration, because binding a slot
//! instantiates the definition and the entry's pipeline is part of that instantiated type. The
//! chain therefore carries the pipeline its slots publish through: [`PublishIdentity`] on a
//! router built on its own (the app that mounts it, and the middleware it carries, exist only
//! later), the app's own on a chain a [`BrokerScope`](crate::runtime::BrokerScope) drives.

// The typed default reply needs a default codec to encode with, so those pieces are gated the
// same way; the byte-reply default publishes bare bytes and needs only `DefaultPublish`.
use crate::{BatchSubscriber, Broker, Connected, DefaultPublish, SubscriptionSource};

use crate::runtime::SourceSubscriber;
use crate::runtime::batch_inject::BatchInjectDef;
use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::inject::InjectDef;
use crate::runtime::input::DecodeWith;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::ReplyWiring;
use crate::runtime::publish::{LowerOutTransforms, RawReplyWiring};
use crate::runtime::publishing::PublishingDef;
use crate::runtime::settings::{BatchSized, DefMountCodec, MountsWith};
use crate::runtime::slot::{
    BindSlots, HasSlots, InitSlots, IntoSlotSource, NoReply, OutAttachment, SlotCodec, WithSource,
};

use super::builder::Router;
use super::builders::{RouterCommit, RouterOut, RouterPublishingOut, RouterWith};
use super::mount::{
    BatchInjectMount, BatchPublishInjectMount, DefaultReply, InjectMount, MountCodec,
    PublishInjectMount, RawReplyInjectMount, RouterMount,
};
use super::{
    BatchInjectedRouter, BatchPublishingRouter, InjectedRouter, PublishingRouter, RawReplyRouter,
    forms,
};

// The five chain-producing entry points.

/// Implements [`RouterMount`] for a slot form: the chain starts with every position unbound, and
/// with the reply position present only where the handler declares a reply.
macro_rules! slot_form {
    ($($form:ty => $mount:ty, $reply:expr, $alias:ident),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def>
            RouterMount<Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def> for $form
        where
            B: Broker + 'static,
            Def: HasSlots,
            Def::Markers: InitSlots,
        {
            type Out = $alias<
                $mount,
                Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
                Def,
                <Def::Markers as InitSlots>::Init,
            >;

            fn begin(
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
            ) -> Self::Out {
                RouterWith::new(def, ($reply, <Def::Markers as InitSlots>::init()), router)
            }
        }
    )+};
}

slot_form! {
    forms::Out => InjectMount, NoReply, RouterOut,
    forms::BatchOut => BatchInjectMount, NoReply, RouterOut,
    forms::PublishingOut => PublishInjectMount, DefaultReply, RouterPublishingOut,
    forms::RawReplyOut => RawReplyInjectMount, DefaultReply, RouterPublishingOut,
    forms::BatchPublishingOut => BatchPublishInjectMount, DefaultReply, RouterPublishingOut,
}

// The slot commits, one macro per form family, for fully-bound tuples only. `Bound` / `Extra`
// name the definition's `BindSlots` outputs so the bounds read flat instead of through
// `<Def::Bound as ..>` projections.

/// The bound-source tuple element of one slot: the policy the runtime pairs, the codec the slot
/// encodes with (its own when the chain named one, else the surface's) and the pipeline it
/// publishes through (its transforms lowered onto the chain's own).
macro_rules! slot_source {
    ($attach:ident, $layers:ident, $enc:ident, $surface:ty, $pipe:ty) => {
        (
            $attach,
            <$enc as SlotCodec<$surface>>::Codec,
            <$layers as LowerOutTransforms<$pipe>>::Out,
        )
    };
}

macro_rules! impl_inject_out_commit {
    ($(($($attach:ident / $layers:ident / $enc:ident),+))+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, Bound, Extra,
             $($attach, $layers, $enc),+>
            RouterCommit<InjectMount, Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def>
            for (NoReply, ($(WithSource<OutAttachment<$attach, $layers, $enc>>,)+))
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            RoutePipe: Clone,
            $(
                $enc: SlotCodec<RouteCodec::Codec>,
                $layers: LowerOutTransforms<RoutePipe>,
            )+
            Def: BindSlots<
                Connected<B>,
                ($(slot_source!($attach, $layers, $enc, RouteCodec::Codec, RoutePipe),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as InjectDef>::Input, RouteCodec>,
            Bound: InjectDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<DefMountCodec<Def, <Bound as InjectDef>::Input, RouteCodec>>,
        {
            type Out = InjectedRouter<
                B,
                Bound::Source,
                Bound,
                DefMountCodec<Def, <Bound as InjectDef>::Input, RouteCodec>,
                Extra,
                RouteCodec,
                RouteLayers,
                RoutePipe,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
            ) -> Self::Out {
                #[allow(non_snake_case)]
                let (_no_reply, ($($attach,)+)) = self;
                // The slots encode with the surface's codec unless one named its own; only the
                // decode side honours the definition's `.codec(..)` override.
                let codec = router.codec.mount_codec();
                let pipeline = router.pipeline.clone();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), pipeline.clone()),
                )+));
                let source = def.source();
                router.mount_inject(source, def, decode, extra)
            }
        }

        impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, Bound, Extra,
             $($attach, $layers, $enc),+>
            RouterCommit<
                BatchInjectMount,
                Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
                Def,
            >
            for (NoReply, ($(WithSource<OutAttachment<$attach, $layers, $enc>>,)+))
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            RoutePipe: Clone,
            $(
                $enc: SlotCodec<RouteCodec::Codec>,
                $layers: LowerOutTransforms<RoutePipe>,
            )+
            Def: BindSlots<
                Connected<B>,
                ($(slot_source!($attach, $layers, $enc, RouteCodec::Codec, RoutePipe),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as BatchInjectDef>::Input, RouteCodec>,
            Bound: BatchInjectDef + BatchSized + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Send + 'static,
            Bound::Input:
                DecodeWith<DefMountCodec<Def, <Bound as BatchInjectDef>::Input, RouteCodec>>,
        {
            type Out = BatchInjectedRouter<
                B,
                Bound::Source,
                Bound,
                DefMountCodec<Def, <Bound as BatchInjectDef>::Input, RouteCodec>,
                Extra,
                RouteCodec,
                RouteLayers,
                RoutePipe,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
            ) -> Self::Out {
                #[allow(non_snake_case)]
                let (_no_reply, ($($attach,)+)) = self;
                // See the single-message commit: surface codec for the slots, override-aware
                // codec for the decode.
                let codec = router.codec.mount_codec();
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

impl_inject_out_commit! {
    (A0 / L0 / E0)
    (A0 / L0 / E0, A1 / L1 / E1)
    (A0 / L0 / E0, A1 / L1 / E1, A2 / L2 / E2)
}

macro_rules! impl_publishing_out_commit {
    ($(($($attach:ident / $layers:ident / $enc:ident),+))+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, Policy, Bound, Extra,
             $($attach, $layers, $enc),+>
            RouterCommit<
                PublishInjectMount,
                Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
                Def,
            >
            for (
                WithSource<Policy>,
                ($(WithSource<OutAttachment<$attach, $layers, $enc>>,)+),
            )
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            RoutePipe: Clone,
            $(
                $enc: SlotCodec<RouteCodec::Codec>,
                $layers: LowerOutTransforms<RoutePipe>,
            )+
            Def: BindSlots<
                Connected<B>,
                ($(slot_source!($attach, $layers, $enc, RouteCodec::Codec, RoutePipe),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as PublishingDef>::Input, RouteCodec>,
            Bound: PublishingDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Send + 'static,
            Bound::Input:
                DecodeWith<DefMountCodec<Def, <Bound as PublishingDef>::Input, RouteCodec>>,
            Policy: 'static,
        {
            type Out = PublishingRouter<
                B,
                Bound::Source,
                Bound,
                DefMountCodec<Def, <Bound as PublishingDef>::Input, RouteCodec>,
                Policy,
                Extra,
                RouteCodec,
                RouteLayers,
                RoutePipe,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                // Surface codec for the slots, override-aware codec for the decode.
                let codec = router.codec.mount_codec();
                let pipeline = router.pipeline.clone();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), pipeline.clone()),
                )+));
                let source = def.source();
                router.mount_publishing_source(source, def, decode, reply.into_source(), extra)
            }
        }

        impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, Policy, Bound, Extra,
             $($attach, $layers, $enc),+>
            RouterCommit<
                RawReplyInjectMount,
                Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
                Def,
            >
            for (
                WithSource<Policy>,
                ($(WithSource<OutAttachment<$attach, $layers, $enc>>,)+),
            )
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            RoutePipe: Clone,
            $(
                $enc: SlotCodec<RouteCodec::Codec>,
                $layers: LowerOutTransforms<RoutePipe>,
            )+
            Def: BindSlots<
                Connected<B>,
                ($(slot_source!($attach, $layers, $enc, RouteCodec::Codec, RoutePipe),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as PublishingDef>::Input, RouteCodec>,
            Bound: PublishingDef + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Send + 'static,
            Bound::Input:
                DecodeWith<DefMountCodec<Def, <Bound as PublishingDef>::Input, RouteCodec>>,
            Policy: 'static,
        {
            type Out = RawReplyRouter<
                B,
                Bound::Source,
                Bound,
                DefMountCodec<Def, <Bound as PublishingDef>::Input, RouteCodec>,
                Policy,
                Extra,
                RouteCodec,
                RouteLayers,
                RoutePipe,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                // Surface codec for the slots, override-aware codec for the decode.
                let codec = router.codec.mount_codec();
                let pipeline = router.pipeline.clone();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), pipeline.clone()),
                )+));
                let source = def.source();
                router.mount_raw_reply_source(source, def, decode, reply.into_source(), extra)
            }
        }

        impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, Policy, Bound, Extra,
             $($attach, $layers, $enc),+>
            RouterCommit<
                BatchPublishInjectMount,
                Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
                Def,
            >
            for (
                WithSource<Policy>,
                ($(WithSource<OutAttachment<$attach, $layers, $enc>>,)+),
            )
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            RoutePipe: Clone,
            $(
                $enc: SlotCodec<RouteCodec::Codec>,
                $layers: LowerOutTransforms<RoutePipe>,
            )+
            Def: BindSlots<
                Connected<B>,
                ($(slot_source!($attach, $layers, $enc, RouteCodec::Codec, RoutePipe),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as BatchPublishingDef>::Input, RouteCodec>,
            Bound: BatchPublishingDef + BatchSized + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Send + 'static,
            Bound::Input:
                DecodeWith<DefMountCodec<Def, <Bound as BatchPublishingDef>::Input, RouteCodec>>,
            Policy: 'static,
        {
            type Out = BatchPublishingRouter<
                B,
                Bound::Source,
                Bound,
                DefMountCodec<Def, <Bound as BatchPublishingDef>::Input, RouteCodec>,
                Policy,
                Extra,
                RouteCodec,
                RouteLayers,
                RoutePipe,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                // Surface codec for the slots, override-aware codec for the decode.
                let codec = router.codec.mount_codec();
                let pipeline = router.pipeline.clone();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), pipeline.clone()),
                )+));
                let source = def.source();
                router.mount_batch_publishing_source(
                    source,
                    def,
                    decode,
                    reply.into_source(),
                    extra,
                )
            }
        }
    )+};
}

impl_publishing_out_commit! {
    (A0 / L0 / E0)
    (A0 / L0 / E0, A1 / L1 / E1)
    (A0 / L0 / E0, A1 / L1 / E1, A2 / L2 / E2)
}

// The defaulted reply sides, committed as if `.out(Reply, ..)` had been chained with the
// broker's own policy.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
macro_rules! impl_default_typed_reply_slot_commit {
    ($($mount:ident),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, Slots>
            RouterCommit<$mount, Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def>
            for (DefaultReply, Slots)
        where
            B: Broker + 'static,
            B::Connected: DefaultPublish,
            (
                WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>>,
                Slots,
            ): RouterCommit<$mount, Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def>,
        {
            type Out = <(
                WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>>,
                Slots,
            ) as RouterCommit<
                $mount,
                Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
                Def,
            >>::Out;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
            ) -> Self::Out {
                (
                    WithSource::new(ReplyWiring::new(
                        <B::Connected as DefaultPublish>::Policy::default(),
                    )),
                    self.1,
                )
                    .commit(def, router)
            }
        }
    )+};
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl_default_typed_reply_slot_commit!(PublishInjectMount, BatchPublishInjectMount);

// The serialized wire's default next to the slot tuple: the broker's plain policy taken bare,
// with no codec demand, keyed by the raw mount token.
impl<B, Routes, RouteCodec, RouteLayers, RoutePipe, Def, Slots>
    RouterCommit<RawReplyInjectMount, Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>, Def>
    for (DefaultReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (
        WithSource<RawReplyWiring<<B::Connected as DefaultPublish>::Policy>>,
        Slots,
    ): RouterCommit<
            RawReplyInjectMount,
            Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
            Def,
        >,
{
    type Out = <(
        WithSource<RawReplyWiring<<B::Connected as DefaultPublish>::Policy>>,
        Slots,
    ) as RouterCommit<
        RawReplyInjectMount,
        Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
        Def,
    >>::Out;

    fn commit(
        self,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>,
    ) -> Self::Out {
        (
            WithSource::new(RawReplyWiring::new(
                <B::Connected as DefaultPublish>::Policy::default(),
            )),
            self.1,
        )
            .commit(def, router)
    }
}
