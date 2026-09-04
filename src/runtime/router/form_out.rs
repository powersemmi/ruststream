//! Router mounts for the forms carrying [`Out`](crate::runtime::Out) slots, and the slot commits
//! their builders resolve through.
//!
//! The attachment is a positional slot tuple, one element per marker, starting all-unbound. Each
//! `.out(marker, policy)` binds one position and `.build()` commits; the commit impls exist only
//! for fully-bound tuples, so a forgotten binding is a compile error naming the slot. A handler
//! with a single slot uses the `.publisher(policy)` shorthand, which binds and commits in one
//! call.
//!
//! Like every other form, the subscription source comes from the definition - here from the one
//! the bound slots instantiate, which is why the commit resolves it rather than the entry point.
//!
//! A slot's publish pipeline is fixed here, at the registration, so a router's slots carry their
//! own `.transform(..)` steps over [`PublishIdentity`] and not the app-wide publish middleware:
//! binding a slot instantiates the definition, and the entry's pipeline is part of that
//! instantiated type, while the app a router is mounted into (and the middleware it carries)
//! exists only later. A handler whose slot publishes have to travel the app-wide
//! [`publish_layer`](crate::runtime::RustStream::publish_layer) chain mounts on the broker scope
//! (`b.include(..)`), which knows it.

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
use crate::runtime::publish::{LowerOutTransforms, PublishIdentity};
use crate::runtime::publishing::PublishingDef;
use crate::runtime::settings::{DefMountCodec, MountsWith, PageSized};
use crate::runtime::slot::{
    BindSlots, HasSlots, InitSlots, IntoSlotSource, OutAttachment, WithSource,
};

use super::builder::Router;
use super::builders::{
    RouterBatchOut, RouterBatchPublishingOut, RouterOut, RouterPublishingOut, RouterRawReplyOut,
    RouterSlotCommit, RouterSlots, RouterSlotsWithReply,
};
use super::mount::{
    BatchInjectMount, BatchPublishInjectMount, DefaultReply, InjectMount, MountCodec,
    PublishInjectMount, RawReplyInjectMount, RouterMount,
};
use super::{
    BatchInjectedRouter, BatchPublishingRouter, InjectedRouter, PublishingRouter, RawReplyRouter,
    forms,
};

// ---------------------------------------------------------------------------------------------
// The five builder-producing entry points.

/// Implements [`RouterMount`] for a slot form: the builder starts with every position unbound.
macro_rules! slot_form {
    ($($form:ty => $builder:ident as $alias:ident),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def>
            RouterMount<B, Routes, RouteCodec, RouteLayers, Def> for $form
        where
            B: Broker + 'static,
            Def: HasSlots,
            Def::Markers: InitSlots,
        {
            type Out = $alias<
                B,
                Routes,
                RouteCodec,
                RouteLayers,
                Def,
                <Def::Markers as InitSlots>::Init,
            >;

            fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
                $builder::new(def, <Def::Markers as InitSlots>::init(), router)
            }
        }
    )+};
}

slot_form! {
    forms::Out => RouterSlots as RouterOut,
    forms::BatchOut => RouterSlots as RouterBatchOut,
}

/// The reply-carrying slot forms start with the reply attachment defaulted next to the unbound
/// slot tuple.
macro_rules! slot_reply_form {
    ($($form:ty => $fallback:ident as $alias:ident),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def>
            RouterMount<B, Routes, RouteCodec, RouteLayers, Def> for $form
        where
            B: Broker + 'static,
            Def: HasSlots,
            Def::Markers: InitSlots,
        {
            type Out = $alias<
                B,
                Routes,
                RouteCodec,
                RouteLayers,
                Def,
                $fallback,
                <Def::Markers as InitSlots>::Init,
            >;

            fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
                RouterSlotsWithReply::new(
                    def,
                    $fallback,
                    <Def::Markers as InitSlots>::init(),
                    router,
                )
            }
        }
    )+};
}

slot_reply_form! {
    forms::PublishingOut => DefaultReply as RouterPublishingOut,
    forms::RawReplyOut => DefaultReply as RouterRawReplyOut,
    forms::BatchPublishingOut => DefaultReply as RouterBatchPublishingOut,
}

// ---------------------------------------------------------------------------------------------
// The slot commits, one macro per form family, for fully-bound tuples only. `Bound` / `Extra`
// name the definition's `BindSlots` outputs so the bounds read flat instead of through
// `<Def::Bound as ..>` projections.

macro_rules! impl_inject_out_commit {
    ($(($($attach:ident / $layers:ident),+))+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def, Bound, Extra, $($attach, $layers),+>
            RouterSlotCommit<InjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for ($(WithSource<OutAttachment<$attach, $layers>>,)+)
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            $($layers: LowerOutTransforms<PublishIdentity>,)+
            Def: BindSlots<
                Connected<B>,
                ($((
                    $attach,
                    RouteCodec::Codec,
                    <$layers as LowerOutTransforms<PublishIdentity>>::Out,
                ),)+),
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
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                // The slots encode with the surface's codec; only the decode side honours the
                // definition's own override.
                let codec = router.codec.mount_codec();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), PublishIdentity),
                )+));
                let source = def.source();
                router.mount_inject(source, def, decode, extra)
            }
        }
    )+};
}

impl_inject_out_commit! {
    (A0 / L0)
    (A0 / L0, A1 / L1)
    (A0 / L0, A1 / L1, A2 / L2)
}

macro_rules! impl_batch_inject_out_commit {
    ($(($($attach:ident / $layers:ident),+))+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def, Bound, Extra, $($attach, $layers),+>
            RouterSlotCommit<BatchInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for ($(WithSource<OutAttachment<$attach, $layers>>,)+)
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            $($layers: LowerOutTransforms<PublishIdentity>,)+
            Def: BindSlots<
                Connected<B>,
                ($((
                    $attach,
                    RouteCodec::Codec,
                    <$layers as LowerOutTransforms<PublishIdentity>>::Out,
                ),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as BatchInjectDef>::Input, RouteCodec>,
            Bound: BatchInjectDef + PageSized + 'static,
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
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                // See the single-message commit: surface codec for the slots, override-aware
                // codec for the decode.
                let codec = router.codec.mount_codec();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), PublishIdentity),
                )+));
                let source = def.source();
                router.mount_batch_inject(source, def, decode, extra)
            }
        }
    )+};
}

impl_batch_inject_out_commit! {
    (A0 / L0)
    (A0 / L0, A1 / L1)
    (A0 / L0, A1 / L1, A2 / L2)
}

macro_rules! impl_publishing_out_commit {
    ($(($($attach:ident / $layers:ident),+))+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def, Policy, Bound, Extra, $($attach, $layers),+>
            RouterSlotCommit<PublishInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for (WithSource<Policy>, ($(WithSource<OutAttachment<$attach, $layers>>,)+))
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            $($layers: LowerOutTransforms<PublishIdentity>,)+
            Def: BindSlots<
                Connected<B>,
                ($((
                    $attach,
                    RouteCodec::Codec,
                    <$layers as LowerOutTransforms<PublishIdentity>>::Out,
                ),)+),
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
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                // Surface codec for the slots, override-aware codec for the decode.
                let codec = router.codec.mount_codec();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), PublishIdentity),
                )+));
                let source = def.source();
                router.mount_publishing_source(source, def, decode, reply.into_source(), extra)
            }
        }

        impl<B, Routes, RouteCodec, RouteLayers, Def, Policy, Bound, Extra, $($attach, $layers),+>
            RouterSlotCommit<RawReplyInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for (WithSource<Policy>, ($(WithSource<OutAttachment<$attach, $layers>>,)+))
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            $($layers: LowerOutTransforms<PublishIdentity>,)+
            Def: BindSlots<
                Connected<B>,
                ($((
                    $attach,
                    RouteCodec::Codec,
                    <$layers as LowerOutTransforms<PublishIdentity>>::Out,
                ),)+),
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
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                // Surface codec for the slots, override-aware codec for the decode.
                let codec = router.codec.mount_codec();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), PublishIdentity),
                )+));
                let source = def.source();
                router.mount_raw_reply_source(source, def, decode, reply.into_source(), extra)
            }
        }

        impl<B, Routes, RouteCodec, RouteLayers, Def, Policy, Bound, Extra, $($attach, $layers),+>
            RouterSlotCommit<BatchPublishInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
            for (WithSource<Policy>, ($(WithSource<OutAttachment<$attach, $layers>>,)+))
        where
            B: Broker + 'static,
            RouteCodec: MountCodec,
            $($layers: LowerOutTransforms<PublishIdentity>,)+
            Def: BindSlots<
                Connected<B>,
                ($((
                    $attach,
                    RouteCodec::Codec,
                    <$layers as LowerOutTransforms<PublishIdentity>>::Out,
                ),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as BatchPublishingDef>::Input, RouteCodec>,
            Bound: BatchPublishingDef + PageSized + 'static,
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
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
            ) -> Self::Out {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                // Surface codec for the slots, override-aware codec for the decode.
                let codec = router.codec.mount_codec();
                let decode = def.mounted_codec(&router.codec);
                let (def, extra) = def.bind(($(
                    $attach.into_source().wire(codec.clone(), PublishIdentity),
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
    (A0 / L0)
    (A0 / L0, A1 / L1)
    (A0 / L0, A1 / L1, A2 / L2)
}

// The defaulted reply sides, committed as if `.publisher(..)` had been chained with the
// broker's own policy.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
macro_rules! impl_default_typed_reply_slot_commit {
    ($($mount:ident),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def, Slots>
            RouterSlotCommit<$mount, B, Routes, RouteCodec, RouteLayers, Def>
            for (DefaultReply, Slots)
        where
            B: Broker + 'static,
            B::Connected: DefaultPublish,
            (
                WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>>,
                Slots,
            ): RouterSlotCommit<$mount, B, Routes, RouteCodec, RouteLayers, Def>,
        {
            type Out = <(
                WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>>,
                Slots,
            ) as RouterSlotCommit<$mount, B, Routes, RouteCodec, RouteLayers, Def>>::Out;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers>,
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
impl<B, Routes, RouteCodec, RouteLayers, Def, Slots>
    RouterSlotCommit<RawReplyInjectMount, B, Routes, RouteCodec, RouteLayers, Def>
    for (DefaultReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (WithSource<<B::Connected as DefaultPublish>::Policy>, Slots):
        RouterSlotCommit<RawReplyInjectMount, B, Routes, RouteCodec, RouteLayers, Def>,
{
    type Out =
        <(WithSource<<B::Connected as DefaultPublish>::Policy>, Slots) as RouterSlotCommit<
            RawReplyInjectMount,
            B,
            Routes,
            RouteCodec,
            RouteLayers,
            Def,
        >>::Out;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        (
            WithSource::new(<B::Connected as DefaultPublish>::Policy::default()),
            self.1,
        )
            .commit(def, router)
    }
}
