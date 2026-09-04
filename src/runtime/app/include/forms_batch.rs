//! Mount forms for the batch shapes: injections, publishing and their slot combinations.

use serde::Serialize;

use crate::{
    BatchSubscriber, Broker, BuildBatchContext, Connected, PublishPolicy, Subscriber,
    SubscriptionSource,
};
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::{DefaultPublish, Publisher};

use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingDef};
use crate::runtime::inject::FromStartup;
use crate::runtime::input::DecodeWith;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::ReplyWiring;
use crate::runtime::publish::{LowerOutTransforms, PublishPipeline, ReplyPublisher};
use crate::runtime::settings::{DefMountCodec, MountsWith, PageSized};
use crate::runtime::slot::{
    BindSlots, HasSlots, InitSlots, IntoSlotSource, OutAttachment, WithSource,
};
use crate::runtime::{SourceMessage, SourceSubscriber};

use super::{
    BatchInjectMount, BatchPublishInjectMount, BatchPublishMount, CommitVia, DefaultReply,
    IncludeBatchOut, IncludeBatchPublishing, IncludeBatchPublishingOut, IncludeMount, IncludeSlots,
    IncludeSlotsWithReply, IncludeWith, MountCodec, SlotCommit, forms,
};
use crate::runtime::app::scope::BrokerScope;

// ---------------------------------------------------------------------------------------------
// Batch injections: the batch counterpart of the Out (builder) form.

/// Implements the slot-tuple commit of the batch Out form for each slot arity, for fully-bound
/// tuples only. `Bound` / `Extra` name the definition's [`BindSlots`] outputs.
macro_rules! impl_batch_inject_out_commit {
    ($(($($attach:ident / $layers:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Bound, Extra, $($attach, $layers),+>
            SlotCommit<BatchInjectMount, B, Layers, C, State, Pipeline, Def>
            for ($(WithSource<OutAttachment<$attach, $layers>>,)+)
        where
            B: Broker + 'static,
            C: MountCodec,
            Pipeline: Clone,
            $($layers: LowerOutTransforms<Pipeline>,)+
            Def: BindSlots<
                Connected<B>,
                ($(($attach, C::Codec, <$layers as LowerOutTransforms<Pipeline>>::Out),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as BatchInjectDef>::Input, C>,
            Bound: BatchInjectCall<State> + PageSized + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<DefMountCodec<Def, <Bound as BatchInjectDef>::Input, C>>,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Bound::Context: BuildBatchContext<SourceMessage<B, Bound::Source>>
                + Send
                + Sync
                + 'static,
            Extra: Send + Sync + 'static,
            State: Send + Sync + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                // See the single-message Out commit: surface codec for the slots, and the app's
                // publish pipeline under each slot's own transforms.
                let codec = scope.codec.mount_codec();
                let decode = def.mounted_codec(&scope.codec);
                let (def, extra) = def.bind(($(
                    $attach
                        .into_source()
                        .wire(codec.clone(), scope.pipeline.clone()),
                )+));
                let source = def.source();
                scope.mount_batch_inject(source, def, decode, extra);
            }
        }
    )+};
}

impl_batch_inject_out_commit! {
    (A0 / L0)
    (A0 / L0, A1 / L1)
    (A0 / L0, A1 / L1, A2 / L2)
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out =
        IncludeBatchOut<'s, B, Layers, C, State, Pipeline, Def, <Def::Markers as InitSlots>::Init>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeSlots::new(def, <Def::Markers as InitSlots>::init(), scope)
    }
}

// ---------------------------------------------------------------------------------------------
// Batch publishing: the same builder shape; the reply source pairs into a ReplyPublisher.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def>
    CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def> for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    Def: BatchPublishingCall<State>
        + PageSized
        + MountsWith<<Def as BatchPublishingDef>::Input, C>
        + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as BatchPublishingDef>::Input, C>>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Def::Context: BuildBatchContext<SourceMessage<B, Def::Source>> + Send + Sync + 'static,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live:
        Publisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let codec = def.mounted_codec(&scope.codec);
        let source = def.source();
        let reply = ReplyWiring::new(<B::Connected as DefaultPublish>::Policy::default());
        scope.mount_batch_publishing_source(source, def, codec, reply, ((),));
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, BatchReply>
    CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    Def: BatchPublishingCall<State>
        + PageSized
        + MountsWith<<Def as BatchPublishingDef>::Input, C>
        + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as BatchPublishingDef>::Input, C>>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Def::Context: BuildBatchContext<SourceMessage<B, Def::Source>> + Send + Sync + 'static,
    Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
    BatchReply: ReplyPublisher<Def::Context> + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let codec = def.mounted_codec(&scope.codec);
        let source = def.source();
        scope.mount_batch_publishing_source(source, def, codec, self.into_source(), ((),));
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchPublishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith::new(def, DefaultReply, scope)
    }
}

// ---------------------------------------------------------------------------------------------
// Batch publishing with Out slots: the two-attachment builder at the batch shape.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def, Slots>
    SlotCommit<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>
    for (DefaultReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (
        WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>>,
        Slots,
    ): SlotCommit<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply, as if the user had chained `.publisher(..)` themselves.
        SlotCommit::commit(
            (
                WithSource::new(ReplyWiring::new(
                    <B::Connected as DefaultPublish>::Policy::default(),
                )),
                self.1,
            ),
            def,
            scope,
        );
    }
}

/// Implements the slot-tuple commit of the batch-publishing-with-Out form for each slot arity,
/// for fully-bound tuples only. `Bound` / `Extra` name the definition's [`BindSlots`] outputs.
macro_rules! impl_batch_publishing_out_commit {
    ($(($($attach:ident / $layers:ident),+))+) => {$(
        impl<
            B,
            Layers,
            C,
            State,
            Pipeline,
            Def,
            Source,
            BatchReply,
            Bound,
            Extra,
            $($attach, $layers),+
        >
            SlotCommit<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>
            for (WithSource<Source>, ($(WithSource<OutAttachment<$attach, $layers>>,)+))
        where
            B: Broker + 'static,
            C: MountCodec,
            $($layers: LowerOutTransforms<Pipeline>,)+
            Def: BindSlots<
                Connected<B>,
                ($(($attach, C::Codec, <$layers as LowerOutTransforms<Pipeline>>::Out),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as BatchPublishingDef>::Input, C>,
            Bound: BatchPublishingCall<State> + PageSized + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<DefMountCodec<Def, <Bound as BatchPublishingDef>::Input, C>>,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Bound::Reply: Serialize + Send + Sync + 'static,
            Bound::Context: BuildBatchContext<SourceMessage<B, Bound::Source>>
                + Send
                + Sync
                + 'static,
            Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
            BatchReply: ReplyPublisher<Bound::Context> + 'static,
            Extra: Send + Sync + 'static,
            Pipeline: PublishPipeline + Clone + Send + 'static,
            State: Send + Sync + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                // Surface codec for the slots, override-aware codec for the decode; the page's
                // replies and the slots ride the same app-wide pipeline.
                let codec = scope.codec.mount_codec();
                let decode = def.mounted_codec(&scope.codec);
                let (def, extra) = def.bind(($(
                    $attach
                        .into_source()
                        .wire(codec.clone(), scope.pipeline.clone()),
                )+));
                let source = def.source();
                scope.mount_batch_publishing_source(
                    source,
                    def,
                    decode,
                    reply.into_source(),
                    extra,
                );
            }
        }
    )+};
}

impl_batch_publishing_out_commit! {
    (A0 / L0)
    (A0 / L0, A1 / L1)
    (A0 / L0, A1 / L1, A2 / L2)
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchPublishingOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out = IncludeBatchPublishingOut<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        DefaultReply,
        <Def::Markers as InitSlots>::Init,
    >;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeSlotsWithReply::new(
            def,
            DefaultReply,
            <Def::Markers as InitSlots>::init(),
            scope,
        )
    }
}
