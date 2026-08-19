//! Mount forms for the batch shapes: injections, publishing and their slot combinations.

use serde::Serialize;

// The typed default-reply commits need a default codec, so that import is gated the same way;
// the raw default-reply commit publishes bare bytes and needs only `DefaultPublish`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
// The default-reply commits build a `TypedPublisher` over the broker's default policy, so those
// imports are gated with the default codec they require.
use crate::{BatchSubscriber, Broker, Connected, PublishPolicy, Subscriber, SubscriptionSource};
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::{DefaultPublish, Publisher};

use crate::runtime::batch_inject::BatchInjectCall;
use crate::runtime::batch_publishing::BatchPublishingCall;
use crate::runtime::inject::FromStartup;
use crate::runtime::input::DecodeWith;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::TypedPublisher;
use crate::runtime::publish::{PublishPipeline, ReplyPublisher};
use crate::runtime::slot::{BindSlots, HasSlots, InitSlots, IntoSlotSource, WithSource};
use crate::runtime::{SourceMessage, SourceSubscriber};

use super::{
    BatchInjectMount, BatchPublishInjectMount, BatchPublishMount, CommitVia, DefaultReply,
    IncludeBatchOut, IncludeBatchPublishing, IncludeBatchPublishingOut, IncludeMount, IncludeSlots,
    IncludeSlotsWithReply, IncludeWith, MountCodec, SlotCommit, forms,
};
use crate::runtime::app::scope::BrokerScope;

// ---------------------------------------------------------------------------------------------
// Batch injections: the batch counterparts of the Seek (eager) and Out (builder) forms.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchSeek
where
    B: Broker + 'static,
    C: MountCodec,
    Def: BatchInjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as MountCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_inject(source, def, ((),));
    }
}

/// Implements the slot-tuple commit of the batch Out form for each slot arity, for fully-bound
/// tuples only. `Bound` / `Extra` name the definition's [`BindSlots`] outputs.
macro_rules! impl_batch_inject_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Bound, Extra, $($attach),+>
            SlotCommit<BatchInjectMount, B, Layers, C, State, Pipeline, Def>
            for ($(WithSource<$attach>,)+)
        where
            B: Broker + 'static,
            C: MountCodec,
            Def: BindSlots<Connected<B>, ($(($attach, C::Codec),)+), Bound = Bound, Extra = Extra>,
            Bound: BatchInjectCall<State> + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<C::Codec>,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Extra: Send + Sync + 'static,
            State: Send + Sync + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                let codec = scope.codec.mount_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                scope.mount_batch_inject(source, def, extra);
            }
        }
    )+};
}

impl_batch_inject_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
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
    C: MountCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as MountCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live:
        Publisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let reply = TypedPublisher::new(<B::Connected as DefaultPublish>::Policy::default());
        scope.mount_batch_publishing_source(source, def, reply, ((),));
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, BatchReply>
    CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: MountCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as MountCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
    BatchReply: ReplyPublisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_publishing_source(source, def, self.into_source(), ((),));
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
        WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>,
        Slots,
    ): SlotCommit<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply, as if the user had chained `.publisher(..)` themselves.
        SlotCommit::commit(
            (
                WithSource::new(TypedPublisher::new(
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
    ($(($($attach:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Source, BatchReply, Bound, Extra, $($attach),+>
            SlotCommit<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>
            for (WithSource<Source>, ($(WithSource<$attach>,)+))
        where
            B: Broker + 'static,
            C: MountCodec,
            Def: BindSlots<Connected<B>, ($(($attach, C::Codec),)+), Bound = Bound, Extra = Extra>,
            Bound: BatchPublishingCall<State> + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<C::Codec>,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Bound::Reply: Serialize + Send + Sync + 'static,
            Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
            BatchReply: ReplyPublisher + 'static,
            Extra: Send + Sync + 'static,
            Pipeline: PublishPipeline + Clone + Send + 'static,
            State: Send + Sync + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                let codec = scope.codec.mount_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                scope.mount_batch_publishing_source(source, def, reply.into_source(), extra);
            }
        }
    )+};
}

impl_batch_publishing_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
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
