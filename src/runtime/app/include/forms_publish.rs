//! Mount forms for reply publishing, alone and combined with Out slots.

// The typed default-reply commits build a `TypedPublisher`, so the codec import is gated.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::{Broker, BuildContext, Connected, DefaultPublish, PublishPolicy, SubscriptionSource};

use crate::runtime::handler::Handler;
use crate::runtime::inject::FromStartup;
use crate::runtime::input::DecodeWith;
use crate::runtime::middleware::Layer;
use crate::runtime::publish::PublishPipeline;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::TypedPublisher;
use crate::runtime::publishing::{PublishingCall, PublishingDef, PublishingHandler, ReplySink};
use crate::runtime::settings::{DefMountCodec, MountsWith};
use crate::runtime::slot::{BindSlots, HasSlots, InitSlots, IntoSlotSource, WithSource};
use crate::runtime::{SourceMessage, SourceSubscriber};

use super::builder::IncludeRawReply;
use super::slot_reply_builder::IncludeRawReplyOut;
use super::{
    CommitVia, DefaultReply, IncludeMount, IncludePublishing, IncludePublishingOut,
    IncludeSlotsWithReply, IncludeWith, MountCodec, PublishInjectMount, PublishMount,
    RawReplyInjectMount, RawReplyMount, SlotCommit, forms,
};
use crate::runtime::app::scope::BrokerScope;

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Publishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith::new(def, DefaultReply, scope)
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawReply
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitVia<RawReplyMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludeRawReply<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith::new(def, DefaultReply, scope)
    }
}

// ---------------------------------------------------------------------------------------------
// Reply publishing with Out slots: two attachment axes on one builder. The reply side keeps its
// default commits (typed or bare policy); the slot side starts all-unbound, each
// `.out(marker, ..)` binds one position, and the SlotCommit impls exist only for fully-bound
// tuples - so `.build()` on an incomplete chain is a compile error naming the slot.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def, Slots>
    SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def> for (DefaultReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (
        WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>,
        Slots,
    ): SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
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

// The serialized wire's default next to the slot tuple: the broker's plain policy taken bare,
// keyed by the raw mount token so it exists with no codec feature at all.
impl<B, Layers, C, State, Pipeline, Def, Slots>
    SlotCommit<RawReplyInjectMount, B, Layers, C, State, Pipeline, Def> for (DefaultReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (WithSource<<B::Connected as DefaultPublish>::Policy>, Slots):
        SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        SlotCommit::commit(
            (
                WithSource::new(<B::Connected as DefaultPublish>::Policy::default()),
                self.1,
            ),
            def,
            scope,
        );
    }
}

// A user policy on the serialized wire commits through the same wire-agnostic machinery the
// encoded attach does; see the scope's `RawReplyMount` commit.
impl<B, Layers, C, State, Pipeline, Def, Source, Slots>
    SlotCommit<RawReplyInjectMount, B, Layers, C, State, Pipeline, Def>
    for (WithSource<Source>, Slots)
where
    B: Broker + 'static,
    Self: SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        <Self as SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def>>::commit(
            self, def, scope,
        );
    }
}

/// Implements the slot-tuple commit of the publishing-with-Out forms for each slot arity, for
/// fully-bound tuples only: the bound sources instantiate the definition ([`BindSlots`],
/// named `Bound` / `Extra` here), the reply source pairs at startup, and the injections
/// resolve against the slot extras.
macro_rules! impl_publishing_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Source, Bound, Extra, $($attach),+>
            SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def>
            for (WithSource<Source>, ($(WithSource<$attach>,)+))
        where
            B: Broker + 'static,
            // Two codec questions, and they are not the same one: the slots encode what leaves
            // through them (`MountCodec`), while the input decodes with whatever its kind asks
            // for - nothing, on the byte path - under the definition's own override.
            C: MountCodec,
            Def: BindSlots<
                Connected<B>,
                ($(($attach, <C as MountCodec>::Codec),)+),
                Bound = Bound,
                Extra = Extra,
            >,
            Def: MountsWith<<Bound as PublishingDef>::Input, C>,
            Bound: PublishingCall<State> + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + Sync + 'static,
            Bound::Input: DecodeWith<DefMountCodec<Def, <Bound as PublishingDef>::Input, C>>,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Bound::Reply: Send + Sync + 'static,
            Bound::Context: BuildContext<SourceMessage<B, Bound::Source>> + Send + Sync + 'static,
            Source: PublishPolicy<Connected<B>> + Send + 'static,
            Source::Live: ReplySink<Bound::Reply, Bound::Context, Pipeline> + 'static,
            Extra: Send + Sync + 'static,
            Pipeline: PublishPipeline + Clone + Send + 'static,
            State: Send + Sync + 'static,
            Layers: Layer<
                PublishingHandler<
                    Bound,
                    DefMountCodec<Def, <Bound as PublishingDef>::Input, C>,
                    Source::Live,
                    Pipeline,
                >,
            >
                + Clone
                + Send
                + 'static,
            Layers::Handler:
                Handler<SourceMessage<B, Bound::Source>, Bound::Context, State> + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                // Surface codec for the slots, override-aware codec for the decode.
                let codec = scope.codec.mount_codec();
                let decode = def.mounted_codec(&scope.codec);
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                scope.mount_publishing_source(source, def, decode, reply.into_source(), extra);
            }
        }
    )+};
}

impl_publishing_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::PublishingOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out = IncludePublishingOut<
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

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawReplyOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out = IncludeRawReplyOut<
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
