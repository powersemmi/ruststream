//! Mount tokens and the commit trait every registration builder resolves through.

use crate::{
    Broker, BuildContext, Connected, DefaultPublish, PublishPolicy, Subscriber, SubscriptionSource,
};

use crate::runtime::handler::Handler;
use crate::runtime::inject::FromStartup;
use crate::runtime::input::DecodeWith;
use crate::runtime::middleware::Layer;
use crate::runtime::publish::PublishPipeline;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::ReplyWiring;
use crate::runtime::publishing::{PublishingCall, PublishingDef, PublishingHandler, ReplySink};
use crate::runtime::settings::{DefMountCodec, MountsWith};
use crate::runtime::slot::{IntoSlotSource, WithSource};

use super::{DefaultReply, PublishMount, RawReplyMount};
use crate::runtime::app::scope::BrokerScope;

// ---------------------------------------------------------------------------------------------
// Builder-producing forms: reply publishing, out injection, and their batch counterparts.
//
// The builder commits on Drop, so `b.include(def)` alone still registers (with the broker's
// default publish policy where one exists), while `b.include(def).publisher(src)` replaces the
// commit with the attached source. User sources are wrapped in `WithSource`: the default marker
// and the source-driven commit must live on different type constructors to keep the impls
// disjoint.
//
// Every form family shares one commit trait, keyed by a mount token. Two generic builders serve
// every family - [`IncludeWith`] (one attachment, replaced by `.publisher(..)`) and
// [`IncludeWithOut`] (a reply attachment plus the `Out` parameter's own `.out(..)`) - and the
// per-form names are aliases picking the token and the initial attachment.

/// One commit strategy of a registration builder, keyed by its `Mount` token. Machinery;
/// never named directly.
#[doc(hidden)]
pub trait CommitVia<Mount, B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def> CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<ReplyWiring<<B::Connected as DefaultPublish>::Policy>>:
        CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply: the broker's plain publish policy under the default codec,
        // committed as if the user had chained `.publisher(<policy>)`.
        CommitVia::commit(
            WithSource::new(ReplyWiring::new(
                <B::Connected as DefaultPublish>::Policy::default(),
            )),
            def,
            scope,
        );
    }
}

// The serialized wire's default: the broker's plain publish policy taken bare, keyed by the
// raw mount token so it exists with no codec feature at all.
impl<B, Layers, C, State, Pipeline, Def>
    CommitVia<RawReplyMount, B, Layers, C, State, Pipeline, Def> for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<<B::Connected as DefaultPublish>::Policy>:
        CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        CommitVia::commit(
            WithSource::new(<B::Connected as DefaultPublish>::Policy::default()),
            def,
            scope,
        );
    }
}

// A user policy on the serialized wire commits through the same wire-agnostic machinery the
// encoded attach does: the scope's `ReplySink` bound is structural, so one generic commit
// serves both mount tokens.
impl<B, Layers, C, State, Pipeline, Def, Source>
    CommitVia<RawReplyMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    Self: CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        <Self as CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>>::commit(
            self, def, scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source>
    CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    // Resolved against the input kind rather than the surface: a byte-input handler decodes
    // with `()`, so this mount carries no demand for a default codec the build may not have.
    Def: PublishingCall<State> + MountsWith<<Def as PublishingDef>::Input, C> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as PublishingDef>::Input, C>>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    Def::Reply: Send + Sync + 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Source: PublishPolicy<Connected<B>> + Send + 'static,
    Source::Live: ReplySink<Def::Reply, Def::Context, Pipeline> + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
            PublishingHandler<
                Def,
                DefMountCodec<Def, <Def as PublishingDef>::Input, C>,
                Source::Live,
                Pipeline,
            >,
        > + Clone
        + Send
        + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let codec = def.mounted_codec(&scope.codec);
        let source = def.source();
        scope.mount_publishing_source(source, def, codec, self.into_source(), ((),));
    }
}
