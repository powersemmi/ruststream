//! Mount tokens and the commit trait every registration builder resolves through.

// The typed default-reply commits need a default codec, so that import is gated the same way;
// the raw default-reply commit publishes bare bytes and needs only `DefaultPublish`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
// The default-reply commits build a `TypedPublisher` over the broker's default policy, so those
// imports are gated with the default codec they require.
use crate::{
    Broker, BuildContext, Connected, DefaultPublish, PublishPolicy, Subscriber, SubscriptionSource,
};

use crate::runtime::handler::Handler;
use crate::runtime::inject::FromStartup;
use crate::runtime::input::DecodeWith;
use crate::runtime::middleware::Layer;
use crate::runtime::publish::PublishPipeline;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::TypedPublisher;
use crate::runtime::publishing::{PublishingCall, PublishingHandler, ReplySink};
use crate::runtime::slot::{IntoSlotSource, WithSource};

use super::ScopeCodec;
use crate::runtime::app::scope::BrokerScope;

// ---------------------------------------------------------------------------------------------
// Builder-producing forms: reply publishing, out injection, and their batch counterparts.
//
// The builder commits on Drop, so `b.include(def)` alone still registers (with the broker's
// default publish policy where one exists), while `b.include(def).publisher(src)` replaces the
// commit with the attached source. User sources are wrapped in `WithSource` so the default
// marker and the source-driven commit live on different type constructors (disjoint impls, no
// negative reasoning needed).
//
// Every form family shares one commit trait, keyed by a mount token: strategies of different
// families are impls on the same attachment types with different concrete tokens, so they
// never overlap without negative reasoning. Two generic builders then serve every family -
// [`IncludeWith`] (one attachment, replaced by `.publisher(..)`) and [`IncludeWithOut`] (a
// reply attachment plus the `Out` parameter's own `.out(..)`) - and the per-form names are
// aliases picking the token and the initial attachment.

/// The default reply commit: the broker's default publish policy under the default codec.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultReply;

/// The default commit of the byte-reply form: the broker's plain publish policy taken bare.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultBareReply;

/// The mount tokens keying [`CommitVia`]: which mount a committed attachment drives.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct InjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishInjectMount;

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
    WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>:
        CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply: the broker's plain publish policy under the default codec,
        // committed as if the user had chained `.publisher(TypedPublisher::new(<policy>))`.
        CommitVia::commit(
            WithSource::new(TypedPublisher::new(
                <B::Connected as DefaultPublish>::Policy::default(),
            )),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def> CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>
    for DefaultBareReply
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

impl<B, Layers, C, State, Pipeline, Def, Source>
    CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: PublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
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
    Layers: Layer<PublishingHandler<Def, <C as ScopeCodec>::Codec, Source::Live, Pipeline>>
        + Clone
        + Send
        + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_publishing_source(source, def, self.into_source(), ((),));
    }
}
