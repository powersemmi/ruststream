//! Eager mount forms: plain, self-deserializing, and the two batch shapes.

use crate::{
    BatchSubscriber, Broker, BuildBatchContext, BuildContext, Connected, Subscriber,
    SubscriptionSource,
};

use crate::runtime::SliceHandler;
use crate::runtime::batch::BatchDef;
use crate::runtime::handle::Deserialized;
use crate::runtime::handler::Handler;
use crate::runtime::input::{DecodeWith, InputKind, Provided};
use crate::runtime::middleware::Layer;
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;

use super::{IncludeMount, forms};
use crate::runtime::app::scope::BrokerScope;
use crate::runtime::settings::{DefMountCodec, MountsWith};

// ---------------------------------------------------------------------------------------------
// Plain subscribing: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Subscribing
where
    B: Broker + 'static,
    Def: SubscriberDef + MountsWith<<Def as SubscriberDef>::Input, C>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as SubscriberDef>::Input, C>>,
    Def::Handler: 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
        Typed<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Input,
            DefMountCodec<Def, <Def as SubscriberDef>::Input, C>,
            Def::Handler,
        >,
    >,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let codec = def.mounted_codec(&scope.codec);
        let source = def.source();
        scope.mount_subscriber(source, def, codec);
    }
}

// ---------------------------------------------------------------------------------------------
// Self-deserializing subscribing: eager, no builder, and no codec anywhere - the input kind
// decodes with `()`, so the scope codec parameter `C` is left unconstrained and the mount works
// without any codec feature.

impl<'s, B, Layers, C, State, Pipeline, Def, F> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawSubscribing
where
    B: Broker + 'static,
    Def: SubscriberDef<Input = Provided<F>>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Handler: 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    F: Send + Sync + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
        Typed<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Provided<F>,
            (),
            Def::Handler,
        >,
    >,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_subscriber(source, def, ());
    }
}

// ---------------------------------------------------------------------------------------------
// Plain batch: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Batch
where
    B: Broker + 'static,
    Def: BatchDef + MountsWith<<Def as BatchDef>::Input, C>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<DefMountCodec<Def, <Def as BatchDef>::Input, C>>,
    Def::Context: BuildBatchContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Def::Handler: SliceHandler<<Def::Input as InputKind>::Owned, Def::Context, State> + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let codec = def.mounted_codec(&scope.codec);
        let source = def.source();
        scope.mount_batch(source, def, codec);
    }
}

// ---------------------------------------------------------------------------------------------
// Self-deserializing batch: eager, and no codec anywhere - each element constructs itself from
// its delivery's payload, so the scope codec parameter `C` is left unconstrained.

impl<'s, B, Layers, C, State, Pipeline, Def, F> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawBatch
where
    B: Broker + 'static,
    Def: BatchDef<Input = Provided<F>>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    Def::Context: BuildBatchContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Def::Handler: for<'p> SliceHandler<F::Output<'p>, Def::Context, State> + 'static,
    F: Deserialized + Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_raw_batch(source, def);
    }
}
