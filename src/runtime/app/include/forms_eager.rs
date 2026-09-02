//! Eager mount forms: plain, raw, and plain batch.

use crate::{BatchSubscriber, Broker, BuildContext, Connected, Subscriber, SubscriptionSource};

use crate::runtime::batch::BatchDef;
use crate::runtime::handler::Handler;
use crate::runtime::input::{DecodeWith, InputKind, RawBytes};
use crate::runtime::middleware::Layer;
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;
use crate::runtime::{RawSliceHandler, SliceHandler};

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
// Raw subscribing: eager, no builder, and no codec anywhere - the byte input kind decodes with
// `()`, so the scope codec parameter `C` is left unconstrained and the mount works without any
// codec feature.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawSubscribing
where
    B: Broker + 'static,
    Def: SubscriberDef<Input = RawBytes>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Handler: 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
        Typed<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            RawBytes,
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
    Def::Handler: SliceHandler<<Def::Input as InputKind>::Owned, State> + 'static,
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
// Raw batch: eager, and no codec anywhere - the handler borrows the batch's payloads as they
// arrived, so the scope codec parameter `C` is left unconstrained.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawBatch
where
    B: Broker + 'static,
    Def: BatchDef<Input = RawBytes>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    Def::Handler: RawSliceHandler<State> + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_raw_batch(source, def);
    }
}
