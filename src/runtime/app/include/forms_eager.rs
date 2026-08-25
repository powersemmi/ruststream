//! Eager mount forms: plain, raw, attachment-free injections and plain batch.

use crate::{BatchSubscriber, Broker, BuildContext, Connected, Subscriber, SubscriptionSource};

use crate::runtime::batch::{BatchDef, BatchWithHeadersDef};
use crate::runtime::handler::Handler;
use crate::runtime::inject::{FromStartup, InjectCall, InjectHandler};
use crate::runtime::input::{DecodeWith, InputKind, RawBytes};
use crate::runtime::middleware::Layer;
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;
use crate::runtime::{RawSliceHandler, SliceHandler, SliceHandlerWithHeaders};

use super::{IncludeMount, MountCodec, forms};
use crate::runtime::app::scope::BrokerScope;

// ---------------------------------------------------------------------------------------------
// Plain subscribing: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Subscribing
where
    B: Broker + 'static,
    C: MountCodec,
    Def: SubscriberDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Input: DecodeWith<<C as MountCodec>::Codec>,
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
            C::Codec,
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
        let codec = scope.codec.mount_codec();
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
// Attachment-free injections: eager, no builder - a definition whose startup injections need
// nothing from the include site (a Seek parameter without an Out one) resolves against the
// subscription itself.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Seek
where
    B: Broker + 'static,
    C: MountCodec,
    Def: InjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<<C as MountCodec>::Codec>,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<InjectHandler<Def, <C as MountCodec>::Codec>> + Clone + Send + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_inject(source, def, ((),));
    }
}

// ---------------------------------------------------------------------------------------------
// Plain batch: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Batch
where
    B: Broker + 'static,
    C: MountCodec,
    Def: BatchDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<<C as MountCodec>::Codec>,
    Def::Handler: SliceHandler<<Def::Input as InputKind>::Owned, State> + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let codec = scope.codec.mount_codec();
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

// ---------------------------------------------------------------------------------------------
// Batch with a per-element header contract: eager, like the plain batch form.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchWithHeaders
where
    B: Broker + 'static,
    C: MountCodec,
    Def: BatchWithHeadersDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<<C as MountCodec>::Codec>,
    Def::Handler: SliceHandlerWithHeaders<
            <Def::Input as InputKind>::Owned,
            <Def as BatchWithHeadersDef>::Headers,
            State,
        > + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let codec = scope.codec.mount_codec();
        scope.mount_batch_with_headers(source, def, codec);
    }
}
