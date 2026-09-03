//! What a [`SubscriberBuilder`] is to the mount machinery: the definition it wraps, with the
//! source and the policies it collected in place of the definition's own.
//!
//! Every definition form forwards the same way - the structural pieces (input kind, context,
//! handler, injections, reply) come from the wrapped definition, the declarative settings come
//! from the builder - so the mount impls never learn that a builder is involved.
//!
//! The source is rebuilt per mount, so it is cloned rather than moved: a definition mounted on
//! two brokers hands each of them its own value, exactly as a definition's own `source()` does.
//!
//! Associated types are named through the wrapped definition rather than through `Self`: the
//! builder implements every definition form, so a bare `Self::Input` would be ambiguous.

use std::future::Future;
use std::marker::PhantomData;

use crate::ConnectedBroker;

use super::SubscriberBuilder;
use crate::runtime::batch::{BatchDef, BatchResult};
use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingDef};
use crate::runtime::context::Context;
use crate::runtime::dispatch::Workers;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::HandlerOutcome;
use crate::runtime::inject::{InjectCall, InjectDef};
use crate::runtime::input::InputKind;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publishing::{PublishingCall, PublishingDef};
use crate::runtime::slot::{BindSlots, HasSlots};
use crate::runtime::subscriber_def::SubscriberDef;

/// The metadata methods every definition trait shares, forwarded to the wrapped definition, plus
/// the two settings the builder owns.
macro_rules! forward_common {
    ($def:ty) => {
        fn source(&self) -> Src {
            self.source.clone()
        }

        fn workers(&self) -> Workers {
            self.workers
        }

        fn failure_policies(&self) -> FailurePolicies {
            self.failures
        }

        fn description(&self) -> Option<&str> {
            <Def as $def>::description(&self.def)
        }

        fn input_schema(&self) -> Option<String> {
            <Def as $def>::input_schema(&self.def)
        }

        fn headers_schema(&self) -> Option<String> {
            <Def as $def>::headers_schema(&self.def)
        }

        fn message_name(&self) -> Option<&'static str> {
            <Def as $def>::message_name(&self.def)
        }

        fn message_description(&self) -> Option<&'static str> {
            <Def as $def>::message_description(&self.def)
        }
    };
}

/// The `outgoing()` declaration, on the forms that carry one.
macro_rules! forward_outgoing {
    ($def:ty) => {
        fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
            <Def as $def>::outgoing(&self.def)
        }
    };
}

impl<Def, Src, State, DC> SubscriberDef for SubscriberBuilder<Def, Src, State, DC>
where
    Def: SubscriberDef,
    Src: Clone,
{
    type Input = <Def as SubscriberDef>::Input;
    type Context = <Def as SubscriberDef>::Context;
    type Handler = <Def as SubscriberDef>::Handler;
    type Source = Src;

    forward_common!(SubscriberDef);

    fn into_handler(self) -> <Def as SubscriberDef>::Handler {
        self.def.into_handler()
    }
}

impl<Def, Src, State, DC> BatchDef for SubscriberBuilder<Def, Src, State, DC>
where
    Def: BatchDef,
    Src: Clone,
{
    type Input = <Def as BatchDef>::Input;
    type Context = <Def as BatchDef>::Context;
    type Handler = <Def as BatchDef>::Handler;
    type Source = Src;

    forward_common!(BatchDef);

    fn into_handler(self) -> <Def as BatchDef>::Handler {
        self.def.into_handler()
    }
}

impl<Def, Src, State, DC> InjectDef for SubscriberBuilder<Def, Src, State, DC>
where
    Def: InjectDef,
    Src: Clone + Send + Sync,
    State: Send + Sync,
    DC: Send + Sync,
{
    type Input = <Def as InjectDef>::Input;
    type Context = <Def as InjectDef>::Context;
    type Source = Src;
    type Injections = <Def as InjectDef>::Injections;

    forward_common!(InjectDef);
    forward_outgoing!(InjectDef);
}

impl<Def, Src, State, DC, S> InjectCall<S> for SubscriberBuilder<Def, Src, State, DC>
where
    Def: InjectCall<S>,
    Src: Clone + Send + Sync,
    State: Send + Sync,
    DC: Send + Sync,
{
    fn call(
        &self,
        input: &<<Def as InjectDef>::Input as InputKind>::Target,
        injections: &<Def as InjectDef>::Injections,
        ctx: &mut Context<'_, <Def as InjectDef>::Context, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        self.def.call(input, injections, ctx)
    }
}

impl<Def, Src, State, DC> PublishingDef for SubscriberBuilder<Def, Src, State, DC>
where
    Def: PublishingDef,
    Src: Clone + Send + Sync,
    State: Send + Sync,
    DC: Send + Sync,
{
    type Input = <Def as PublishingDef>::Input;
    type Injections = <Def as PublishingDef>::Injections;
    type Reply = <Def as PublishingDef>::Reply;
    type Context = <Def as PublishingDef>::Context;
    type Source = Src;

    forward_common!(PublishingDef);
    forward_outgoing!(PublishingDef);

    fn reply_name(&self) -> &str {
        self.def.reply_name()
    }
}

impl<Def, Src, State, DC, S> PublishingCall<S> for SubscriberBuilder<Def, Src, State, DC>
where
    Def: PublishingCall<S>,
    Src: Clone + Send + Sync,
    State: Send + Sync,
    DC: Send + Sync,
{
    fn call(
        &self,
        input: &<<Def as PublishingDef>::Input as InputKind>::Target,
        injections: &<Def as PublishingDef>::Injections,
        ctx: &mut Context<'_, <Def as PublishingDef>::Context, S>,
    ) -> impl Future<Output = Result<<Def as PublishingDef>::Reply, HandlerOutcome>> + Send {
        self.def.call(input, injections, ctx)
    }
}

impl<Def, Src, State, DC> BatchInjectDef for SubscriberBuilder<Def, Src, State, DC>
where
    Def: BatchInjectDef,
    Src: Clone + Send + Sync,
    State: Send + Sync,
    DC: Send + Sync,
{
    type Input = <Def as BatchInjectDef>::Input;
    type Source = Src;
    type Injections = <Def as BatchInjectDef>::Injections;

    forward_common!(BatchInjectDef);
    forward_outgoing!(BatchInjectDef);
}

impl<Def, Src, State, DC, S> BatchInjectCall<S> for SubscriberBuilder<Def, Src, State, DC>
where
    Def: BatchInjectCall<S>,
    Src: Clone + Send + Sync,
    State: Send + Sync,
    DC: Send + Sync,
{
    fn call(
        &self,
        batch: &[<<Def as BatchInjectDef>::Input as InputKind>::Owned],
        injections: &<Def as BatchInjectDef>::Injections,
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = BatchResult> + Send {
        self.def.call(batch, injections, ctx)
    }
}

impl<Def, Src, State, DC> BatchPublishingDef for SubscriberBuilder<Def, Src, State, DC>
where
    Def: BatchPublishingDef,
    Src: Clone + Send + Sync,
    State: Send + Sync,
    DC: Send + Sync,
{
    type Input = <Def as BatchPublishingDef>::Input;
    type Injections = <Def as BatchPublishingDef>::Injections;
    type Reply = <Def as BatchPublishingDef>::Reply;
    type Source = Src;

    forward_common!(BatchPublishingDef);
    forward_outgoing!(BatchPublishingDef);

    fn reply_name(&self) -> &str {
        self.def.reply_name()
    }

    fn page_cap(&self) -> Option<std::num::NonZeroUsize> {
        self.def.page_cap()
    }
}

impl<Def, Src, State, DC, S> BatchPublishingCall<S> for SubscriberBuilder<Def, Src, State, DC>
where
    Def: BatchPublishingCall<S>,
    Src: Clone + Send + Sync,
    State: Send + Sync,
    DC: Send + Sync,
{
    fn call(
        &self,
        batch: &[<<Def as BatchPublishingDef>::Input as InputKind>::Owned],
        injections: &<Def as BatchPublishingDef>::Injections,
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Vec<<Def as BatchPublishingDef>::Reply>, BatchResult>> + Send
    {
        self.def.call(batch, injections, ctx)
    }
}

impl<Def: HasSlots, Src, State, DC> HasSlots for SubscriberBuilder<Def, Src, State, DC> {
    type Markers = Def::Markers;
}

// Binding the slots instantiates the publisher-generic definition; the settings and the source
// ride along, so the bound value is the same builder over the bound definition.
impl<Def, Src, State, DC, C, Sources> BindSlots<C, Sources>
    for SubscriberBuilder<Def, Src, State, DC>
where
    C: ConnectedBroker,
    Def: BindSlots<C, Sources>,
{
    type Bound = SubscriberBuilder<Def::Bound, Src, State, DC>;
    type Extra = Def::Extra;

    fn bind(self, sources: Sources) -> (Self::Bound, Self::Extra) {
        let (def, source, workers, failures, codec) = self.into_parts();
        let (bound, extra) = def.bind(sources);
        (
            SubscriberBuilder {
                def: bound,
                source,
                workers,
                failures,
                codec,
                _state: PhantomData,
            },
            extra,
        )
    }
}
