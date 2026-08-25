//! Mount form for Out injection.

use crate::{Broker, BuildContext, Connected, SubscriptionSource};

use crate::runtime::handler::Handler;
use crate::runtime::inject::{FromStartup, InjectCall, InjectHandler};
use crate::runtime::input::DecodeWith;
use crate::runtime::middleware::Layer;
use crate::runtime::slot::{BindSlots, HasSlots, InitSlots, IntoSlotSource, WithSource};
use crate::runtime::{SourceMessage, SourceSubscriber};

use super::{IncludeMount, IncludeOut, IncludeSlots, InjectMount, MountCodec, SlotCommit, forms};
use crate::runtime::app::scope::BrokerScope;

// ---------------------------------------------------------------------------------------------
// Out injection: the attachment is a positional slot tuple, one element per marker, starting
// all-unbound. `.publisher(..)` is the single-slot shorthand (it binds and commits in one
// call); `.out(marker, ..)` binds one named position and `.mount()` commits - it compiles only
// once every position is bound.

/// Implements the slot-tuple commit of the plain Out form for each slot arity, for fully-bound
/// tuples only. `Bound` / `Extra` name the definition's [`BindSlots`] outputs so the bounds
/// read flat instead of through `<Def::Bound as ..>` projections.
macro_rules! impl_inject_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Bound, Extra, $($attach),+>
            SlotCommit<InjectMount, B, Layers, C, State, Pipeline, Def>
            for ($(WithSource<$attach>,)+)
        where
            B: Broker + 'static,
            C: MountCodec,
            Def: BindSlots<Connected<B>, ($(($attach, C::Codec),)+), Bound = Bound, Extra = Extra>,
            Bound: InjectCall<State> + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + Sync + 'static,
            Bound::Input: DecodeWith<C::Codec>,
            Bound::Context: BuildContext<SourceMessage<B, Bound::Source>> + Send + Sync + 'static,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Extra: Send + Sync + 'static,
            State: Send + Sync + 'static,
            Layers: Layer<InjectHandler<Bound, C::Codec>> + Clone + Send + 'static,
            Layers::Handler:
                Handler<SourceMessage<B, Bound::Source>, Bound::Context, State> + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                let codec = scope.codec.mount_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                scope.mount_inject(source, def, extra);
            }
        }
    )+};
}

impl_inject_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Out
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
        IncludeOut<'s, B, Layers, C, State, Pipeline, Def, <Def::Markers as InitSlots>::Init>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeSlots::new(def, <Def::Markers as InitSlots>::init(), scope)
    }
}
