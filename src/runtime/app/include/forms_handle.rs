//! Scope mounts for the sealed manual-path reply forms: the chain already carries the reply
//! attach (a policy, or a default marker), so the registration commits right here instead of
//! handing back a builder.

use crate::Broker;

use crate::runtime::app::scope::BrokerScope;
use crate::runtime::handle::{
    SealedBatchPublishing, SealedBatchPublishingOut, SealedPublishing, SealedPublishingOut,
    SealedRawReply, SealedRawReplyOut, SplitAttach,
};
use crate::runtime::slot::{HasSlots, InitSlots};

use super::{
    BatchPublishInjectMount, BatchPublishMount, CommitVia, IncludeMount, IncludeSlotsWithReply,
    PublishInjectMount, PublishMount, RawReplyInjectMount, RawReplyMount,
};

/// Implements the scope mount of one sealed reply token: split the attach off and commit it
/// through the same machinery a post-include `.publisher(..)` resolves.
macro_rules! sealed_reply_scope_mount {
    ($($token:ty => $mount:ty),+ $(,)?) => {$(
        impl<'s, B, Layers, C, State, Pipeline, Def>
            IncludeMount<'s, B, Layers, C, State, Pipeline, Def> for $token
        where
            B: Broker + 'static,
            Def: SplitAttach,
            Def::Attach: CommitVia<$mount, B, Layers, C, State, Pipeline, Def::Rest>,
        {
            type Out = ();

            fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
                let (rest, attach) = def.split_attach();
                attach.commit(rest, scope);
            }
        }
    )+};
}

sealed_reply_scope_mount! {
    SealedPublishing => PublishMount,
    SealedRawReply => RawReplyMount,
    SealedBatchPublishing => BatchPublishMount,
}

/// Implements the scope mount of one sealed reply-and-slots token: split the attach off and
/// seed the slot binder with it.
macro_rules! sealed_reply_out_scope_mount {
    ($($token:ty => $mount:ty),+ $(,)?) => {$(
        impl<'s, B, Layers, C, State, Pipeline, Def>
            IncludeMount<'s, B, Layers, C, State, Pipeline, Def> for $token
        where
            B: Broker + 'static,
            Layers: 's,
            C: 's,
            State: 's,
            Pipeline: 's,
            Def: SplitAttach,
            Def::Rest: HasSlots,
            <Def::Rest as HasSlots>::Markers: InitSlots,
        {
            type Out = IncludeSlotsWithReply<
                's,
                $mount,
                B,
                Layers,
                C,
                State,
                Pipeline,
                Def::Rest,
                Def::Attach,
                <<Def::Rest as HasSlots>::Markers as InitSlots>::Init,
            >;

            fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
                let (rest, attach) = def.split_attach();
                IncludeSlotsWithReply::new(
                    rest,
                    attach,
                    <<Def::Rest as HasSlots>::Markers as InitSlots>::init(),
                    scope,
                )
            }
        }
    )+};
}

sealed_reply_out_scope_mount! {
    SealedPublishingOut => PublishInjectMount,
    SealedRawReplyOut => RawReplyInjectMount,
    SealedBatchPublishingOut => BatchPublishInjectMount,
}
