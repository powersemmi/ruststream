//! Scope mounts for the sealed manual-path reply forms: the chain already carries the reply
//! attach (a policy, or a default marker), so the registration commits right here instead of
//! handing back a builder.

use crate::Broker;

use crate::runtime::app::scope::BrokerScope;
use crate::runtime::handle::{
    SealedBatchPublishing, SealedPublishing, SealedRawReply, SplitAttach,
};

use super::{BatchPublishMount, CommitVia, IncludeMount, PublishMount};

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
    SealedRawReply => PublishMount,
    SealedBatchPublishing => BatchPublishMount,
}
