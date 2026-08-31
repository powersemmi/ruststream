//! Router mounts for the sealed manual-path reply-and-slots forms: the chain's reply attach
//! seeds the slot binder, which still takes one `.out(marker, policy)` per slot before its
//! terminal commits.

use crate::Broker;

use crate::runtime::handle::{
    SealedBatchPublishingOut, SealedPublishingOut, SealedRawReplyOut, SplitAttach,
};
use crate::runtime::slot::{HasSlots, InitSlots};

use super::builder::Router;
use super::builders::RouterSlotsWithReply;
use super::mount::{
    BatchPublishInjectMount, PublishInjectMount, RawReplyInjectMount, RouterMount,
};

/// Implements the router mount of one sealed reply-and-slots token: split the attach off and
/// seed the slot binder with it.
macro_rules! sealed_reply_out_router_mount {
    ($($token:ty => $mount:ty),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def>
            RouterMount<B, Routes, RouteCodec, RouteLayers, Def> for $token
        where
            B: Broker + 'static,
            Def: SplitAttach,
            Def::Rest: HasSlots,
            <Def::Rest as HasSlots>::Markers: InitSlots,
        {
            type Out = RouterSlotsWithReply<
                $mount,
                B,
                Routes,
                RouteCodec,
                RouteLayers,
                Def::Rest,
                Def::Attach,
                <<Def::Rest as HasSlots>::Markers as InitSlots>::Init,
            >;

            fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
                let (rest, attach) = def.split_attach();
                RouterSlotsWithReply::new(
                    rest,
                    attach,
                    <<Def::Rest as HasSlots>::Markers as InitSlots>::init(),
                    router,
                )
            }
        }
    )+};
}

sealed_reply_out_router_mount! {
    SealedPublishingOut => PublishInjectMount,
    SealedRawReplyOut => RawReplyInjectMount,
    SealedBatchPublishingOut => BatchPublishInjectMount,
}
