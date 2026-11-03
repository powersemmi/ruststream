//! How many bytes a registration's reply can carry, read off the publisher the reply leaves
//! through.

#[allow(unused_imports)] // named by the doc links
use ruststream_core::Capped;

use ruststream_core::{PayloadForm, Unbounded};

use super::fold_retry::OneTaskRetriedRoute;
use super::routes::{BatchRoute, DeclaredRoute, SubscribeRoute};
use super::routes_inject::{BatchInjectRoute, InjectRoute};
use super::routes_publish::{BatchPublishingRoute, PublishingRoute, RawReplyRoute};
use crate::publish::{RawReplyWiring, ReplyWiring};
use crate::{Broker, Connected, PublishPolicy, Publisher};

/// The room a registration's reply can carry on broker `B`: unbounded where the reply publisher
/// only reads the bytes (`Lend`), and the room of the codec's own buffer where it keeps them
/// (`Take`). A mount that asks its workers for more room reads the type off here, so the error
/// names both numbers. Machinery; never named directly.
#[doc(hidden)]
pub trait ReplyRoom<B: Broker> {
    /// The room, as a type: [`Unbounded`] or [`Capped`].
    type Room;
}

impl<B: Broker, Source, Handler, Cx> ReplyRoom<B> for SubscribeRoute<Source, Handler, Cx> {
    type Room = Unbounded;
}

impl<B: Broker, Source, Handler, Cx, Size> ReplyRoom<B> for BatchRoute<Source, Handler, Cx, Size> {
    type Room = Unbounded;
}

impl<B: Broker, Source, Def, DecodeCodec, Extra> ReplyRoom<B>
    for InjectRoute<Source, Def, DecodeCodec, Extra>
{
    type Room = Unbounded;
}

impl<B: Broker, Source, Def, DecodeCodec, Extra, Size> ReplyRoom<B>
    for BatchInjectRoute<Source, Def, DecodeCodec, Extra, Size>
{
    type Room = Unbounded;
}

impl<B, Source, Def, DecodeCodec, Policy, Enc, PL, BL, Tx, Extra> ReplyRoom<B>
    for PublishingRoute<Source, Def, DecodeCodec, ReplyWiring<Policy, Enc, PL, BL, Tx>, Extra>
where
    B: Broker,
    Policy: PublishPolicy<Connected<B>, Live: Publisher>,
{
    type Room = <<Policy::Live as Publisher>::Payload as PayloadForm>::Room;
}

impl<B, Source, Def, DecodeCodec, Policy, Enc, PL, BL, Tx, Extra, Size> ReplyRoom<B>
    for BatchPublishingRoute<
        Source,
        Def,
        DecodeCodec,
        ReplyWiring<Policy, Enc, PL, BL, Tx>,
        Extra,
        Size,
    >
where
    B: Broker,
    Policy: PublishPolicy<Connected<B>, Live: Publisher>,
{
    type Room = <<Policy::Live as Publisher>::Payload as PayloadForm>::Room;
}

impl<B, Source, Def, DecodeCodec, Policy, Extra> ReplyRoom<B>
    for RawReplyRoute<Source, Def, DecodeCodec, RawReplyWiring<Policy>, Extra>
where
    B: Broker,
    Policy: PublishPolicy<Connected<B>, Live: Publisher>,
{
    type Room = <<Policy::Live as Publisher>::Payload as PayloadForm>::Room;
}

impl<B: Broker, Route: ReplyRoom<B>, Dead> ReplyRoom<B> for DeclaredRoute<Route, Dead> {
    type Room = <Route as ReplyRoom<B>>::Room;
}

impl<B: Broker, Route: ReplyRoom<B>, Policy, Stack, Settled> ReplyRoom<B>
    for OneTaskRetriedRoute<Route, Policy, Stack, Settled>
{
    type Room = <Route as ReplyRoom<B>>::Room;
}
