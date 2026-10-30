//! The head of a broker scope's mount chain: what the scope registered before its first
//! `include`, carried at the bottom of the chain's route list.
//!
//! A scope is a value its `with_broker` closure consumes. A startup publish, a whole router or a
//! hand-built subscription registered on it grows a list of its own (`Pre`), and the scope's
//! `include` opens the router chain over that list, so the registrations ride the chain the
//! closure returns. The runtime that mounts the chain splits the head back off and registers it
//! before the routes. Each runtime decides what its list holds; the router only carries it.
//!
//! Machinery for the runtimes; a service never names it.

use super::builder::Router;
use super::routes::RetryDestinationsDeclared;
use crate::{Broker, Identity};

use core::marker::PhantomData;

/// The bottom of a scope's route list: the registrations the scope made before its first
/// `include`. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct ChainHead<Pre>(pub Pre);

/// Splits a route list into the scope's head and the routes mounted after it.
///
/// Implemented for [`ChainHead`] and for every route list built on top of one. A list that ends
/// in `()` (a [`Router`] built on its own) has no head and does not implement it. Machinery;
/// never named directly.
#[doc(hidden)]
pub trait SplitHead {
    /// What the scope registered before its first `include`.
    type Pre;
    /// The routes mounted after it, as an ordinary list ending in `()`.
    type Rest;

    /// Takes the list apart.
    fn split_head(self) -> (Self::Pre, Self::Rest);
}

impl<Pre> SplitHead for ChainHead<Pre> {
    type Pre = Pre;
    type Rest = ();

    fn split_head(self) -> (Pre, ()) {
        (self.0, ())
    }
}

impl<Route, Tail: SplitHead> SplitHead for (Route, Tail) {
    type Pre = Tail::Pre;
    type Rest = (Route, Tail::Rest);

    fn split_head(self) -> (Self::Pre, Self::Rest) {
        let (pre, rest) = self.1.split_head();
        (pre, (self.0, rest))
    }
}

// The head names no retry position of its own, so it answers for nothing a `.to(..)` could owe.
impl<Pre> RetryDestinationsDeclared for ChainHead<Pre> {}

impl<B: Broker + 'static, Pre, RouteCodec, RoutePipe>
    Router<B, ChainHead<Pre>, RouteCodec, Identity, RoutePipe>
{
    /// The chain a broker scope opens: its registrations so far as the head, its codec and its
    /// publish pipeline, no router-scope layers of its own (the app's stack wraps at the mount).
    #[doc(hidden)]
    pub const fn for_scope_with(head: Pre, codec: RouteCodec, pipeline: RoutePipe) -> Self {
        Self {
            routes: ChainHead(head),
            codec,
            layers: Identity,
            pipeline,
            _broker: PhantomData,
        }
    }
}

impl<B: Broker + 'static, Routes: SplitHead, RouteCodec, RouteLayers, RoutePipe>
    Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>
{
    /// Takes a scope's finished chain apart: the head the scope registered first, and a router
    /// over the routes mounted after it, with the codec, layers and pipeline unchanged.
    #[doc(hidden)]
    #[allow(clippy::type_complexity)] // the two halves of the chain's own type
    pub fn split_head(
        self,
    ) -> (
        Routes::Pre,
        Router<B, Routes::Rest, RouteCodec, RouteLayers, RoutePipe>,
    ) {
        let (pre, rest) = self.routes.split_head();
        (
            pre,
            Router {
                routes: rest,
                codec: self.codec,
                layers: self.layers,
                pipeline: self.pipeline,
                _broker: PhantomData,
            },
        )
    }
}
