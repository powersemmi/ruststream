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

/// Hands a mount's output through a projection of the bound the mount needs, on a scope's chain
/// alone.
///
/// A mount whose bound fails reports it at the `include` that wrote it. Its output is read
/// through the projection that bound provides (the witness), so on a failed bound the output has
/// no type and the compiler says nothing more about the chain built on it. The poison applies
/// only to a list that ends in a [`ChainHead`], where the broker is known: a [`Router`] built on
/// its own ends in `()`, passes the output through unchanged and keeps inferring its broker from
/// the place it is returned to. Machinery; never named directly.
#[doc(hidden)]
pub trait PoisonAt {
    /// The mount's output, through the witness on a scope's chain.
    type Out<Witness, Out>;

    /// Hands the output over as that type.
    fn poison<Witness, Out>(out: Out) -> Self::Out<Witness, Out>;
}

impl PoisonAt for () {
    type Out<Witness, Out> = Out;

    fn poison<Witness, Out>(out: Out) -> Out {
        out
    }
}

impl<Route, Tail: PoisonAt> PoisonAt for (Route, Tail) {
    type Out<Witness, Out> = Tail::Out<Witness, Out>;

    fn poison<Witness, Out>(out: Out) -> Self::Out<Witness, Out> {
        Tail::poison::<Witness, Out>(out)
    }
}

impl<Pre> PoisonAt for ChainHead<Pre> {
    type Out<Witness, Out> = <Witness as Poisoned<Out>>::Out;

    fn poison<Witness, Out>(out: Out) -> Self::Out<Witness, Out> {
        Witness::pass(out)
    }
}

// A router answers for its route list: its chain steps read their output through it.
impl<B, Routes: PoisonAt, RouteCodec, RouteLayers, RoutePipe> PoisonAt
    for Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>
{
    type Out<Witness, Out> = Routes::Out<Witness, Out>;

    fn poison<Witness, Out>(out: Out) -> Self::Out<Witness, Out> {
        Routes::poison::<Witness, Out>(out)
    }
}

/// Carries an output through any witness: what [`PoisonAt`] reads a scope chain's output
/// through. Machinery; never named directly.
#[doc(hidden)]
pub trait Poisoned<Out> {
    /// The output, unchanged.
    type Out;

    /// Hands it over.
    fn pass(out: Out) -> Self::Out;
}

impl<Witness, Out> Poisoned<Out> for Witness {
    type Out = Out;

    fn pass(out: Out) -> Out {
        out
    }
}

/// The output of a mount on the route list `Routes`, read through `Witness` on a scope's chain.
pub(crate) type ScopeOut<Routes, Witness, Out> = <Routes as PoisonAt>::Out<Witness, Out>;

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
