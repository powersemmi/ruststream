//! The guard a scope's `include` hands back: a router chain plus the scope it drains into.

use std::fmt;
use std::marker::PhantomData;

use crate::Broker;

use crate::runtime::middleware::BlanketLayer;
use crate::runtime::publish::PublishPipeline;
use crate::runtime::router::{MapPublisher, Router, RouterCommit, RouterDef, RouterWith};
use crate::runtime::slot::{
    BatchTransformLast, BindAt, CodecLast, MapPolicyLast, NamedStep, TransactionalLast,
    TransformLast,
};

use crate::runtime::app::scope::BrokerScope;

/// A mount chain that can be drained into a scope's sink. Machinery; never named directly.
#[doc(hidden)]
pub trait ScopeCommit<B: Broker, Layers, C, State, Pipeline>: Sized {
    fn commit_into(self, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

// A finished chain is a router: mounting it is the same operation `include_router` performs, so
// a scope registration and a router one reach the sink through one path.
impl<B, Layers, C, State, Pipeline, Routes, RC, RL, RP> ScopeCommit<B, Layers, C, State, Pipeline>
    for Router<B, Routes, RC, RL, RP>
where
    B: Broker + 'static,
    Routes: RouterDef<B, State>,
    RL: BlanketLayer + Clone + Send + Sync + 'static,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit_into(self, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        RouterDef::mount(self, &scope.global, &scope.pipeline, &mut scope.sink);
    }
}

// An unfinished chain commits by running the router's own terminal first.
impl<B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last>
    ScopeCommit<B, Layers, C, State, Pipeline> for RouterWith<Mount, R, Def, Attach, Last>
where
    B: Broker + 'static,
    Attach: RouterCommit<Mount, R, Def>,
    Attach::Out: ScopeCommit<B, Layers, C, State, Pipeline>,
{
    fn commit_into(self, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        self.build().commit_into(scope);
    }
}

/// How one form's guard finishes: which of the two terminals runs, and what a dropped guard that
/// never reached it does. Machinery; never named directly.
#[doc(hidden)]
pub trait ScopeTerminal<B: Broker, Layers, C, State, Pipeline, Chain> {
    /// Runs when the guard is dropped still holding its chain.
    fn on_drop(chain: Chain, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

/// The terminal of a registration that is complete as it stands: dropping the guard at the end of
/// the statement is what commits it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct OnDrop;

impl<B, Layers, C, State, Pipeline, Chain> ScopeTerminal<B, Layers, C, State, Pipeline, Chain>
    for OnDrop
where
    B: Broker + 'static,
    Chain: ScopeCommit<B, Layers, C, State, Pipeline>,
{
    fn on_drop(chain: Chain, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        chain.commit_into(scope);
    }
}

/// The terminal of a registration that is not complete until every [`Out`](crate::runtime::Out)
/// slot is bound: [`build`](Mounting::build) commits, and a guard dropped before it registered
/// nothing.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct OnBuild;

impl<B, Layers, C, State, Pipeline, Chain> ScopeTerminal<B, Layers, C, State, Pipeline, Chain>
    for OnBuild
where
    B: Broker + 'static,
{
    fn on_drop(_chain: Chain, _scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // A build-time assert, like the on_startup ordering check: `must_use` already warns, but
        // a deliberately discarded incomplete registration must not silently vanish - the
        // handler would never consume.
        panic!(
            "a handler with Out slots was included but never mounted: bind each slot with \
             .out(marker, policy) and finish the chain with .build()"
        );
    }
}

/// The guard [`BrokerScope::include`](crate::runtime::BrokerScope::include) hands back: one
/// router mount chain, the scope it drains into, and the terminal that finishes it.
///
/// Every step forwards to the chain underneath, so the vocabulary, the typestate and the
/// diagnostics are the router's. `Term` decides the terminal: [`OnDrop`] for a registration that
/// is complete as it stands, [`OnBuild`] for one whose [`Out`](crate::runtime::Out) slots are
/// bound first.
#[must_use = "an Out handler registers nothing until .out(marker, policy) per slot and .build() commit it"]
pub struct Mounting<'s, B, Layers, C, State, Pipeline, Chain, Term>
where
    B: Broker + 'static,
    Term: ScopeTerminal<B, Layers, C, State, Pipeline, Chain>,
{
    // Options only so a step can move the pieces into the next state out of a Drop type; both
    // stay `Some` until the terminal or that replacement takes them.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    chain: Option<Chain>,
    _terminal: PhantomData<fn() -> Term>,
}

impl<'s, B, Layers, C, State, Pipeline, Chain, Term>
    Mounting<'s, B, Layers, C, State, Pipeline, Chain, Term>
where
    B: Broker + 'static,
    Term: ScopeTerminal<B, Layers, C, State, Pipeline, Chain>,
{
    pub(super) fn new(
        chain: Chain,
        scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) -> Self {
        Self {
            scope: Some(scope),
            chain: Some(chain),
            _terminal: PhantomData,
        }
    }

    /// The chain and the scope, moved out without running the terminal.
    ///
    /// # Panics
    ///
    /// Never in practice: both stay present until the terminal or a step takes them.
    fn take(mut self) -> (Chain, &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let chain = self
            .chain
            .take()
            .expect("the guard holds its chain until the terminal or a step takes it");
        let scope = self
            .scope
            .take()
            .expect("the guard holds its scope until the terminal or a step takes it");
        (chain, scope)
    }

    /// Rebuilds the guard over a stepped chain: how every forwarded step reaches the chain.
    ///
    /// # Panics
    ///
    /// Never in practice: see [`take`](Self::take).
    fn map_chain<NewChain, NewTerm>(
        self,
        f: impl FnOnce(Chain) -> NewChain,
    ) -> Mounting<'s, B, Layers, C, State, Pipeline, NewChain, NewTerm>
    where
        NewTerm: ScopeTerminal<B, Layers, C, State, Pipeline, NewChain>,
    {
        let (chain, scope) = self.take();
        Mounting::new(f(chain), scope)
    }
}

/// The guard over a stepped chain: what each forwarded step returns.
type Stepped<'s, B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last, Term> =
    Mounting<'s, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>, Term>;

impl<'s, B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last, Term>
    Mounting<'s, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>, Term>
where
    B: Broker + 'static,
    Term: ScopeTerminal<B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>>,
{
    /// See [`RouterWith::out`]: names the publish policy of one position, the reply's
    /// ([`Reply`](crate::runtime::Reply)) or one slot's.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn out<M, Policy, Index>(
        self,
        marker: M,
        policy: Policy,
    ) -> Stepped<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Mount,
        R,
        Def,
        <Attach as BindAt<Mount, M, Policy, Index>>::Out,
        Index,
        Term,
    >
    where
        Attach: BindAt<Mount, M, Policy, Index>,
        Term: ScopeTerminal<
                B,
                Layers,
                C,
                State,
                Pipeline,
                RouterWith<Mount, R, Def, <Attach as BindAt<Mount, M, Policy, Index>>::Out, Index>,
            >,
    {
        self.map_chain(|chain| chain.out(marker, policy))
    }

    /// See [`RouterWith::codec`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn codec<Cd>(
        self,
        codec: Cd,
    ) -> Stepped<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Mount,
        R,
        Def,
        <Attach as CodecLast<Cd, Last>>::Out,
        Last,
        Term,
    >
    where
        Attach: CodecLast<Cd, Last, Step: NamedStep>,
        Term: ScopeTerminal<
                B,
                Layers,
                C,
                State,
                Pipeline,
                RouterWith<Mount, R, Def, <Attach as CodecLast<Cd, Last>>::Out, Last>,
            >,
    {
        self.map_chain(|chain| chain.codec(codec))
    }

    /// See [`RouterWith::transform`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn transform<N>(
        self,
        transform: N,
    ) -> Stepped<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Mount,
        R,
        Def,
        <Attach as TransformLast<N, Last>>::Out,
        Last,
        Term,
    >
    where
        Attach: TransformLast<N, Last, Step: NamedStep>,
        Term: ScopeTerminal<
                B,
                Layers,
                C,
                State,
                Pipeline,
                RouterWith<Mount, R, Def, <Attach as TransformLast<N, Last>>::Out, Last>,
            >,
    {
        self.map_chain(|chain| chain.transform(transform))
    }

    /// See [`RouterWith::batch_transform`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn batch_transform<N>(
        self,
        transform: N,
    ) -> Stepped<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Mount,
        R,
        Def,
        <Attach as BatchTransformLast<N, Last>>::Out,
        Last,
        Term,
    >
    where
        Attach: BatchTransformLast<N, Last, Step: NamedStep>,
        Term: ScopeTerminal<
                B,
                Layers,
                C,
                State,
                Pipeline,
                RouterWith<Mount, R, Def, <Attach as BatchTransformLast<N, Last>>::Out, Last>,
            >,
    {
        self.map_chain(|chain| chain.batch_transform(transform))
    }

    /// See [`RouterWith::transactional`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn transactional(
        self,
    ) -> Stepped<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Mount,
        R,
        Def,
        <Attach as TransactionalLast<Last>>::Out,
        Last,
        Term,
    >
    where
        Attach: TransactionalLast<Last, Step: NamedStep>,
        Term: ScopeTerminal<
                B,
                Layers,
                C,
                State,
                Pipeline,
                RouterWith<Mount, R, Def, <Attach as TransactionalLast<Last>>::Out, Last>,
            >,
    {
        self.map_chain(RouterWith::transactional)
    }
}

// The broker's own publisher settings reach a scope registration exactly as they reach a router
// one: through the chain the guard forwards to. The replacement policy has the same type, so the
// guard's terminal is the one it already had.
impl<B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last, Term> MapPublisher
    for Mounting<'_, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>, Term>
where
    B: Broker + 'static,
    Attach: MapPolicyLast<Last, Step: NamedStep>,
    Term: ScopeTerminal<B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>>,
{
    type Policy = Attach::Policy;

    fn map_publisher(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self {
        self.map_chain(|chain| chain.map_publisher(f))
    }
}

/// Commits a slot-carrying registration once every position is bound.
impl<'s, B, Layers, C, State, Pipeline, Chain>
    Mounting<'s, B, Layers, C, State, Pipeline, Chain, OnBuild>
where
    B: Broker + 'static,
{
    /// Commits the registration. Exists only once every [`Out`](crate::runtime::Out) slot is
    /// bound: a chain that still has a `MissingSlot<..>` in its attachment fails to compile here,
    /// naming the slot.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard invariants that hold until this terminal
    /// consumes them.
    pub fn build(self)
    where
        Chain: ScopeCommit<B, Layers, C, State, Pipeline>,
    {
        let (chain, scope) = self.take();
        chain.commit_into(scope);
    }
}

impl<B, Layers, C, State, Pipeline, Chain, Term> fmt::Debug
    for Mounting<'_, B, Layers, C, State, Pipeline, Chain, Term>
where
    B: Broker + 'static,
    Term: ScopeTerminal<B, Layers, C, State, Pipeline, Chain>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mounting").finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, Chain, Term> Drop
    for Mounting<'_, B, Layers, C, State, Pipeline, Chain, Term>
where
    B: Broker + 'static,
    Term: ScopeTerminal<B, Layers, C, State, Pipeline, Chain>,
{
    fn drop(&mut self) {
        if let (Some(chain), Some(scope)) = (self.chain.take(), self.scope.take()) {
            Term::on_drop(chain, scope);
        }
    }
}
