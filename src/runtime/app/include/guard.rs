//! The guards a scope's `include` hands back: a router chain plus the scope it drains into.
//!
//! There are two of them, because a registration has two ways to finish. One that is complete as
//! it stands commits when the guard drops at the end of the statement ([`Mounting`]), so the type
//! carries no `#[must_use]`: dropping it IS the registration. One carrying
//! [`Out`](crate::runtime::Out) slots has nothing to commit until every slot is bound, so its
//! terminal is a call ([`MountingSlots::build`]) and the type says so with `#[must_use]` - a
//! forgotten `.build()` is then a warning at the mount site rather than a panic at startup.

use std::fmt;

use crate::Broker;

use crate::runtime::middleware::BlanketLayer;
use crate::runtime::publish::PublishPipeline;
use crate::runtime::router::{MapPublisher, Router, RouterCommit, RouterDef, RouterWith};
use crate::runtime::slot::{
    BatchTransformLast, BindAt, CodecLast, MapPolicyLast, NamedStep, ReplyStep, TransactionalLast,
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

/// The guard [`BrokerScope::include`](crate::runtime::BrokerScope::include) hands back for a
/// registration that is complete as it stands: one router mount chain and the scope it drains
/// into.
///
/// Every step forwards to the chain underneath, so the vocabulary, the typestate and the
/// diagnostics are the router's. Dropping the guard at the end of the statement is what commits,
/// which is why the type is not `#[must_use]`: `b.include(respond).out(Reply, Publish);` is a
/// whole registration. A registration whose [`Out`](crate::runtime::Out) slots have to be bound
/// first gets [`MountingSlots`] instead.
pub struct Mounting<'s, B, Layers, C, State, Pipeline, Chain>
where
    B: Broker + 'static,
    Chain: ScopeCommit<B, Layers, C, State, Pipeline>,
{
    // Options only so a step can move the pieces into the next state out of a Drop type; both
    // stay `Some` until the terminal or that replacement takes them.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    chain: Option<Chain>,
}

impl<'s, B, Layers, C, State, Pipeline, Chain> Mounting<'s, B, Layers, C, State, Pipeline, Chain>
where
    B: Broker + 'static,
    Chain: ScopeCommit<B, Layers, C, State, Pipeline>,
{
    pub(super) fn new(
        chain: Chain,
        scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) -> Self {
        Self {
            scope: Some(scope),
            chain: Some(chain),
        }
    }

    /// Rebuilds the guard over a stepped chain: how every forwarded step reaches the chain.
    ///
    /// # Panics
    ///
    /// Never in practice: both pieces stay present until the terminal or a step takes them.
    fn map_chain<NewChain>(
        mut self,
        f: impl FnOnce(Chain) -> NewChain,
    ) -> Mounting<'s, B, Layers, C, State, Pipeline, NewChain>
    where
        NewChain: ScopeCommit<B, Layers, C, State, Pipeline>,
    {
        let chain = self
            .chain
            .take()
            .expect("the guard holds its chain until the terminal or a step takes it");
        let scope = self
            .scope
            .take()
            .expect("the guard holds its scope until the terminal or a step takes it");
        Mounting::new(f(chain), scope)
    }
}

/// The guard over a stepped chain: what each forwarded step of [`Mounting`] returns.
type Stepped<'s, B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last> =
    Mounting<'s, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>>;

/// The chain one step of [`Mounting`] produces, named once so the bound below reads.
type SteppedChain<Mount, R, Def, Attach, Last> = RouterWith<Mount, R, Def, Attach, Last>;

impl<'s, B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last>
    Mounting<'s, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>>
where
    B: Broker + 'static,
    RouterWith<Mount, R, Def, Attach, Last>: ScopeCommit<B, Layers, C, State, Pipeline>,
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
    >
    where
        Attach: BindAt<Mount, M, Policy, Index>,
        SteppedChain<Mount, R, Def, <Attach as BindAt<Mount, M, Policy, Index>>::Out, Index>:
            ScopeCommit<B, Layers, C, State, Pipeline>,
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
    >
    where
        Attach: CodecLast<Cd, Last, Step: NamedStep>,
        SteppedChain<Mount, R, Def, <Attach as CodecLast<Cd, Last>>::Out, Last>:
            ScopeCommit<B, Layers, C, State, Pipeline>,
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
    >
    where
        Attach: TransformLast<N, Last, Step: NamedStep>,
        SteppedChain<Mount, R, Def, <Attach as TransformLast<N, Last>>::Out, Last>:
            ScopeCommit<B, Layers, C, State, Pipeline>,
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
    >
    where
        Attach: BatchTransformLast<N, Last, Step: ReplyStep>,
        SteppedChain<Mount, R, Def, <Attach as BatchTransformLast<N, Last>>::Out, Last>:
            ScopeCommit<B, Layers, C, State, Pipeline>,
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
    >
    where
        Attach: TransactionalLast<Last, Step: ReplyStep>,
        SteppedChain<Mount, R, Def, <Attach as TransactionalLast<Last>>::Out, Last>:
            ScopeCommit<B, Layers, C, State, Pipeline>,
    {
        self.map_chain(RouterWith::transactional)
    }
}

// The broker's own publisher settings reach a scope registration exactly as they reach a router
// one: through the chain the guard forwards to. The replacement policy has the same type, so the
// chain the guard hands on is the one it already held.
impl<B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last> MapPublisher
    for Mounting<'_, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>>
where
    B: Broker + 'static,
    Attach: MapPolicyLast<Last, Step: NamedStep>,
    RouterWith<Mount, R, Def, Attach, Last>: ScopeCommit<B, Layers, C, State, Pipeline>,
{
    type Policy = Attach::Policy;

    fn map_publisher(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self {
        self.map_chain(|chain| chain.map_publisher(f))
    }
}

impl<B, Layers, C, State, Pipeline, Chain> fmt::Debug
    for Mounting<'_, B, Layers, C, State, Pipeline, Chain>
where
    B: Broker + 'static,
    Chain: ScopeCommit<B, Layers, C, State, Pipeline>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mounting").finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, Chain> Drop
    for Mounting<'_, B, Layers, C, State, Pipeline, Chain>
where
    B: Broker + 'static,
    Chain: ScopeCommit<B, Layers, C, State, Pipeline>,
{
    fn drop(&mut self) {
        if let (Some(chain), Some(scope)) = (self.chain.take(), self.scope.take()) {
            chain.commit_into(scope);
        }
    }
}

/// The guard a registration carrying [`Out`](crate::runtime::Out) slots gets.
///
/// It holds the same chain as [`Mounting`], finished by [`build`](Self::build) once every slot is
/// bound. A chain that still holds an unbound slot has nothing to commit, so this guard's
/// terminal is a call rather than a drop - and the type is `#[must_use]` for exactly that reason:
/// a chain dropped before `.build()` registered nothing, so the warning lands on the mount site
/// that forgot it. The drop of such a chain panics rather than starting a service whose handler
/// would never consume.
#[must_use = "a handler with Out slots is not mounted until `.build()` commits it: bind every \
              slot with `.out(marker, policy)`, then finish the chain with `.build()`"]
pub struct MountingSlots<'s, B, Layers, C, State, Pipeline, Chain>
where
    B: Broker + 'static,
{
    // See `Mounting`: the pieces move out of a Drop type one step at a time.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    chain: Option<Chain>,
}

impl<'s, B, Layers, C, State, Pipeline, Chain>
    MountingSlots<'s, B, Layers, C, State, Pipeline, Chain>
where
    B: Broker + 'static,
{
    pub(super) fn new(
        chain: Chain,
        scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) -> Self {
        Self {
            scope: Some(scope),
            chain: Some(chain),
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
    fn map_chain<NewChain>(
        self,
        f: impl FnOnce(Chain) -> NewChain,
    ) -> MountingSlots<'s, B, Layers, C, State, Pipeline, NewChain> {
        let (chain, scope) = self.take();
        MountingSlots::new(f(chain), scope)
    }

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

/// The guard over a stepped chain: what each forwarded step of [`MountingSlots`] returns.
type SteppedSlots<'s, B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last> =
    MountingSlots<'s, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>>;

impl<'s, B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last>
    MountingSlots<'s, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>>
where
    B: Broker + 'static,
{
    /// See [`RouterWith::out`]: names the publish policy of one position, the reply's
    /// ([`Reply`](crate::runtime::Reply)) or one slot's.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn out<M, Policy, Index>(
        self,
        marker: M,
        policy: Policy,
    ) -> SteppedSlots<
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
    >
    where
        Attach: BindAt<Mount, M, Policy, Index>,
    {
        self.map_chain(|chain| chain.out(marker, policy))
    }

    /// See [`RouterWith::codec`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn codec<Cd>(
        self,
        codec: Cd,
    ) -> SteppedSlots<
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
    >
    where
        Attach: CodecLast<Cd, Last, Step: NamedStep>,
    {
        self.map_chain(|chain| chain.codec(codec))
    }

    /// See [`RouterWith::transform`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn transform<N>(
        self,
        transform: N,
    ) -> SteppedSlots<
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
    >
    where
        Attach: TransformLast<N, Last, Step: NamedStep>,
    {
        self.map_chain(|chain| chain.transform(transform))
    }

    /// See [`RouterWith::batch_transform`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn batch_transform<N>(
        self,
        transform: N,
    ) -> SteppedSlots<
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
    >
    where
        Attach: BatchTransformLast<N, Last, Step: ReplyStep>,
    {
        self.map_chain(|chain| chain.batch_transform(transform))
    }

    /// See [`RouterWith::transactional`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn transactional(
        self,
    ) -> SteppedSlots<
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
    >
    where
        Attach: TransactionalLast<Last, Step: ReplyStep>,
    {
        self.map_chain(RouterWith::transactional)
    }
}

// See `Mounting`'s own impl: the settings hook is the chain's, forwarded.
impl<B, Layers, C, State, Pipeline, Mount, R, Def, Attach, Last> MapPublisher
    for MountingSlots<'_, B, Layers, C, State, Pipeline, RouterWith<Mount, R, Def, Attach, Last>>
where
    B: Broker + 'static,
    Attach: MapPolicyLast<Last, Step: NamedStep>,
{
    type Policy = Attach::Policy;

    fn map_publisher(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self {
        self.map_chain(|chain| chain.map_publisher(f))
    }
}

impl<B, Layers, C, State, Pipeline, Chain> fmt::Debug
    for MountingSlots<'_, B, Layers, C, State, Pipeline, Chain>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MountingSlots").finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, Chain> Drop
    for MountingSlots<'_, B, Layers, C, State, Pipeline, Chain>
where
    B: Broker + 'static,
{
    fn drop(&mut self) {
        // The `must_use` warning is the diagnostic that belongs at the mount site; this is the
        // backstop for a mount that silenced or ignored it, like the on_startup ordering check.
        // A deliberately discarded incomplete registration must not vanish quietly - the handler
        // would never consume.
        assert!(
            self.chain.is_none(),
            "a handler with Out slots was included but never mounted: bind each slot with \
             .out(marker, policy) and finish the chain with .build()"
        );
    }
}
