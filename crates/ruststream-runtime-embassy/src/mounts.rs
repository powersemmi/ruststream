//! The app's brokers as a typed list: each broker with the routes its scope returned, connected
//! and opened in the order they were registered, and shut down in the reverse order.

// Written over the local twins of the contract: the futures hold brokers and subscriptions that
// are not `Send`, and the app's one task polls them where it was spawned.
#![allow(clippy::future_not_send)]

use alloc::rc::Rc;
use core::any::type_name;
use core::fmt::{self, Debug, Formatter};
use core::future::ready;

use ruststream_core::{Broker, ConnectedBroker};
use ruststream_runtime::{
    OpenRoutes, RetryDestinationsDeclared, Router, RouterCommit, RouterWith, RunRoutes,
    ScopeDelivery,
};
use tracing::warn;

use crate::app::RunError;

/// What a broker's scope returns: the router chain, finished into the routes the app opens.
///
/// Implemented by [`Router`] and by an open mount chain ([`RouterWith`]), so a scope ends on
/// `.include(def)` or on a step such as `.out_reply(policy)` alike.
///
/// # Examples
///
/// See [`EmbeddedApp::with_broker`](crate::EmbeddedApp::with_broker).
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a mount chain of the embedded app",
    label = "the broker's scope returns this",
    note = "the closure of `with_broker` returns the router chain it was handed: \
            `|b| b.include(a).include(b)`"
)]
pub trait MountChain {
    /// The finished routes.
    type Routes;

    /// Finishes the chain: commits the registration a step left open.
    fn finish(self) -> Self::Routes;
}

impl<B, Routes, RouteCodec, Layers, Pipe> MountChain
    for Router<B, Routes, RouteCodec, Layers, Pipe>
{
    type Routes = Self;

    fn finish(self) -> Self {
        self
    }
}

impl<Mount, R, Def, Attach, Last> MountChain for RouterWith<Mount, R, Def, Attach, Last>
where
    Attach: RouterCommit<Mount, R, Def, Out: RetryDestinationsDeclared>,
{
    type Routes = Attach::Out;

    fn finish(self) -> Attach::Out {
        self.build()
    }
}

/// Where a broker's connected form is shared once the app has connected it: the slot a
/// [`Bindable`](crate::Bindable) fills for the tokens it minted, or [`NoSlot`] for a broker that
/// shares nothing.
#[doc(hidden)]
pub trait ConnectSlot<B: Broker> {
    /// Hands the slot the connected form, once the broker is connected.
    fn fill(&self, connected: &Rc<B::Connected>);

    /// Takes the connected form back out, before the broker shuts down.
    fn empty(&self);
}

/// The slot of a broker registered as it is: it shares its connected form with no one.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSlot;

impl<B: Broker> ConnectSlot<B> for NoSlot {
    fn fill(&self, _connected: &Rc<B::Connected>) {}

    fn empty(&self) {}
}

/// What `with_broker` takes: a broker as it is, or a [`Bindable`](crate::Bindable) whose tokens
/// publish through the broker's connected form.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a broker the embedded app runs",
    label = "not a `Broker`",
    note = "register a type that implements `Broker`, or a `Bindable` around one"
)]
pub trait Registration {
    /// The broker being registered.
    type Broker: Broker + 'static;

    /// The slot its connected form is shared through.
    #[doc(hidden)]
    type Slot: ConnectSlot<Self::Broker>;

    #[doc(hidden)]
    fn into_parts(self) -> (Self::Broker, Self::Slot);
}

impl<B: Broker + 'static> Registration for B {
    type Broker = B;
    type Slot = NoSlot;

    fn into_parts(self) -> (B, NoSlot) {
        (self, NoSlot)
    }
}

/// One broker of the app and the routes its scope returned: an element of the app's broker
/// list, which holds the latest registration at its head.
#[doc(hidden)]
#[derive(Debug)]
pub struct BrokerMount<B, Routes, Slot = NoSlot> {
    pub(crate) broker: B,
    pub(crate) routes: Routes,
    pub(crate) slot: Slot,
}

/// A broker connected: the app owns it, and the slot of a [`Bindable`](crate::Bindable) holds the
/// other count until the broker shuts down.
#[doc(hidden)]
pub struct SharedConnection<B: Broker, Slot> {
    pub(crate) connected: Rc<B::Connected>,
    pub(crate) slot: Slot,
}

impl<B: Broker, Slot> Debug for SharedConnection<B, Slot> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedConnection").finish_non_exhaustive()
    }
}

impl<B: Broker, Slot: ConnectSlot<B>> CloseAll for SharedConnection<B, Slot> {
    async fn close_all(self) {
        // The slot lets go first, so the app holds the one count. The harness keeps the connected
        // form weakly and upgrades it only inside a synchronous call of its own, never across
        // the app's poll.
        self.slot.empty();
        if let Ok(connected) = Rc::try_unwrap(self.connected) {
            close(connected).await;
        }
    }
}

/// The app's brokers, started: every broker connected and its slot filled, then every route
/// opened, so a [`Bound`](crate::Bound) token pairs against a broker registered after the one
/// that publishes through it.
///
/// Implemented by one [`BrokerMount`] and by a pair of them; the app re-associates its broker
/// list into a balanced tree of pairs first, so an app of many brokers stays inside the
/// compiler's layout depth.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "a broker of the embedded app does not open its routes",
    note = "every registration of an embedded app is one the one-task fold opens, and its \
            handler takes the app's state type"
)]
pub trait StartMounts<State>: Sized {
    /// The connected brokers, shut down at the end.
    type Connections: CloseAll;
    /// The opened routes of every broker, run joined.
    type Opened: RunRoutes<State>;
    /// The brokers connected, their routes not yet open.
    type Linked: OpenLinked<State, Connections = Self::Connections, Opened = Self::Opened>;

    /// Connects every broker, first registered first, and fills their slots. A failure shuts
    /// down the brokers already connected.
    fn connect(self) -> impl Future<Output = Result<Self::Linked, RunError>>;

    /// Connects every broker, then opens every route.
    fn start(self) -> impl Future<Output = Result<(Self::Connections, Self::Opened), RunError>> {
        async move {
            let (connections, opened) = self.connect().await?.open().await;
            match opened {
                Ok(opened) => Ok((connections, opened)),
                Err(err) => {
                    // The broker connected last closes first.
                    connections.close_all().await;
                    Err(err)
                }
            }
        }
    }
}

/// Brokers connected with their routes still closed: the second step of the start.
#[doc(hidden)]
pub trait OpenLinked<State> {
    /// The connected brokers, shut down at the end.
    type Connections: CloseAll;
    /// The opened routes of every broker, run joined.
    type Opened: RunRoutes<State>;

    /// Opens every route, first registered first, and hands back the connections whether or not
    /// every route opened, so the caller shuts the brokers down in the reverse order of their
    /// connecting.
    fn open(self) -> impl Future<Output = (Self::Connections, Result<Self::Opened, RunError>)>;

    /// The connections as they are, without opening a route.
    fn connections(self) -> Self::Connections;
}

/// A broker connected, its slot filled, its routes waiting to open under their scope.
#[doc(hidden)]
pub struct Linked<B: Broker, Routes, Slot> {
    pub(crate) connection: SharedConnection<B, Slot>,
    pub(crate) routes: Routes,
    pub(crate) scope: ScopeDelivery,
}

impl<B: Broker, Routes, Slot> Debug for Linked<B, Routes, Slot> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Linked").finish_non_exhaustive()
    }
}

/// The connected brokers of the app: each shut down once, last connected first.
#[doc(hidden)]
pub trait CloseAll {
    /// Shuts every broker down, logging a failure and going on to the next one.
    fn close_all(self) -> impl Future<Output = ()>;
}

impl<State> StartMounts<State> for () {
    type Connections = ();
    type Opened = ();
    type Linked = ();

    fn connect(self) -> impl Future<Output = Result<(), RunError>> {
        ready(Ok(()))
    }
}

impl<State> OpenLinked<State> for () {
    type Connections = ();
    type Opened = ();

    fn open(self) -> impl Future<Output = ((), Result<(), RunError>)> {
        ready(((), Ok(())))
    }

    fn connections(self) {}
}

impl<State, B, Routes, Slot> StartMounts<State> for BrokerMount<B, Routes, Slot>
where
    B: Broker,
    Routes: OpenRoutes<B, State>,
    Slot: ConnectSlot<B>,
{
    type Connections = SharedConnection<B, Slot>;
    type Opened = Routes::Opened;
    type Linked = Linked<B, Routes, Slot>;

    async fn connect(self) -> Result<Self::Linked, RunError> {
        let Self {
            broker,
            routes,
            slot,
        } = self;
        let connected = broker.connect().await.map_err(|err| RunError::Connect {
            broker: type_name::<B>(),
            source: err.into(),
        })?;
        let connected = Rc::new(connected);
        slot.fill(&connected);
        Ok(Linked {
            connection: SharedConnection { connected, slot },
            routes,
            scope: ScopeDelivery::new(),
        })
    }
}

impl<State, B, Routes, Slot> OpenLinked<State> for Linked<B, Routes, Slot>
where
    B: Broker,
    Routes: OpenRoutes<B, State>,
    Slot: ConnectSlot<B>,
{
    type Connections = SharedConnection<B, Slot>;
    type Opened = Routes::Opened;

    async fn open(self) -> (Self::Connections, Result<Self::Opened, RunError>) {
        let Self {
            connection,
            routes,
            scope,
        } = self;
        let opened = routes
            .open(&connection.connected, &scope)
            .await
            .map_err(|source| RunError::Subscribe {
                broker: type_name::<B>(),
                source,
            });
        (connection, opened)
    }

    fn connections(self) -> Self::Connections {
        self.connection
    }
}

// A pair of the balanced broker tree: the right side holds the brokers registered first, so it
// connects first and the left side's failure shuts it down.
impl<State, Later, Earlier> StartMounts<State> for (Later, Earlier)
where
    Later: StartMounts<State>,
    Earlier: StartMounts<State>,
{
    type Connections = (Later::Connections, Earlier::Connections);
    type Opened = (Later::Opened, Earlier::Opened);
    type Linked = (Later::Linked, Earlier::Linked);

    async fn connect(self) -> Result<Self::Linked, RunError> {
        let (later, earlier) = self;
        let earlier = earlier.connect().await?;
        match later.connect().await {
            Ok(later) => Ok((later, earlier)),
            Err(err) => {
                OpenLinked::<State>::connections(earlier).close_all().await;
                Err(err)
            }
        }
    }
}

impl<State, Later, Earlier> OpenLinked<State> for (Later, Earlier)
where
    Later: OpenLinked<State>,
    Earlier: OpenLinked<State>,
{
    type Connections = (Later::Connections, Earlier::Connections);
    type Opened = (Later::Opened, Earlier::Opened);

    async fn open(self) -> (Self::Connections, Result<Self::Opened, RunError>) {
        let (later, earlier) = self;
        let (earlier_connections, earlier_opened) = earlier.open().await;
        let earlier_opened = match earlier_opened {
            Ok(opened) => opened,
            Err(err) => return ((later.connections(), earlier_connections), Err(err)),
        };
        let (later_connections, later_opened) = later.open().await;
        (
            (later_connections, earlier_connections),
            later_opened.map(|opened| (opened, earlier_opened)),
        )
    }

    fn connections(self) -> Self::Connections {
        (self.0.connections(), self.1.connections())
    }
}

impl CloseAll for () {
    fn close_all(self) -> impl Future<Output = ()> {
        ready(())
    }
}

// The later brokers were connected last, so they are shut down first.
impl<Later: CloseAll, Earlier: CloseAll> CloseAll for (Later, Earlier) {
    async fn close_all(self) {
        self.0.close_all().await;
        self.1.close_all().await;
    }
}

/// Shuts one broker down, logging a failure with the broker it came from.
pub(crate) async fn close<Connected: ConnectedBroker>(connected: Connected) {
    if let Err(err) = connected.shutdown().await {
        warn!(
            target: "ruststream::embassy",
            broker = type_name::<Connected>(),
            error = %err,
            "broker shutdown failed",
        );
    }
}
