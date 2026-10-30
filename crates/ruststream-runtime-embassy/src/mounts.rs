//! The app's brokers as a typed list: each broker with the routes its scope returned, connected
//! and opened in the order they were registered, and shut down in the reverse order.

// Written over the local twins of the contract: the futures hold brokers and subscriptions that
// are not `Send`, and the app's one task polls them where it was spawned.
#![allow(clippy::future_not_send)]

use core::any::type_name;
use core::future::ready;

use ruststream_core::{LocalBroker, LocalConnectedBroker};
use ruststream_runtime::{
    OpenRoutes, RetryDestinationsDeclared, Router, RouterCommit, RouterWith, RunRoutes,
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

/// One broker of the app and the routes its scope returned: an element of the app's broker
/// list, which holds the latest registration at its head.
#[doc(hidden)]
#[derive(Debug)]
pub struct BrokerMount<B, Routes> {
    pub(crate) broker: B,
    pub(crate) routes: Routes,
}

/// The app's brokers, started: every broker connected and every route opened.
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
pub trait StartMounts<State> {
    /// The connected brokers, shut down at the end.
    type Connections: CloseAll;
    /// The opened routes of every broker, run joined.
    type Opened: RunRoutes<State>;

    /// Connects every broker and opens its routes, first registered first. A failure shuts down
    /// the brokers already connected.
    fn start(self) -> impl Future<Output = Result<(Self::Connections, Self::Opened), RunError>>;
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

    fn start(self) -> impl Future<Output = Result<((), ()), RunError>> {
        ready(Ok(((), ())))
    }
}

impl<State, B, Routes> StartMounts<State> for BrokerMount<B, Routes>
where
    B: LocalBroker,
    Routes: OpenRoutes<B, State>,
{
    type Connections = (B::Connected,);
    type Opened = Routes::Opened;

    async fn start(self) -> Result<(Self::Connections, Self::Opened), RunError> {
        let Self { broker, routes } = self;
        let connected = broker.connect().await.map_err(|err| RunError::Connect {
            broker: type_name::<B>(),
            source: err.into(),
        })?;
        match routes.open(&connected).await {
            Ok(routes) => Ok(((connected,), routes)),
            Err(source) => {
                close(connected).await;
                Err(RunError::Subscribe {
                    broker: type_name::<B>(),
                    source,
                })
            }
        }
    }
}

// A pair of the balanced broker tree: the right side holds the brokers registered first, so it
// starts first and the left side's failure shuts it down.
impl<State, Later, Earlier> StartMounts<State> for (Later, Earlier)
where
    Later: StartMounts<State>,
    Earlier: StartMounts<State>,
{
    type Connections = (Later::Connections, Earlier::Connections);
    type Opened = (Later::Opened, Earlier::Opened);

    async fn start(self) -> Result<(Self::Connections, Self::Opened), RunError> {
        let (later, earlier) = self;
        let (earlier_connections, earlier_opened) = earlier.start().await?;
        match later.start().await {
            Ok((connections, opened)) => {
                Ok(((connections, earlier_connections), (opened, earlier_opened)))
            }
            Err(err) => {
                earlier_connections.close_all().await;
                Err(err)
            }
        }
    }
}

impl CloseAll for () {
    fn close_all(self) -> impl Future<Output = ()> {
        ready(())
    }
}

impl<Connected: LocalConnectedBroker> CloseAll for (Connected,) {
    fn close_all(self) -> impl Future<Output = ()> {
        close(self.0)
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
async fn close<Connected: LocalConnectedBroker>(connected: Connected) {
    if let Err(err) = connected.shutdown().await {
        warn!(
            target: "ruststream::embassy",
            broker = type_name::<Connected>(),
            error = %err,
            "broker shutdown failed",
        );
    }
}
