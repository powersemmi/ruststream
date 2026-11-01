//! The app's brokers connected in process, for the test harness: each broker through its
//! in-process transition, its routes opened under the scope the harness records them in.

// Written over the local twins of the contract, as the production start is.
#![allow(clippy::future_not_send)]

use alloc::rc::Rc;
use core::any::type_name;
use core::future::ready;

use ruststream_runtime::testing::{InProcess, InProcessRun};
use ruststream_runtime::{OpenRoutes, RunRoutes};

use crate::app::RunError;
use crate::mounts::{BrokerMount, CloseAll, ConnectSlot, Linked, OpenLinked, SharedConnection};

/// The app's brokers, started in process: every broker connected through its in-process
/// transition and noted in the harness's run, then every route opened under its broker's scope,
/// so a [`Bound`](crate::Bound) token pairs against a broker registered after its publisher.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "a broker of the embedded app has no in-process mode",
    note = "the test harness connects every broker of the app in process: each implements \
            `InProcess` under its crate's `testing` feature"
)]
pub trait StartInProcess<State>: Sized {
    /// The connected brokers, shut down at the end.
    type Connections: CloseAll;
    /// The opened routes of every broker, run joined.
    type Opened: RunRoutes<State>;
    /// The brokers connected, their routes not yet open.
    type Linked: OpenLinked<State, Connections = Self::Connections, Opened = Self::Opened>;

    /// Connects every broker in process, first registered first. A failure shuts down the
    /// brokers already connected.
    fn connect_in_process(
        self,
        run: &InProcessRun,
    ) -> impl Future<Output = Result<Self::Linked, RunError>>;

    /// Connects every broker in process, then opens every route.
    fn start_in_process(
        self,
        run: &InProcessRun,
    ) -> impl Future<Output = Result<(Self::Connections, Self::Opened), RunError>> {
        async move {
            let (connections, opened) = self.connect_in_process(run).await?.open().await;
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

impl<State> StartInProcess<State> for () {
    type Connections = ();
    type Opened = ();
    type Linked = ();

    fn connect_in_process(self, _run: &InProcessRun) -> impl Future<Output = Result<(), RunError>> {
        ready(Ok(()))
    }
}

impl<State, B, Routes, Slot> StartInProcess<State> for BrokerMount<B, Routes, Slot>
where
    B: InProcess + 'static,
    Routes: OpenRoutes<B, State>,
    Slot: ConnectSlot<B>,
{
    type Connections = SharedConnection<B, Slot>;
    type Opened = Routes::Opened;
    type Linked = Linked<B, Routes, Slot>;

    async fn connect_in_process(self, run: &InProcessRun) -> Result<Self::Linked, RunError> {
        let Self {
            broker,
            routes,
            slot,
        } = self;
        let connected = broker
            .connect_in_process()
            .await
            .map_err(|err| RunError::Connect {
                broker: type_name::<B>(),
                source: err.into(),
            })?;
        let connected = Rc::new(connected);
        slot.fill(&connected);
        let scope = run.connected::<B>(&connected);
        Ok(Linked {
            connection: SharedConnection { connected, slot },
            routes,
            scope,
        })
    }
}

// As the production start: the right side holds the brokers registered first.
impl<State, Later, Earlier> StartInProcess<State> for (Later, Earlier)
where
    Later: StartInProcess<State>,
    Earlier: StartInProcess<State>,
{
    type Connections = (Later::Connections, Earlier::Connections);
    type Opened = (Later::Opened, Earlier::Opened);
    type Linked = (Later::Linked, Earlier::Linked);

    async fn connect_in_process(self, run: &InProcessRun) -> Result<Self::Linked, RunError> {
        let (later, earlier) = self;
        let earlier = earlier.connect_in_process(run).await?;
        match later.connect_in_process(run).await {
            Ok(later) => Ok((later, earlier)),
            Err(err) => {
                OpenLinked::<State>::connections(earlier).close_all().await;
                Err(err)
            }
        }
    }
}
