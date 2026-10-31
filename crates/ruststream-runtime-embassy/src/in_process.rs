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
use crate::mounts::{BrokerMount, CloseAll, ConnectSlot, SharedConnection};

/// The app's brokers, started in process: every broker connected through its in-process
/// transition and noted in the harness's run, every route opened under its broker's scope.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "a broker of the embedded app has no in-process mode",
    note = "the test harness connects every broker of the app in process: each implements \
            `InProcess` under its crate's `testing` feature"
)]
pub trait StartInProcess<State> {
    /// The connected brokers, shut down at the end.
    type Connections: CloseAll;
    /// The opened routes of every broker, run joined.
    type Opened: RunRoutes<State>;

    /// Connects every broker in process and opens its routes, first registered first. A failure
    /// shuts down the brokers already connected.
    fn start_in_process(
        self,
        run: &InProcessRun,
    ) -> impl Future<Output = Result<(Self::Connections, Self::Opened), RunError>>;
}

impl<State> StartInProcess<State> for () {
    type Connections = ();
    type Opened = ();

    fn start_in_process(
        self,
        _run: &InProcessRun,
    ) -> impl Future<Output = Result<((), ()), RunError>> {
        ready(Ok(((), ())))
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

    async fn start_in_process(
        self,
        run: &InProcessRun,
    ) -> Result<(Self::Connections, Self::Opened), RunError> {
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
        match routes.open(&connected, &scope).await {
            Ok(routes) => Ok((SharedConnection { connected, slot }, routes)),
            Err(source) => {
                SharedConnection { connected, slot }.close_all().await;
                Err(RunError::Subscribe {
                    broker: type_name::<B>(),
                    source,
                })
            }
        }
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

    async fn start_in_process(
        self,
        run: &InProcessRun,
    ) -> Result<(Self::Connections, Self::Opened), RunError> {
        let (later, earlier) = self;
        let (earlier_connections, earlier_opened) = earlier.start_in_process(run).await?;
        match later.start_in_process(run).await {
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
