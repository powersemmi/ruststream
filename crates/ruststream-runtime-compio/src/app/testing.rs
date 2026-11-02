//! The in-process start a test harness drives: the brokers connect through their registered
//! in-process transitions, the mounts record into the harness's hooks, and the state may be the
//! one the test builds.

// The start path runs on the task that awaits it and is never spawned.
#![allow(clippy::future_not_send)]

use std::any::TypeId;
use std::fmt;
use std::sync::Arc;

use ruststream_runtime::testing::{Coordinator, TestHooks, TestableRegistration};
use ruststream_runtime::{Placement, Workers};
use thiserror::Error;
use tracing::info;

use crate::dispatch::Fabric;
use crate::{BoxError, ConnectedLifecycle, Stop};

use super::run::Wiring;
use super::service::RegisteredBroker;
use super::teardown::ConnectedEntry;
use super::{RunningApp, RustStream, RustStreamError, StateInit};

/// Why a broker did not connect in process.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InProcessError {
    /// No crate registered an in-process transition for the broker's type.
    #[error("the broker {0} has no in-process transition")]
    NoTransport(String),
    /// The broker failed to connect in process.
    #[error("the broker {broker} failed to connect in process: {source}")]
    Connect {
        /// The broker's label, or its type name.
        broker: String,
        /// Why it failed.
        #[source]
        source: BoxError,
    },
}

/// The hooks of the harness an app runs under, which each mount records its deliveries into.
#[doc(hidden)]
#[derive(Clone)]
pub struct Harness(pub(crate) Arc<TestHooks>);

impl fmt::Debug for Harness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Harness").finish_non_exhaustive()
    }
}

/// Builds the state of a test run from the connected brokers, in registration order.
type BuildState<State> = Box<dyn FnOnce(&[&(dyn ConnectedLifecycle + 'static)]) -> State + Send>;

/// Where the state of a test run comes from.
#[doc(hidden)]
#[allow(missing_debug_implementations)] // holds a closure
pub enum StateSource<State> {
    /// The app's own `on_startup` producer, before any broker connects.
    App,
    /// A state the test builds from the connected brokers, in registration order.
    Built(BuildState<State>),
}

/// What the harness hands the app to start it in process.
#[doc(hidden)]
pub struct InProcessStart<State> {
    coordinator: Coordinator,
    registration_of: fn(TypeId) -> Option<&'static TestableRegistration>,
    state: StateSource<State>,
}

impl<State> fmt::Debug for InProcessStart<State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InProcessStart").finish_non_exhaustive()
    }
}

impl<State> InProcessStart<State> {
    /// A start that records into `coordinator` and finds each broker's transition with
    /// `registration_of`.
    #[must_use]
    pub fn new(
        coordinator: Coordinator,
        registration_of: fn(TypeId) -> Option<&'static TestableRegistration>,
        state: StateSource<State>,
    ) -> Self {
        Self {
            coordinator,
            registration_of,
            state,
        }
    }

    /// Makes the state and connects every broker in process; a broker that fails shuts down the
    /// ones connected before it.
    pub(super) async fn connect(
        self,
        state_init: StateInit<State>,
        brokers: Vec<RegisteredBroker>,
    ) -> Result<(Arc<State>, Vec<ConnectedEntry>, Harness), RustStreamError> {
        let Self {
            coordinator,
            registration_of,
            state,
        } = self;
        let hooks = Arc::new(TestHooks::detached());
        hooks.install(coordinator.clone());

        // Looked up before any broker connects: an app with one unregistered broker fails
        // without having half-started the others.
        let mut registered = Vec::with_capacity(brokers.len());
        for broker in brokers {
            let Some(registration) = registration_of(broker.lifecycle.broker_type()) else {
                let name = broker.lifecycle.broker_name().to_owned();
                return Err(RustStreamError::Connect(
                    InProcessError::NoTransport(name).into(),
                ));
            };
            registered.push((broker, registration));
        }

        let produced = match state {
            StateSource::App => Some(state_init().await.map_err(RustStreamError::Startup)?),
            StateSource::Built(_) => None,
        };
        let mut connected: Vec<ConnectedEntry> = Vec::with_capacity(registered.len());
        for (RegisteredBroker { lifecycle, label }, registration) in registered {
            let name = label
                .clone()
                .unwrap_or_else(|| lifecycle.broker_name().to_owned());
            let lifecycle = match lifecycle
                .connect_in_process(registration.transition())
                .await
            {
                Ok(lifecycle) => lifecycle,
                Err(source) => {
                    super::run::unwind_connected(connected).await;
                    return Err(RustStreamError::Connect(
                        InProcessError::Connect {
                            broker: name,
                            source,
                        }
                        .into(),
                    ));
                }
            };
            registration
                .resolve(lifecycle.as_any())
                .expect("a registration resolves the connected form its own transition produced")
                .install_coordinator(coordinator.clone());
            info!(
                target: "ruststream::lifecycle",
                broker = %name,
                "broker connected in process",
            );
            connected.push(ConnectedEntry { lifecycle, label });
        }
        coordinator.locate(connected.iter().map(|entry| entry.lifecycle.as_any()));
        let state = match state {
            StateSource::App => produced.expect("the app's state was produced"),
            StateSource::Built(build) => {
                let views: Vec<&(dyn ConnectedLifecycle + 'static)> =
                    connected.iter().map(|entry| &*entry.lifecycle).collect();
                build(&views)
            }
        };
        Ok((Arc::new(state), connected, Harness(hooks)))
    }
}

/// The policy under the harness: the same concurrency, as tasks of the runtime the test runs on.
fn on_runtime(workers: Workers) -> Workers {
    match (workers.placement(), workers.by_key()) {
        (Placement::Threads, true) => Workers::keyed(workers.count()),
        (Placement::Threads, false) => Workers::pool(workers.count()),
        _ => workers,
    }
}

impl<Layers: Send, State: Send + Sync + 'static, Pipeline, Phase>
    RustStream<Layers, State, Pipeline, Phase>
{
    /// Starts the service on the calling runtime as [`start`](Self::start) does, with the
    /// brokers connected in process and the mounts recording into the harness.
    ///
    /// # Errors
    ///
    /// As [`start`](Self::start); a broker with no in-process transition, or one that fails to
    /// connect in process, is a [`RustStreamError::Connect`] carrying an [`InProcessError`].
    #[doc(hidden)]
    pub async fn start_in_process(
        self,
        start: InProcessStart<State>,
    ) -> Result<RunningApp, RustStreamError> {
        // The harness runs every subscription on the test's own runtime, so a `threads(n)` mount
        // is placed as `n` workers of it, as the dispatch will run it.
        let layout = self.layout_of(1, on_runtime)?;
        let stop = Stop::new(self.subscription_end);
        self.start_on(layout, stop, Fabric::open(1), Wiring::InProcess(start))
            .await
    }
}
