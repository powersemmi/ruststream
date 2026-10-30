//! [`EmbeddedApp`]: a device service's app, its brokers, mounts, state and hooks in its type, run
//! as one future on the task that awaits it.

// The app's future holds brokers, subscriptions and handlers that are not `Send`: it runs on the
// one embassy task that awaits it.
#![allow(clippy::future_not_send)]

use alloc::sync::Arc;
use core::error::Error as StdError;

use futures::future::join;
use ruststream_core::LocalBroker;
use ruststream_runtime::{BoxError, Identity, LocalPipeline, Router, RunRoutes};
use thiserror::Error;

use crate::control::Control;
use crate::hooks::{HookList, Hooks, NoHooks};
use crate::host::AppHost;
use crate::mounts::{BrokerMount, CloseAll, MountChain, StartMounts};

/// The router a broker's scope hands the closure of [`EmbeddedApp::with_broker`].
///
/// # Examples
///
/// A scope written as a function of its own:
///
/// ```
/// use ruststream_memory::local::LocalMemoryBroker;
/// use ruststream_runtime_embassy::{EmbeddedApp, Mounts, Scope};
///
/// type Bus = LocalMemoryBroker<4>;
///
/// /// The bus's mounts, kept apart from the app.
/// fn mounts(b: Scope<Bus>) -> Scope<Bus> {
///     b
/// }
///
/// fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, mounts)
/// }
/// # let _ = app(Bus::new());
/// ```
pub type Scope<B> = Router<B, (), (), Identity, LocalPipeline>;

/// A device service's app: its brokers with their mounts, its state and its lifecycle hooks, all
/// in its type.
///
/// It runs as one future on the embassy task that awaits [`run`](Self::run): the future connects
/// every broker, opens every subscription, joins their loops, and shuts the brokers down once
/// the app's [`Control`] asks it to stop. Nothing is boxed or erased; the subscriptions' loops
/// allocate nothing per delivery.
///
/// # Examples
///
/// A firmware's app over the local bus, run by its own task until a button stops it:
///
/// ```
/// use core::convert::Infallible;
/// use core::future::ready;
///
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream_runtime::{Context, Deserialized, Handle, Input, SoloDeserialized, subscriber};
/// use ruststream_runtime_embassy::{Control, EmbeddedApp, Mounts};
///
/// /// A reading, as the sensor sends it: the frame's bytes.
/// struct Reading<'a>(&'a [u8]);
///
/// impl Deserialized for Reading<'_> {
///     type Output<'a> = Reading<'a>;
///     type Error = Infallible;
///
///     fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Infallible> {
///         Ok(Reading(payload))
///     }
/// }
///
/// impl Input for Reading<'_> {
///     type Axis = SoloDeserialized<Reading<'static>>;
/// }
///
/// /// Logs every reading.
/// struct OnReading;
///
/// impl Handle<Reading<'_>> for OnReading {
///     fn handle(
///         &self,
///         reading: &Reading<'_>,
///         _outs: &(),
///         _ctx: &mut Context<'_>,
///     ) -> impl Future<Output = Result<(), ruststream_runtime::HandlerOutcome>> + Send {
///         tracing::info!(bytes = reading.0.len(), "reading");
///         ready(Ok(()))
///     }
/// }
///
/// type Bus = LocalMemoryBroker<4>;
///
/// fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(subscriber(LocalMemorySource::new("sensors/temp"), OnReading).build())
///     })
/// }
///
/// static CONTROL: Control = Control::new();
///
/// // The app's task in a firmware: `#[embassy_executor::task] async fn run_app(bus: Bus)`.
/// async fn run_app(bus: Bus) {
///     if let Err(err) = app(bus).run(&CONTROL).await {
///         tracing::error!(error = %err, "the app stopped on an error");
///     }
/// }
/// # use embassy_executor as _;
/// # CONTROL.stop();
/// # embassy_futures::block_on(run_app(Bus::new()));
/// ```
#[derive(Debug)]
#[must_use = "an app does nothing until it runs"]
pub struct EmbeddedApp<M> {
    mounts: M,
}

/// What an [`EmbeddedApp`] holds: its state, its brokers with their routes, and its hooks.
#[doc(hidden)]
#[derive(Debug)]
pub struct Parts<State, Brokers, AppHooks> {
    state: State,
    brokers: Brokers,
    hooks: AppHooks,
}

impl EmbeddedApp<Parts<(), (), NoHooks>> {
    /// An app with no broker, no state and no hooks.
    pub const fn new() -> Self {
        Self {
            mounts: Parts {
                state: (),
                brokers: (),
                hooks: Hooks {
                    after_startup: (),
                    on_shutdown: (),
                    after_shutdown: (),
                },
            },
        }
    }
}

impl Default for EmbeddedApp<Parts<(), (), NoHooks>> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Brokers> EmbeddedApp<Parts<(), Brokers, NoHooks>> {
    /// Gives the app a state its handlers read through the `State` extractor and its hooks
    /// receive.
    ///
    /// It is set before any hook, because a hook is written against the state's type.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::LocalMemoryBroker;
    /// use ruststream_runtime_embassy::{Control, EmbeddedApp, Mounts};
    ///
    /// /// What the device's handlers share.
    /// struct Device {
    ///     serial: u32,
    /// }
    ///
    /// fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    ///     EmbeddedApp::new()
    ///         .with_state(Device { serial: 7 })
    ///         .with_broker(bus, |b| b)
    ///         .after_startup(async |device: std::sync::Arc<Device>| {
    ///             tracing::info!(serial = device.serial, "up");
    ///             Ok::<(), core::fmt::Error>(())
    ///         })
    /// }
    /// # static CONTROL: Control = Control::new();
    /// # CONTROL.stop();
    /// # embassy_futures::block_on(app(LocalMemoryBroker::new()).run(&CONTROL)).unwrap();
    /// ```
    pub fn with_state<State>(self, state: State) -> EmbeddedApp<Parts<State, Brokers, NoHooks>> {
        EmbeddedApp {
            mounts: Parts {
                state,
                brokers: self.mounts.brokers,
                hooks: self.mounts.hooks,
            },
        }
    }
}

impl<State, Brokers, AfterStartup, OnShutdown, AfterShutdown>
    EmbeddedApp<Parts<State, Brokers, Hooks<AfterStartup, OnShutdown, AfterShutdown>>>
{
    /// Adds `broker` with the routes `mount` returns.
    ///
    /// The closure takes the broker's scope, a local [`Router`], by value and returns the chain
    /// it built: `|b| b.include(a).out_reply(policy).include(b)`. Each call adds a broker; they
    /// connect in the order they were added. Nothing connects until the app runs.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::LocalMemoryBroker;
    /// use ruststream_runtime_embassy::{EmbeddedApp, Mounts};
    ///
    /// /// The board's two buses: the sensors' and the actuators'.
    /// fn app(sensors: LocalMemoryBroker<8>, actuators: LocalMemoryBroker<2>) -> EmbeddedApp<impl Mounts> {
    ///     EmbeddedApp::new()
    ///         .with_broker(sensors, |b| b)
    ///         .with_broker(actuators, |b| b)
    /// }
    /// # let _ = app(LocalMemoryBroker::new(), LocalMemoryBroker::new());
    /// ```
    #[allow(clippy::type_complexity)] // the app's own type, which the caller names as `impl Mounts`
    pub fn with_broker<B, Mount, Chain>(
        self,
        broker: B,
        mount: Mount,
    ) -> EmbeddedApp<
        Parts<
            State,
            (Brokers, BrokerMount<B, Chain::Routes>),
            Hooks<AfterStartup, OnShutdown, AfterShutdown>,
        >,
    >
    where
        B: LocalBroker + 'static,
        Mount: FnOnce(Scope<B>) -> Chain,
        Chain: MountChain,
    {
        let routes = mount(Router::local()).finish();
        let Parts {
            state,
            brokers,
            hooks,
        } = self.mounts;
        EmbeddedApp {
            mounts: Parts {
                state,
                brokers: (brokers, BrokerMount { broker, routes }),
                hooks,
            },
        }
    }

    /// Adds a hook called once every subscription is open, beside the running subscriptions;
    /// an error stops the app and [`run`](Self::run) returns it.
    ///
    /// # Examples
    ///
    /// See [`with_state`](EmbeddedApp::with_state).
    #[allow(clippy::type_complexity)] // the app's own type, which the caller names as `impl Mounts`
    pub fn after_startup<Hook, Fut, E>(
        self,
        hook: Hook,
    ) -> EmbeddedApp<Parts<State, Brokers, Hooks<(AfterStartup, Hook), OnShutdown, AfterShutdown>>>
    where
        Hook: FnOnce(Arc<State>) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: StdError + Send + Sync + 'static,
    {
        let Parts {
            state,
            brokers,
            hooks,
        } = self.mounts;
        EmbeddedApp {
            mounts: Parts {
                state,
                brokers,
                hooks: Hooks {
                    after_startup: (hooks.after_startup, hook),
                    on_shutdown: hooks.on_shutdown,
                    after_shutdown: hooks.after_shutdown,
                },
            },
        }
    }

    /// Adds a hook called once every subscription has stopped, while the brokers are still
    /// connected; an error is logged.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::LocalMemoryBroker;
    /// use ruststream_runtime_embassy::{EmbeddedApp, Mounts};
    ///
    /// fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    ///     EmbeddedApp::new()
    ///         .with_broker(bus, |b| b)
    ///         .on_shutdown(async |_state| {
    ///             tracing::info!("flushing the log");
    ///             Ok::<(), core::fmt::Error>(())
    ///         })
    /// }
    /// # let _ = app(LocalMemoryBroker::new());
    /// ```
    #[allow(clippy::type_complexity)] // the app's own type, which the caller names as `impl Mounts`
    pub fn on_shutdown<Hook, Fut, E>(
        self,
        hook: Hook,
    ) -> EmbeddedApp<Parts<State, Brokers, Hooks<AfterStartup, (OnShutdown, Hook), AfterShutdown>>>
    where
        Hook: FnOnce(Arc<State>) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: StdError + Send + Sync + 'static,
    {
        let Parts {
            state,
            brokers,
            hooks,
        } = self.mounts;
        EmbeddedApp {
            mounts: Parts {
                state,
                brokers,
                hooks: Hooks {
                    after_startup: hooks.after_startup,
                    on_shutdown: (hooks.on_shutdown, hook),
                    after_shutdown: hooks.after_shutdown,
                },
            },
        }
    }

    /// Adds a hook called once every broker has shut down; an error is logged.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::LocalMemoryBroker;
    /// use ruststream_runtime_embassy::{EmbeddedApp, Mounts};
    ///
    /// fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    ///     EmbeddedApp::new()
    ///         .with_broker(bus, |b| b)
    ///         .after_shutdown(async |_state| {
    ///             tracing::info!("radio off");
    ///             Ok::<(), core::fmt::Error>(())
    ///         })
    /// }
    /// # let _ = app(LocalMemoryBroker::new());
    /// ```
    #[allow(clippy::type_complexity)] // the app's own type, which the caller names as `impl Mounts`
    pub fn after_shutdown<Hook, Fut, E>(
        self,
        hook: Hook,
    ) -> EmbeddedApp<Parts<State, Brokers, Hooks<AfterStartup, OnShutdown, (AfterShutdown, Hook)>>>
    where
        Hook: FnOnce(Arc<State>) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: StdError + Send + Sync + 'static,
    {
        let Parts {
            state,
            brokers,
            hooks,
        } = self.mounts;
        EmbeddedApp {
            mounts: Parts {
                state,
                brokers,
                hooks: Hooks {
                    after_startup: hooks.after_startup,
                    on_shutdown: hooks.on_shutdown,
                    after_shutdown: (hooks.after_shutdown, hook),
                },
            },
        }
    }
}

impl<M: Mounts> EmbeddedApp<M> {
    /// Runs the app until `control` asks it to stop or a subscription fails it.
    ///
    /// Connects every broker and opens every subscription (health `Starting`), runs them joined
    /// (`Running`), and once a stop is asked for lets each subscription finish the delivery it
    /// holds and the retries it keeps, then shuts every broker down (`Stopped`). A failure at
    /// any point leaves the health at `Failed`.
    ///
    /// # Errors
    ///
    /// A broker that did not connect, a subscription the broker refused, a failing
    /// `after_startup` hook, or a subscription that failed under a fail-fast policy.
    ///
    /// # Examples
    ///
    /// See [`EmbeddedApp`].
    pub fn run(self, control: &Control) -> impl Future<Output = Result<(), RunError>> {
        self.mounts.run_app(control)
    }
}

/// The content of an [`EmbeddedApp`]: its brokers with their mounts, its state and its hooks.
///
/// A function that builds the app returns `EmbeddedApp<impl Mounts>`, so its type stays the
/// compiler's to name.
///
/// # Examples
///
/// See [`EmbeddedApp`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an app the embedded runner runs",
    note = "every broker of the app opens its routes on the one-task runner, and every hook \
            takes the app's state as `Arc<State>`"
)]
pub trait Mounts {
    /// Runs the app; [`EmbeddedApp::run`] is the call.
    #[doc(hidden)]
    fn run_app(self, control: &Control) -> impl Future<Output = Result<(), RunError>>;
}

impl<State, Brokers, AfterStartup, OnShutdown, AfterShutdown> Mounts
    for Parts<State, Brokers, Hooks<AfterStartup, OnShutdown, AfterShutdown>>
where
    Brokers: StartMounts<State>,
    AfterStartup: HookList<State>,
    OnShutdown: HookList<State>,
    AfterShutdown: HookList<State>,
{
    async fn run_app(self, control: &Control) -> Result<(), RunError> {
        let Self {
            state,
            brokers,
            hooks,
        } = self;
        let state = Arc::new(state);
        let (connections, opened) = match brokers.start().await {
            Ok(started) => started,
            Err(err) => {
                control.fail_app("app", "startup failed");
                return Err(err);
            }
        };
        control.running();
        let host = AppHost::new();
        let startup = async {
            let outcome = hooks.after_startup.call(&state).await;
            if outcome.is_err() {
                control.fail_app("app", "an after_startup hook failed");
            }
            outcome
        };
        let serve = async {
            opened.run(control, &state, &host).await;
            // Every loop has ended, so nothing detaches more work: the host finishes what it holds.
            host.close();
        };
        let ((), startup) = join(join(serve, host.drive()).map_unit(), startup).await;
        hooks.on_shutdown.call_all(&state, "on_shutdown").await;
        connections.close_all().await;
        hooks
            .after_shutdown
            .call_all(&state, "after_shutdown")
            .await;
        if let Err(source) = startup {
            return Err(RunError::Startup { source });
        }
        if control.has_failed() {
            return Err(RunError::Failed);
        }
        control.stopped();
        Ok(())
    }
}

/// Discards the unit outputs of a joined pair.
trait MapUnit {
    fn map_unit(self) -> impl Future<Output = ()>;
}

impl<Pair: Future<Output = ((), ())>> MapUnit for Pair {
    async fn map_unit(self) {
        self.await;
    }
}

/// Why an [`EmbeddedApp`] stopped other than on its control's request.
///
/// # Examples
///
/// See [`EmbeddedApp`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RunError {
    /// A broker did not connect.
    #[error("broker `{broker}` did not connect")]
    Connect {
        /// The broker's type.
        broker: &'static str,
        /// The broker's error.
        #[source]
        source: BoxError,
    },
    /// A broker refused a subscription.
    #[error("broker `{broker}` refused a subscription")]
    Subscribe {
        /// The broker's type.
        broker: &'static str,
        /// The broker's error.
        #[source]
        source: BoxError,
    },
    /// An `after_startup` hook failed.
    #[error("an after_startup hook failed")]
    Startup {
        /// The hook's error.
        #[source]
        source: BoxError,
    },
    /// A subscription failed under a fail-fast policy; the log names it and the reason.
    #[error("a subscription failed under a fail-fast policy")]
    Failed,
}
