//! The tokio runner of [RustStream](https://docs.rs/ruststream): it runs the shared delivery body
//! of `ruststream-runtime` on tokio, and owns what is tokio's: the app's lifecycle, spawning, the
//! worker pools and dedicated threads, the signals, the main runtime handle and the clock.
//!
//! Services use these items through the `ruststream` facade, which re-exports them under the
//! paths its documentation teaches:
//!
//! - the crate root beside the shared body, as
//!   [`ruststream::runtime`](https://docs.rs/ruststream/latest/ruststream/runtime/);
//! - [`time`] as [`ruststream::time`](https://docs.rs/ruststream/latest/ruststream/time/).

// The contract and the shared body, under the names the runner's modules reach them by
// (`crate::Broker`, `crate::Router`, ...).
#[allow(clippy::wildcard_imports)]
use ruststream_core::*;
#[allow(clippy::wildcard_imports)]
use ruststream_runtime::*;

mod app;
mod buffered;
mod clock;
mod detach;
mod dispatch;
mod shutdown;
mod sink;
#[cfg(test)]
mod testkit;
pub mod time;

#[cfg(feature = "testing")]
#[doc(hidden)]
pub use app::TestParts;
pub use app::{
    App, AppInfo, BrokerScope, HealthProbe, HealthState, RunningApp, RustStream, RustStreamError,
    Setup, SubscriptionCount, SubscriptionEnd, Wired,
};
#[doc(hidden)]
pub use app::{ConnectedEntry, LifecycleHook, RegisteredBroker, ShutdownHooks, Starter, Teardown};
#[doc(hidden)]
pub use app::{MountChain, ScopeRegistrations, ScopedHandle, ScopedRouter, ScopedStartup};
pub use buffered::{Buffered, BufferedSubscriber};
pub use clock::Tokio;
pub use ruststream_runtime::MainRuntime;
#[doc(hidden)]
pub use shutdown::Shutdown;
pub use sink::RouterSink;
