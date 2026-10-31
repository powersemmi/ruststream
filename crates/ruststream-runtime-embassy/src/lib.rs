//! The embedded runner of RustStream: a device service's app on embassy's executor and clock,
//! on `no_std` with a heap.
//!
//! The app is one future that one embassy task awaits. Its type carries every broker, mount,
//! state and hook, so nothing is boxed or erased, and a delivery allocates nothing.
//!
//! - [`EmbeddedApp`]: the app builder: `with_broker(broker, |b| chain)`, `with_state`, the
//!   lifecycle hooks, and [`run`](EmbeddedApp::run). [`Mounts`] is the bound a function that
//!   builds the app returns (`EmbeddedApp<impl Mounts>`); [`RunError`] says why a run ended
//!   other than on request.
//! - [`MountSteps`]: the runner's mount steps with counts fixed in the registration's type:
//!   `.pool::<N>()`, `.keyed::<N>()` and `.retry_capacity::<CAP>()`. [`Scope`] is the local
//!   router a broker's scope starts from; [`MountChain`] finishes what the scope returns.
//! - [`Control`] and [`Health`]: the stop device code asks for and the health it reads, both in
//!   a `static`.
//! - [`EmbassyClock`]: the runner's clock over embassy-time.
//! - [`frames`]: a broker stand-in over an embassy-sync channel, for the runner's host tests.
//!
//! How a device service is built is taught on the site's embedded runtime page.
//!
#![no_std]

extern crate alloc;

mod app;
mod bindable;
mod clock;
mod control;
mod copies;
pub mod frames;
mod hooks;
mod host;
#[cfg(feature = "testing")]
mod in_process;
mod lanes;
mod mailbox;
mod mounts;

#[doc(hidden)]
pub use app::AppBrokers;
pub use app::{EmbeddedApp, Mounts, Parts, RunError, Scope};
pub use bindable::{Bindable, Bound};
pub use clock::EmbassyClock;
pub use control::{Control, Health, Stopping};
#[doc(hidden)]
pub use hooks::{HookList, Hooks, NoHooks};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use in_process::StartInProcess;
pub use lanes::MountSteps;
#[doc(hidden)]
pub use lanes::{EmbeddedPlan, Keyed, Lanes, NoRoom, OneLane, Pool, Room, SoFar};
#[doc(hidden)]
pub use mounts::{BrokerMount, CloseAll, ConnectSlot, NoSlot, SharedConnection, StartMounts};
pub use mounts::{MountChain, Registration};
