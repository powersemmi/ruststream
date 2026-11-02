//! The compio runner of [RustStream](https://docs.rs/ruststream): one compio runtime per core,
//! with the clock, the shutdown and the launcher a service on `io_uring` runs on.
//!
//! - [`CompioClock`] and [`time::sleep`]: the app's clock over compio's timers.
//! - [`Cores`]: the launcher, core 0 on the calling thread and a thread per further core.
//! - [`Stop`]: the shutdown signal, a flag and a wait.
//! - [`Tasks`]: where the work a delivery leaves behind runs, and the wait for it to end.
//! - [`yield_now`]: the cooperative yield after each delivery.

mod clock;
mod cores;
#[doc(hidden)]
pub mod dispatch;
mod stop;
mod tasks;
pub mod time;
#[cfg(feature = "testing")]
mod virtual_clock;
mod yield_now;

pub use clock::CompioClock;
pub use cores::{Cores, CoresError};
pub use stop::{Cancelled, Stop, StreamEnd};
pub use tasks::Tasks;
pub use yield_now::{YieldNow, yield_now};

/// The virtual clock the test harness moves.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod testing {
    pub use crate::virtual_clock::VirtualClock;
}
