//! The shared delivery body on embassy: a device's executor, clock and broker, hosted on a thread.
//!
//! The body that decodes a delivery, runs the handler and settles it is written once for every
//! runtime. This crate holds it to the constraints of embassy, the executor of a device without
//! an operating system:
//!
//! - the body is generic, and an embassy task is not, so a concrete task awaits a generic body;
//! - the broker's types are not `Send`, so the body reaches them through the `Local*` contract
//!   twins alone;
//! - the delivery path keeps nothing on the heap: frames are `heapless` buffers in a `static`
//!   channel, and the shutdown's wait list is a fixed array;
//! - every timer is the clock's own future, polled by the task's own waker, which embassy's
//!   integrated timer queue requires;
//! - the shutdown check is one relaxed load.
//!
//! [`EmbassyClock`] is the runner's clock, [`Stop`] its shutdown, and [`frames`] a broker stand-in
//! over an embassy-sync channel. The test binary runs them on the `std` platform's executor and
//! moves embassy-time's mock clock by hand.

#![no_std]

mod clock;
pub mod frames;
mod stop;

pub use clock::EmbassyClock;
pub use stop::{Stop, Stopping};
