//! Where the work a delivery leaves behind runs: its continuations, kept beside the
//! subscriptions and polled by the app's own task, so every timer they arm is the task's own.
//!
//! With `alloc` the host keeps the continuations deliveries hand over in a list the app's driver
//! polls. Without it each worker keeps one continuation in a slot of its own (`slot`).
//!
//! A registration's retry copies wait in its own store (`.retry_capacity::<CAP>()`), as data the
//! registration's loop publishes once due; the host keeps none.

#[cfg(ruststream_alloc)]
mod boxed;
#[cfg(not(ruststream_alloc))]
mod slot;

#[cfg(ruststream_alloc)]
pub(crate) use boxed::AppHost;
#[cfg(not(ruststream_alloc))]
pub(crate) use slot::{AppHost, WorkerHost};
