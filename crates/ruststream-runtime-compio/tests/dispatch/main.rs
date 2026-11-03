//! The compio dispatch loops: the runner machinery below the harness, driven directly.
//!
//! These tests start the cores by hand and call the spawn functions the builder will call: the
//! subject is where each delivery runs and in what order, which the in-process harness, running
//! one core, does not reproduce.

mod cores;
mod forms;
mod keyed;
mod sequential;
mod support;
mod workers;
