//! {{project-name}} - the device service as a library, so the same code runs on the board and under
//! the tests on the host.
//!
//! `main.rs` is the board and hands the app its bus; `service` holds the messages, the handlers
//! and the app.

#![cfg_attr(not(test), no_std)]

pub mod service;

use ruststream::memory::local::LocalMemoryBroker;

/// The board's bus: each subscription holds four messages, waiting or in its handler's hands.
pub type Bus = LocalMemoryBroker<4>;
