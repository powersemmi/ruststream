//! The in-memory broker passes the opt-in suites, and each new check fails against a broker
//! broken in exactly the way the check exists to catch.
//!
//! The broken doubles wrap the in-memory broker and change one behaviour each: a settlement, a
//! seek, a request, a commit. A fault that leaves work to the caller's runtime does so only on a
//! current-thread runtime, the one a handler on a dedicated thread calls from: on the suite's own
//! multi-threaded runtime the double behaves, so the suite reaches the check it is aimed at.

// The factories' bounds are higher-ranked (`Fn(&str) -> _`, `Fn(&C) -> _`), and a bare path to a
// constructor or a method binds one concrete lifetime, so the closures stay.
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]

use std::time::Duration;

use tokio::runtime::{Handle, RuntimeFlavor};

use ruststream::memory::{MemoryBroker, Retaining, Retention};
use ruststream_core::nonzero;

mod requesters;
mod subscriptions;
mod transactional;

/// How long a task left on a stopping runtime waits before it would act: longer than the rest of
/// the call, so the runtime is gone first.
const LOST_AFTER: Duration = Duration::from_millis(50);

fn retaining() -> MemoryBroker<Retaining> {
    MemoryBroker::retaining(Retention::Messages(nonzero!(64)))
}

/// Whether the caller runs on a current-thread runtime, where a handler on a dedicated thread
/// calls from.
fn on_current_thread_runtime() -> bool {
    Handle::current().runtime_flavor() == RuntimeFlavor::CurrentThread
}
