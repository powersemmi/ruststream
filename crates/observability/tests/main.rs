//! The observability layers' integration suite, one test binary: each file is a module of it.

#[path = "../../runtime/tests/support/common.rs"]
mod common;

mod metrics;
mod otel;
