//! The tokio runner's integration suite, one test binary: each directory is named for the module of
//! the crate it covers.

#[path = "../../runtime/tests/support/common.rs"]
mod common;
#[path = "../../runtime/tests/support/log_capture.rs"]
mod log_capture;

mod app;
mod buffered;
mod dispatch;
mod shutdown;
mod time;
