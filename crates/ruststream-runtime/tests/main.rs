//! The runtime's integration suite, one test binary: each directory is named for the module of
//! the crate it covers.

#[path = "support/carrying.rs"]
mod carrying;
#[path = "support/common.rs"]
mod common;
#[path = "support/log_capture.rs"]
mod log_capture;

mod batch;
mod codec;
mod context;
mod debug_forms;
mod dispatch;
mod extract;
mod failure;
mod handle;
mod in_messages;
mod inject;
mod input;
mod lifecycle;
mod local_out;
mod local_reply;
mod poll_diagnostics;
mod publish;
mod publishing;
mod retry;
mod router;
mod settings;
