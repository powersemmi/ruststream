//! The conformance suites run against the reference broker, one test binary: each file is a module
//! of it.

#[path = "../../runtime/tests/support/carrying.rs"]
mod carrying;

mod capabilities;
mod harness;
mod in_process;
mod message_shape;
