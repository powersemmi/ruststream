//! The core launcher: the thread topology of the runner.
//!
//! These tests are the one place a test starts the runner's cores by hand: the subject is the
//! topology itself (which thread is which core), which the in-process harness, running one core,
//! does not reproduce.

mod launch;
mod signal;
