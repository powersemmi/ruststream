//! The compio app builder: what a built `RustStream` tells about itself and how it places its
//! mounts over the cores, and how it runs: start, health and teardown.

mod batch;
mod builder;
mod connect;
mod harness;
mod health;
mod lifecycle;
mod methods;
mod replies;
mod scope;
mod shutdown;
mod support;
