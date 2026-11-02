//! The compio app builder: what a built `RustStream` tells about itself and how it places its
//! mounts over the cores, and how it runs: start, health and teardown.

mod builder;
mod health;
mod lifecycle;
mod shutdown;
mod support;
