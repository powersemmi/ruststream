//! In-process broker: the bus, and what it keeps of what went over it.
//!
//! [`MemoryBroker`] is a complete broker inside the service's process and the reference
//! implementation the [`conformance`](crate::conformance) suites run against. A service mounting
//! on it globs [`prelude`].
//!
//! - [Memory broker][memory]: [`MemoryBroker`], its subscription source [`MemorySource`], its
//!   publish policies, and how its deliveries settle and come back.
//! - [Retention][retention]: [`MemoryBroker::retaining`] keeps the newest messages of every name
//!   within a [`Retention`] bound, and its subscriptions replay over them through
//!   [`MemoryPosition`] and [`MemorySeeker`].
//! - [Patterns][patterns]: a [`MemoryPattern`] subscription reads every name its pattern
//!   matches, and the broker's [`Routing`] decides who receives a shared match.
//!
//! - [Local memory broker][local]: `memory::local`, the bus a service on the `embassy` runtime
//!   mounts on, whose types are not `Send`.
//!
//! [memory]: https://powersemmi.github.io/ruststream/latest/components/memory/
//! [retention]: https://powersemmi.github.io/ruststream/latest/components/memory/retention/
//! [patterns]: https://powersemmi.github.io/ruststream/latest/components/memory/patterns/
//! [local]: https://powersemmi.github.io/ruststream/latest/components/memory/local/

#[doc(inline)]
pub use ruststream_memory::*;

#[cfg(feature = "tokio")]
pub mod prelude;
