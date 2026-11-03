//! The local bus on the heap: `Rc` or `Arc` shares its state, and its messages are `bytes`
//! buffers. It is the bus of every build whose framework has `alloc`.

mod broker;
mod bus;
mod share;
mod subscriber;

/// The heap's types, which the standard prelude names when `std` is on and this module reaches
/// through `alloc` without it.
mod alloc_prelude {
    pub(super) use alloc::boxed::Box;
    pub(super) use alloc::string::String;
    pub(super) use alloc::vec::Vec;
}

pub use broker::{
    ClosedLocalMemoryBroker, ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError,
    LocalMemoryPublish, LocalMemoryPublisher,
};
pub use subscriber::{LocalMemoryMessage, LocalMemorySource, LocalMemorySubscriber};
