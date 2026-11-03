//! The local bus without `alloc`: a [`LocalMemoryBus`] of fixed size that a board declares as a
//! `static`, the broker over it, and its subscriptions. The messages sit in fixed buffers, so no
//! publish, delivery or settlement allocates.

mod broker;
mod bus;
mod error;
mod publisher;
mod subscriber;
#[cfg(feature = "testing")]
mod testing;

pub use broker::{ClosedLocalMemoryBroker, ConnectedLocalMemoryBroker, LocalMemoryBroker};
pub use bus::LocalMemoryBus;
pub use error::LocalMemoryError;
pub use publisher::{LocalMemoryPublish, LocalMemoryPublisher};
pub use subscriber::{
    LocalMemoryBatch, LocalMemoryMessage, LocalMemorySource, LocalMemorySubscriber,
};
