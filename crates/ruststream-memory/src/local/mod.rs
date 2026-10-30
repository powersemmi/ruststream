//! The in-process broker for local runtimes: [`LocalMemoryBroker`], a full broker on
//! embassy-sync channels whose types are not `Send`, and the reference implementation of the
//! `Local*` broker contract.
//!
//! It runs on any executor and builds without `std`, with an allocator. A device service uses it
//! for queues between the parts of one firmware; the host harness drives it in tests.
//!
//! - [`LocalMemoryBroker`], [`ConnectedLocalMemoryBroker`], [`ClosedLocalMemoryBroker`]: the
//!   lifecycle ladder of the bus. Its `CAP` is the capacity of each subscription.
//! - [`LocalMemorySource`], [`LocalMemorySubscriber`], [`LocalMemoryMessage`]: a subscription to
//!   one name, its stream and batches, and one delivery with its settlement.
//! - [`LocalMemoryPublish`], [`LocalMemoryPublisher`]: the publish policy and the live publisher.
//! - [`LocalMemoryError`]: a publish to a full subscription, or any use of a shut-down bus.
//! - [`prelude`]: the bus and the `Local*` traits a device service calls it through.
//!
//! How a device service uses it is taught on its component page:
//! [Local memory broker](https://powersemmi.github.io/ruststream/latest/components/memory/local/).
//!
//! # Examples
//!
//! ```
//! use futures::StreamExt;
//! use ruststream_memory::local::prelude::*;
//!
//! /// Two tasks of one firmware: the sensor publishes, the logger reads.
//! async fn firmware() -> Result<Vec<u8>, LocalMemoryError> {
//!     let connected = LocalMemoryBroker::<8>::new().connect().await?;
//!     let mut readings = LocalMemorySource::new("sensors/temp")
//!         .subscribe(&connected)
//!         .await?;
//!     let sensor = connected.publisher();
//!     for celsius in [21u8, 22] {
//!         let reading = OutgoingMessage::produced("sensors/temp", BytesMut::from(&[celsius][..]));
//!         sensor.publish(reading, None).await?;
//!     }
//!
//!     let mut logged = Vec::new();
//!     let mut deliveries = core::pin::pin!(readings.stream());
//!     for _ in 0..2 {
//!         let reading = deliveries.next().await.expect("the bus is open")?;
//!         logged.push(reading.payload()[0]);
//!         reading.ack().await.expect("the bus settles");
//!     }
//!     connected.shutdown().await?;
//!     Ok(logged)
//! }
//!
//! assert_eq!(embassy_futures::block_on(firmware()), Ok(vec![21, 22]));
//! ```

// The bus is `Rc` and `RefCell` by design, so no future of the local family is `Send`, and the
// nursery lint that asks for it does not apply here.
#![allow(clippy::future_not_send)]

mod broker;
mod bus;
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

/// The local bus and the `Local*` traits a device service calls it through.
///
/// It carries the local family of the contract, so a module that imports it does not import the
/// Send family's prelude beside it: a method call would then match two traits.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// /// One glob brings the bus, its descriptor and the traits its methods come from.
/// async fn open(broker: LocalMemoryBroker<4>) -> Result<LocalMemorySubscriber<4>, LocalMemoryError> {
///     let connected = broker.connect().await?;
///     LocalMemorySource::new("sensors/temp").subscribe(&connected).await
/// }
///
/// assert!(embassy_futures::block_on(open(LocalMemoryBroker::new())).is_ok());
/// ```
pub mod prelude {
    pub use super::{
        ClosedLocalMemoryBroker, ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError,
        LocalMemoryMessage, LocalMemoryPublish, LocalMemoryPublisher, LocalMemorySource,
        LocalMemorySubscriber,
    };
    pub use ruststream_core::{
        AckError, BytesMut, HeaderMap, LocalBatchSubscriber, LocalBroker, LocalConnectedBroker,
        LocalIncomingMessage, LocalPublishPolicy, LocalPublisher, LocalSubscribe, LocalSubscriber,
        LocalSubscriptionSource, OutgoingMessage,
    };
}
