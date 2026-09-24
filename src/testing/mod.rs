//! Application unit-testing support, behind the `testing` feature.
//!
//! [`TestApp`] drives a built [`RustStream`](crate::runtime::RustStream) application in process:
//! no network `connect`, no server. It connects the app's brokers, mounts the handlers, and
//! records what each handler received (raw and decoded), how it settled, and what it published,
//! so a unit test asserts on the real dispatch path. A broker plugs into the harness (and into
//! the [`conformance`](crate::conformance) suite) by implementing [`TestableBroker`] on its
//! in-process transport and registering it with
//! [`register_testable_broker!`](crate::register_testable_broker).
//! [`MemoryBroker`](crate::memory::MemoryBroker) is a real broker, not a test double; the harness
//! drives it, or any broker, through the dispatch path the production runtime uses. What the
//! in-process transport does not cover is the real server's storage and redelivery semantics:
//! durable consumers, redelivery timers and partitions belong in an integration suite gated
//! behind an environment variable, over the same handler modules.
//!
//! # Examples
//!
//! Enable the feature in the dev-dependencies, build the app exactly as production does, start
//! the harness and publish an input. The publish drives the whole reaction to completion before
//! it returns: the handler, its downstream publishes, any cross-broker cascade. Then assert.
//!
//! ```
//! # #[cfg(all(feature = "testing", feature = "macros", feature = "memory", feature = "json"))]
//! # mod demo {
//! use ruststream::memory::prelude::*;
//! use ruststream::testing::TestApp;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
//! struct Order {
//!     id: u64,
//!     quantity: u32,
//! }
//!
//! #[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
//! #[outgoing(name = "confirmations")]
//! struct Confirmation {
//!     id: u64,
//!     accepted: bool,
//! }
//!
//! #[subscriber("orders", publish)]
//! async fn confirm(order: &Order) -> Confirmation {
//!     Confirmation {
//!         id: order.id,
//!         accepted: order.quantity > 0,
//!     }
//! }
//!
//! pub async fn confirms_valid_orders() -> Result<(), Box<dyn std::error::Error>> {
//!     let app = RustStream::new(AppInfo::new("orders", "0.0.0")).with_broker(
//!         MemoryBroker::new(),
//!         |b| {
//!             b.include(confirm).out_reply(Publish);
//!         },
//!     );
//!     let tb = TestApp::start(app).await?;
//!     tb.broker::<MemoryBroker>()
//!         .message(&Order { id: 1, quantity: 2 })
//!         .to("orders")
//!         .publish()
//!         .await?;
//!
//!     tb.broker::<MemoryBroker>()
//!         .subscriber("orders")
//!         .assert_called_once()
//!         .with(&Order { id: 1, quantity: 2 })
//!         .settled(HandlerOutcome::ack());
//!     tb.broker::<MemoryBroker>()
//!         .published::<Confirmation>("confirmations")
//!         .assert_called_once()
//!         .with(&Confirmation {
//!             id: 1,
//!             accepted: true,
//!         });
//!     Ok(())
//! }
//! # }
//! # #[cfg(all(feature = "testing", feature = "macros", feature = "memory", feature = "json"))]
//! # fn main() {
//! #     tokio::runtime::Builder::new_multi_thread()
//! #         .enable_all()
//! #         .build()
//! #         .unwrap()
//! #         .block_on(demo::confirms_valid_orders())
//! #         .unwrap();
//! # }
//! # #[cfg(not(all(
//! #     feature = "testing", feature = "macros", feature = "memory", feature = "json"
//! # )))]
//! # fn main() {}
//! ```
//!
//! # What a test can say
//!
//! [`TestApp::broker`] addresses a broker by type, [`broker_named`](TestApp::broker_named) by
//! the label of `with_broker_labeled`; a single-broker app may leave it out and publish through
//! [`TestApp::message`]. Input goes in through the same builder the service publishes with:
//! `message(&value)` for an [`Outgoing`](macro@crate::Outgoing) value, `with_headers(&meta)` for
//! a typed contract, `to(name)` when the type names no destination. Bytes that are no model ride
//! a `#[derive(Outgoing, Serialized)]` newtype, which is how a test injects an undecodable
//! payload or the input of a handler that decodes the bytes itself.
//!
//! `subscriber(name)` asserts on what a handler received. `assert_called_once`,
//! `assert_called(n)` and `assert_not_called` count handler calls, one per delivery or one per
//! batch; `with(&value)` and `with_raw(bytes)` read the most recent call's payload;
//! `settled(outcome)` and `assert_outcome(..)` how it settled; `assert_batch_sizes(&[2, 1])` how
//! the stream was cut; `panicked()` and `assert_last_failed_to_decode()` the failures.
//! `received::<T>()`, `received_raw()`, `batches::<T>()` and `outcomes()` return the lists for a
//! check of your own. `published::<T>(name)` reads the broker's publish log with the same
//! vocabulary, plus `with_header(key, value)` for what a transform or a publish layer added.
//! [`TestApp::out`] reads exactly what left through one `Out` slot, across brokers, and
//! `with_options` and `assert_options_default` read the per-message settings a publish carried.
//! The decoding assertions use the default codec; a mounting with another codec passes it
//! through the `with_codec`, `received_with` and `decoded_with` variants.
//!
//! The harness runs every subscription loop and every worker of `workers(n)` on the test's own
//! runtime, so [`advance`](TestApp::advance) reaches every timer the app arms and
//! [`settle`](TestApp::settle) sees every delivery.
//!
//! The harness runs dispatch under the app's real failure policy. A panic under the default
//! `fail_fast` shuts the service down, [`run_result`](TestApp::run_result) returns what `run`
//! would, and [`assert_running`](TestApp::assert_running) states the opposite. A handler
//! answering `retry_after` records the immediate outcome, and [`advance`](TestApp::advance)
//! moves the paused clock to drive the redelivery. A copy the runtime publishes for a zero delay
//! goes out at once, so it arrives in the same reaction, without `advance`.
//! [`settle`](TestApp::settle) drives to completion a reaction the test started through a bare
//! publisher, which is how a batch gets more than one element on a broker that assembles its
//! batches on the client.

#[cfg(feature = "testing")]
mod assertions;
#[cfg(feature = "testing")]
mod broker;
#[cfg(feature = "testing")]
pub(crate) mod coordinator;
#[cfg(feature = "testing")]
mod harness;

#[cfg(feature = "testing")]
pub use assertions::{PublishedAssertions, SubscriberAssertions};
#[cfg(feature = "testing")]
pub use broker::{TestableBroker, TestableRegistration, expect_published};
#[cfg(feature = "testing")]
pub use coordinator::{Coordinator, Outcome};
#[cfg(feature = "testing")]
pub use harness::{BrokerHandle, InjectSink, TestApp, TestBrokers, TestError};
