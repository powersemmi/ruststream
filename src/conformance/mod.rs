//! Conformance suites for broker authors: the checks that prove a broker crate honours the core
//! contract.
//!
//! A broker crate takes them as a dev-dependency. The feature enables `testing`, so the one
//! in-process mode the crate ships serves these suites and the [`TestApp`](crate::testing::TestApp)
//! harness a service writes:
//!
//! ```toml
//! [dev-dependencies]
//! ruststream = { version = "0.7", features = ["conformance"] }
//! ```
//!
//! Every suite panics with a message naming the step on the first contract violation. The
//! [`MemoryBroker`](crate::memory::MemoryBroker) passes every suite of every capability it
//! implements, and is the executable reference for what each expects.
//!
//! # The suites
//!
//! - [`harness::run_suite`]: the routing surface of the broker's in-process mode, from a factory
//!   of production brokers. These are the routing guarantees every broker meets; semantics of the
//!   broker's own (durable resume, redelivery on timeout, partition assignment) belong to the
//!   crate's end-to-end suite against a real server.
//! - [`harness::lifecycle`]: the lifecycle ladder end to end on the real broker, from every runtime
//!   a service reaches it from, and the aliased-handle contract after shutdown.
//! - [`lifecycle`]: what a shutdown must finish, and clones of a shareable connected form.
//! - [`settlement`]: what `ack` and `nack` mean on the transport the broker connects to, and the
//!   same answers live and in process.
//! - [`retry`]: where a retry copy goes and how a broker that moves deliveries itself maps the
//!   cap; [`harness::redelivery_address`] is the check for an addressed descriptor.
//! - [`in_process`]: the in-process transport's declarations held to the server's behaviour.
//! - [`message_shape`]: keys, per-message settings and credentials in what a broker publishes.
//! - [`capabilities`]: one suite per capability trait, called for each capability the broker
//!   implements.
//!
//! The suites that take a [`Broker`](crate::Broker) connect it with `connect`. Wrapping the
//! production broker in [`harness::InProcessBroker`] runs each of them a second time with no
//! server, over the crate's own descriptors and publish policies: the live pass proves the
//! transport, the in-process pass keeps the in-process mode honest. A suite that needs a server is
//! enabled by an environment variable, such as `NATS_TEST_URL`.
//!
//! Every suite names its subject through [`helpers::unique_subject`], so a run reads only what it
//! published and stays re-runnable against a server that keeps what the last run left. A crate's
//! own end-to-end suite has the same problem and the same answer.
//!
//! # Author checklist
//!
//! Before publishing a broker crate:
//!
//! - [`Broker`](crate::Broker), [`ConnectedBroker`](crate::ConnectedBroker),
//!   [`Subscribe`](crate::Subscribe) or a [`SubscriptionSource`](crate::SubscriptionSource),
//!   [`Subscriber`](crate::Subscriber), [`IncomingMessage`](crate::IncomingMessage),
//!   [`Publisher`](crate::Publisher) and the [`PublishPolicy`](crate::PublishPolicy) that
//!   constructs it are implemented.
//! - `shutdown` performs every teardown step that can fail, and never blocks or panics.
//! - `nack` honours its `requeue` flag.
//! - The crate owns its `Config`, and a field with no sane default has no `Default`.
//! - A capability trait is implemented only where the transport supports it, and passes its
//!   [`capabilities`] suite.
//! - The in-process mode ships under a `testing` feature:
//!   [`InProcess`](crate::testing::InProcess) on the broker,
//!   [`TestableBroker`](crate::testing::TestableBroker) on its connected form, and
//!   [`register_testable_broker!`](crate::register_testable_broker). It reads every setting from
//!   the production broker and never succeeds where the real broker fails.
//! - [`harness::run_suite`] passes, and the other suites pass in process through
//!   [`harness::InProcessBroker`] as well as against a real server.
//! - [`harness::lifecycle`] passes against a real server.
//! - [`lifecycle::shutdown_flushes`] passes with the broker's `Backlog` answer, and
//!   [`lifecycle::shared_handle_closes`] where the connected form is `Clone`.
//! - [`settlement::matches_in_process`] passes against a real server.
//! - [`harness::redelivery_address`] passes for every `AddressedCopies` descriptor, and for the
//!   bare `Name` where `Subscribe::Copies` is `AddressedCopies`; [`retry::broker_moves`] passes for
//!   every `BrokerMoves` descriptor.
//! - Where the transport has keys or per-message settings, [`message_shape::keyed_order`] and
//!   [`message_shape::publish_options`] pass, in process and against a real server.
//! - An end-to-end suite covers the broker's own semantics against a real server.
//! - `Cargo.toml` metadata is complete (`description`, `license`, `repository`, `keywords`,
//!   `categories`), and CI checks `--no-default-features` and `--all-features`.

#[doc(inline)]
pub use ruststream_conformance::*;
