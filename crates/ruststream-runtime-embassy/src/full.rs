//! What a delayed retry does when its mount's store of copies is full: the policies a mount site
//! picks with `.when_retry_full(..)`.

use ruststream_runtime::{DeadLetterDeclared, WhenFull};

use crate::lanes::Room;

/// The default when a mount site names nothing: the run panics.
///
/// A device resets rather than losing a retry or spinning on a record that never frees. The panic
/// message names the subscription, the message type and the capacity.
///
/// # Examples
///
/// ```
/// # use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// # use ruststream_runtime::{Deserialized, HandlerOutcome, Input, SoloDeserialized, Context, LocalHandle, local_subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts, Panic};
/// # struct Job;
/// # impl Deserialized for Job { type Output<'a> = Job; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Job, Self::Error> { Ok(Job) } }
/// # impl Input for Job { type Axis = SoloDeserialized<Job>; }
/// # struct Work;
/// # impl LocalHandle<Job> for Work {
/// #     fn handle(&self, _: &Job, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(local_subscriber(LocalMemorySource::new("jobs"), Work).build())
///             .retry_capacity::<4>()
///             .when_retry_full(Panic)
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct Panic;

/// Sends the delivery to the mount's dead-letter destination when the store is full, and settles
/// the original. Needs `.dead_letter(..)` declared on the mount: a mount without it does not
/// compile.
///
/// # Examples
///
/// ```
/// # use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// # use ruststream_runtime::{Deserialized, HandlerOutcome, Input, SoloDeserialized, Context, LocalHandle, local_subscriber};
/// # use ruststream_runtime_embassy::{DeadLetter, EmbeddedApp, MountSteps, Mounts};
/// # struct Job;
/// # impl Deserialized for Job { type Output<'a> = Job; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Job, Self::Error> { Ok(Job) } }
/// # impl Input for Job { type Axis = SoloDeserialized<Job>; }
/// # struct Work;
/// # impl LocalHandle<Job> for Work {
/// #     fn handle(&self, _: &Job, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(local_subscriber(LocalMemorySource::new("jobs"), Work).build())
///             .dead_letter("jobs.dead")
///             .retry_capacity::<4>()
///             .when_retry_full(DeadLetter)
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct DeadLetter;

/// Publishes the copy at once when the store is full, instead of holding it for its delay. The
/// retry count is advanced, so `max_attempts` still counts the attempt; the original is settled.
///
/// # Examples
///
/// ```
/// # use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// # use ruststream_runtime::{Deserialized, HandlerOutcome, Input, SoloDeserialized, Context, LocalHandle, local_subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts, SkipDelay};
/// # struct Job;
/// # impl Deserialized for Job { type Output<'a> = Job; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Job, Self::Error> { Ok(Job) } }
/// # impl Input for Job { type Axis = SoloDeserialized<Job>; }
/// # struct Work;
/// # impl LocalHandle<Job> for Work {
/// #     fn handle(&self, _: &Job, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(local_subscriber(LocalMemorySource::new("jobs"), Work).build())
///             .retry_capacity::<4>()
///             .when_retry_full(SkipDelay)
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct SkipDelay;

/// A policy for a full store of copies. Machinery; never named directly.
#[doc(hidden)]
pub trait FullPolicy {
    /// What the store does.
    const WHEN: WhenFull;
}

impl FullPolicy for Panic {
    const WHEN: WhenFull = WhenFull::Panic;
}

impl FullPolicy for DeadLetter {
    const WHEN: WhenFull = WhenFull::DeadLetter;
}

impl FullPolicy for SkipDelay {
    const WHEN: WhenFull = WhenFull::SkipDelay;
}

/// A policy the registration `Registration` can use. Machinery; the guidance a mount site reads
/// lives here.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no `.dead_letter(..)` to send the delivery to",
    label = "this registration declares no dead-letter destination",
    note = "declare one right after `include(..)`, before `.retry_capacity::<CAP>()`: \
            `b.include(job).dead_letter(\"jobs.dead\").retry_capacity::<4>()\
            .when_retry_full(DeadLetter)`"
)]
pub trait PolicyFits<Registration>: FullPolicy {}

impl<Registration> PolicyFits<Registration> for Panic {}

impl<Registration> PolicyFits<Registration> for SkipDelay {}

impl<Registration: DeadLetterDeclared> PolicyFits<Registration> for DeadLetter {}

/// A registration that keeps a store of copies. Machinery; the guidance a mount site reads lives
/// here.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.when_retry_full(..)` needs a store of copies to be full",
    label = "this registration keeps none",
    note = "add `.retry_capacity::<CAP>()` before it"
)]
pub trait HasRetryRoom {}

impl<const CAPACITY: usize> HasRetryRoom for Room<CAPACITY> {}
