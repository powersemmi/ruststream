//! The declarative settings of a subscriber, as a builder the `#[subscriber]` attribute shares.
//!
//! Name, worker policy, failure policies and start position are values, so each can be named in
//! the attribute, or at the mount site, or partly in each. Both paths run through the same
//! [`SubscriberBuilder`]: the attribute expands into [`Declared::declare`], which applies exactly
//! the settings it names, and the mount site chains the rest on the result.
//!
//! What the attribute already fixed is fixed in the type: each setting's builder step is a trait
//! implemented only for the state where that setting is still open, so naming it twice is a
//! compile error carrying its own message rather than a precedence rule to remember.
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
//! # mod demo {
//! use ruststream::nonzero;
//! use ruststream::runtime::{HandlerResult, SubscriberSettings};
//! use ruststream::subscriber;
//! # #[derive(serde::Deserialize)]
//! # struct Order;
//!
//! // The attribute fixes the worker policy; the name is left to the mount site.
//! #[subscriber(workers(4))]
//! async fn audit(order: &Order) -> HandlerResult {
//!     HandlerResult::Ack
//! }
//!
//! # fn wire(subject: String) {
//! let _mountable = audit.name(subject);
//! # }
//! # }
//! ```

mod forward;

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::{Buffered, FromName, StartAt, Unnamed};

use super::dispatch::Workers;
use super::failure::FailurePolicies;
use super::router::IncludeDef;

/// A setting the attribute left out, still fillable at the mount site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Open;

/// A setting already named, in the attribute or by an earlier builder call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Fixed;

/// The settings state of a definition whose attribute named none of them.
pub type AllOpen = (Open, Open, Open);

/// A value `include` accepts: a `#[subscriber]` definition, or a settings builder over one.
///
/// The macro implements it on the generated definition, applying the settings its attribute
/// named; every hand-written definition gets it for free through its
/// [`IncludeDef`] impl, and so does the builder itself, which makes
/// [`SubscriberSettings`] chainable from either end.
pub trait Declared: Sized {
    /// The mount form token the definition dispatches on (one of the markers in
    /// [`forms`](super::forms)).
    type Form;

    /// The definition the mount machinery drives: the settings builder for a macro-generated
    /// definition, the definition itself for a hand-written one.
    type Settings;

    /// Applies the settings the declaration fixed.
    fn declare(self) -> Self::Settings;
}

// A hand-written definition names its own form and carries its own settings, so it is already
// what the mount machinery drives.
impl<T: IncludeDef> Declared for T {
    type Form = <T as IncludeDef>::Form;
    type Settings = Self;

    fn declare(self) -> Self {
        self
    }
}

/// The declarative settings of one subscriber, over the definition the macro generated.
///
/// Built by [`Declared::declare`] and grown by the [`SubscriberSettings`] methods; the mount
/// machinery reads the source and the policies off it instead of off the definition, so an
/// attribute-named setting and a mount-site one take the same path.
///
/// The `State` parameter records which settings are still open, as
/// `(workers, failure policies, start position)` over [`Open`] / [`Fixed`]. The subscription's
/// name is recorded in `Src` instead: an unnamed definition carries [`Unnamed<S>`], which is no
/// [`SubscriptionSource`](crate::SubscriptionSource) at all, so mounting it is a compile error.
pub struct SubscriberBuilder<Def, Src, State> {
    def: Def,
    source: Src,
    workers: Workers,
    failures: FailurePolicies,
    _state: PhantomData<fn() -> State>,
}

impl<Def, Src> SubscriberBuilder<Def, Src, AllOpen> {
    /// The builder over `def` subscribing on `source`, with every setting still open.
    ///
    /// Called by the `#[subscriber]` expansion; a hand-written definition that wants the same
    /// surface calls it from its own [`Declared`] impl.
    #[must_use]
    pub fn new(def: Def, source: Src) -> Self {
        Self {
            def,
            source,
            workers: Workers::sequential(),
            failures: FailurePolicies::default(),
            _state: PhantomData,
        }
    }
}

impl<Def, Src, State> SubscriberBuilder<Def, Src, State> {
    /// The pieces a step rebuilds from: the source moves out, so a step can wrap it without
    /// demanding `Clone` of a broker's descriptor.
    fn into_parts(self) -> (Def, Src, Workers, FailurePolicies) {
        (self.def, self.source, self.workers, self.failures)
    }

    /// Rebuilds a builder from moved-out pieces, at whatever source and settings state the step
    /// produced.
    fn from_parts<NewSrc, NewState>(
        def: Def,
        source: NewSrc,
        workers: Workers,
        failures: FailurePolicies,
    ) -> SubscriberBuilder<Def, NewSrc, NewState> {
        SubscriberBuilder {
            def,
            source,
            workers,
            failures,
            _state: PhantomData,
        }
    }

    /// Replaces the wrapped definition, keeping the source and the collected settings: the hook
    /// the value-definition methods (`describe`, `documented`, `to`, `codec`) grow their
    /// definitions through.
    pub(crate) fn map_def<NewDef>(
        self,
        f: impl FnOnce(Def) -> NewDef,
    ) -> SubscriberBuilder<NewDef, Src, State> {
        let (builder, ()) = self.split_def(|def| (f(def), ()));
        builder
    }

    /// [`map_def`](Self::map_def) with a passenger: the mapping hands back a second value the
    /// caller keeps (a codec split off a wrapped definition).
    pub(crate) fn split_def<NewDef, Extra>(
        self,
        f: impl FnOnce(Def) -> (NewDef, Extra),
    ) -> (SubscriberBuilder<NewDef, Src, State>, Extra) {
        let (def, source, workers, failures) = self.into_parts();
        let (def, extra) = f(def);
        (
            SubscriberBuilder {
                def,
                source,
                workers,
                failures,
                _state: PhantomData,
            },
            extra,
        )
    }
}

impl<Def, Src, State> Declared for SubscriberBuilder<Def, Src, State>
where
    Def: Declared,
{
    type Form = Def::Form;
    type Settings = Self;

    fn declare(self) -> Self {
        self
    }
}

impl<Def, Src, State> fmt::Debug for SubscriberBuilder<Def, Src, State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriberBuilder")
            .field("workers", &self.workers)
            .field("failures", &self.failures)
            .finish_non_exhaustive()
    }
}

/// Naming the subscription, which is what constructs the source. See
/// [`SubscriberSettings::name`].
#[diagnostic::on_unimplemented(
    message = "this subscriber's subscription is already named",
    label = "the name is fixed in the `#[subscriber(..)]` attribute",
    note = "a setting is named once: keep the name in the attribute, or leave the attribute's \
            source open (`#[subscriber]`, `#[subscriber(Kind)]`) and name it here"
)]
pub trait NameStep: Sized {
    /// The builder over the constructed source.
    type Out;

    /// Builds the subscription kind from `name`.
    fn apply_name(self, name: Cow<'static, str>) -> Self::Out;
}

impl<Def, S: FromName, State> NameStep for SubscriberBuilder<Def, Unnamed<S>, State> {
    type Out = SubscriberBuilder<Def, S, State>;

    fn apply_name(self, name: Cow<'static, str>) -> Self::Out {
        let (def, _unnamed, workers, failures) = self.into_parts();
        Self::from_parts(def, S::from_name(name), workers, failures)
    }
}

/// Setting the dispatch concurrency. See [`SubscriberSettings::workers`].
#[diagnostic::on_unimplemented(
    message = "this subscriber's worker policy is already fixed",
    label = "`workers(..)` is named in the `#[subscriber(..)]` attribute",
    note = "a setting is named once: keep `workers(..)` in the attribute, or drop it there and \
            name the policy here"
)]
pub trait WorkersStep: Sized {
    /// The builder with the worker policy fixed.
    type Out;

    /// Fixes the dispatch concurrency.
    fn apply_workers(self, workers: Workers) -> Self::Out;
}

impl<Def, Src, F, P> WorkersStep for SubscriberBuilder<Def, Src, (Open, F, P)> {
    type Out = SubscriberBuilder<Def, Src, (Fixed, F, P)>;

    fn apply_workers(self, workers: Workers) -> Self::Out {
        let (def, source, _default, failures) = self.into_parts();
        Self::from_parts(def, source, workers, failures)
    }
}

/// Setting the failure policies. See [`SubscriberSettings::on_failure`].
#[diagnostic::on_unimplemented(
    message = "this subscriber's failure policies are already fixed",
    label = "`on_failure(..)` is named in the `#[subscriber(..)]` attribute",
    note = "a setting is named once: keep `on_failure(..)` in the attribute, or drop it there \
            and name the policies here"
)]
pub trait FailureStep: Sized {
    /// The builder with the failure policies fixed.
    type Out;

    /// Fixes the panic and materialization policies.
    fn apply_failures(self, policies: FailurePolicies) -> Self::Out;
}

impl<Def, Src, W, P> FailureStep for SubscriberBuilder<Def, Src, (W, Open, P)> {
    type Out = SubscriberBuilder<Def, Src, (W, Fixed, P)>;

    fn apply_failures(self, policies: FailurePolicies) -> Self::Out {
        let (def, source, workers, _defaults) = self.into_parts();
        Self::from_parts(def, source, workers, policies)
    }
}

/// Setting the start position, which decorates the source. See
/// [`SubscriberSettings::start_at`].
#[diagnostic::on_unimplemented(
    message = "this subscriber's start position is already fixed",
    label = "`start_at(..)` is named in the `#[subscriber(..)]` attribute",
    note = "a setting is named once: keep `start_at(..)` in the attribute, or drop it there and \
            name the position here"
)]
pub trait StartAtStep<P>: Sized {
    /// The builder over the position-decorated source.
    type Out;

    /// Opens the subscription at `position` instead of the broker's default.
    fn apply_start_at(self, position: P) -> Self::Out;
}

impl<Def, Src, W, F, P> StartAtStep<P> for SubscriberBuilder<Def, Src, (W, F, Open)> {
    type Out = SubscriberBuilder<Def, StartAt<Src, P>, (W, F, Fixed)>;

    fn apply_start_at(self, position: P) -> Self::Out {
        let (def, source, workers, failures) = self.into_parts();
        Self::from_parts(def, StartAt::new(source, position), workers, failures)
    }
}

/// Wrapping the source in the framework's own buffer. See [`SubscriberSettings::buffered`].
pub trait BufferedStep: Sized {
    /// The builder over the buffered source.
    type Out;

    /// Buffers single deliveries into batches on the client.
    fn apply_buffered(self, max_size: NonZeroUsize, max_wait: Duration) -> Self::Out;
}

impl<Def, Src, State> BufferedStep for SubscriberBuilder<Def, Src, State> {
    type Out = SubscriberBuilder<Def, Buffered<Src>, State>;

    fn apply_buffered(self, max_size: NonZeroUsize, max_wait: Duration) -> Self::Out {
        let (def, source, workers, failures) = self.into_parts();
        let buffered = Buffered::new(source).max_size(max_size).max_wait(max_wait);
        Self::from_parts(def, buffered, workers, failures)
    }
}

/// Transforming the source under construction: the hook a broker's own settings trait layers on.
/// See [`SubscriberSettings::map_source`].
pub trait MapSourceStep<F>: Sized {
    /// The builder over the transformed source.
    type Out;

    /// Replaces the source with `f`'s result.
    fn apply_map_source(self, f: F) -> Self::Out;
}

impl<Def, Src, State, F, NewSrc> MapSourceStep<F> for SubscriberBuilder<Def, Src, State>
where
    F: FnOnce(Src) -> NewSrc,
{
    type Out = SubscriberBuilder<Def, NewSrc, State>;

    fn apply_map_source(self, f: F) -> Self::Out {
        let (def, source, workers, failures) = self.into_parts();
        Self::from_parts(def, f(source), workers, failures)
    }
}

/// The declarative settings, chainable on a `#[subscriber]` definition and on the builder alike.
///
/// Every method is available exactly while its setting is open: the attribute fixes what it
/// names, and the corresponding step trait is implemented only for the open state, so a second
/// naming fails to compile with a message of its own.
///
/// The order in a chain follows from what each step does to the source: [`name`](Self::name)
/// comes first because it constructs the source, a broker's own settings then transform it
/// through [`map_source`](Self::map_source), and [`buffered`](Self::buffered) wraps it last -
/// broker methods bound to the unwrapped source type stop applying past the wrap.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::nonzero;
/// use ruststream::runtime::{FailurePolicies, FailurePolicy, HandlerResult, SubscriberSettings};
/// use ruststream::subscriber;
/// # #[derive(serde::Deserialize)]
/// # struct Order;
///
/// #[subscriber]
/// async fn audit(order: &Order) -> HandlerResult {
///     HandlerResult::Ack
/// }
///
/// # fn wire(shard: u8) {
/// let _mountable = audit
///     .name(format!("orders-{shard}"))
///     .workers(nonzero!(4))
///     .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip));
/// # }
/// # }
/// ```
pub trait SubscriberSettings: Declared {
    /// Names the subscription, building the kind the attribute fixed.
    ///
    /// Available while the attribute left the source open (`#[subscriber]` for the by-name
    /// source, `#[subscriber(Kind)]` for a named kind); the kind is built through
    /// [`FromName`].
    fn name(self, name: impl Into<Cow<'static, str>>) -> <Self::Settings as NameStep>::Out
    where
        Self::Settings: NameStep,
    {
        self.declare().apply_name(name.into())
    }

    /// Processes up to `count` deliveries (or batches) of this subscriber concurrently, each in
    /// its own task. The mount-site spelling of `workers(n)`.
    fn workers(self, count: NonZeroUsize) -> <Self::Settings as WorkersStep>::Out
    where
        Self::Settings: WorkersStep,
    {
        self.declare().apply_workers(Workers::pool(count))
    }

    /// Runs `count` sequential lanes keyed by the message's partition key, preserving per-key
    /// ordering. The mount-site spelling of `workers(n, by_key)`.
    fn workers_by_key(self, count: NonZeroUsize) -> <Self::Settings as WorkersStep>::Out
    where
        Self::Settings: WorkersStep,
    {
        self.declare().apply_workers(Workers::keyed(count))
    }

    /// Sets the policies applied to a handler panic and to a delivery that fails to
    /// materialize. The mount-site spelling of `on_failure(panic = .., decode = ..)`.
    fn on_failure(self, policies: FailurePolicies) -> <Self::Settings as FailureStep>::Out
    where
        Self::Settings: FailureStep,
    {
        self.declare().apply_failures(policies)
    }

    /// Opens the subscription at `position` instead of the broker's default. The mount-site
    /// spelling of `start_at(<position>)`.
    fn start_at<P>(self, position: P) -> <Self::Settings as StartAtStep<P>>::Out
    where
        Self::Settings: StartAtStep<P>,
    {
        self.declare().apply_start_at(position)
    }

    /// Wraps the subscription in the framework's own buffer, so a handler taking `&[T]` gets
    /// batches on a broker whose subscription does not batch by itself.
    ///
    /// A batch closes at `max_size` deliveries, or `max_wait` after its first one. Broker-native
    /// batching is a different thing, configured through the broker's own settings.
    fn buffered(
        self,
        max_size: NonZeroUsize,
        max_wait: Duration,
    ) -> <Self::Settings as BufferedStep>::Out
    where
        Self::Settings: BufferedStep,
    {
        self.declare().apply_buffered(max_size, max_wait)
    }

    /// Transforms the source under construction.
    ///
    /// The hook a broker crate layers its own settings trait on: core cannot know that a
    /// subscription has a `JetStream` stream or a consumer group, so the broker declares a trait
    /// over `SubscriberBuilder` bound to its own source type and implements each method as one
    /// `map_source` call. The bound means those methods simply do not exist on a builder for
    /// another broker.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # mod demo {
    /// use ruststream::memory::MemorySource;
    /// use ruststream::runtime::{HandlerResult, SubscriberSettings};
    /// use ruststream::subscriber;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order;
    ///
    /// #[subscriber(MemorySource)]
    /// async fn audit(order: &Order) -> HandlerResult {
    ///     HandlerResult::Ack
    /// }
    ///
    /// # fn wire() {
    /// let _mountable = audit.name("orders").map_source(|source| source);
    /// # }
    /// # }
    /// ```
    fn map_source<F>(self, f: F) -> <Self::Settings as MapSourceStep<F>>::Out
    where
        Self::Settings: MapSourceStep<F>,
    {
        self.declare().apply_map_source(f)
    }
}

impl<D: Declared> SubscriberSettings for D {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{AllOpen, Declared, SubscriberBuilder, SubscriberSettings};
    // Reading a source's name needs some connected broker to name the impl, and the in-process
    // one is the only broker the core ships. The settings themselves are broker-agnostic, so
    // that one assertion is gated rather than the whole module.
    #[cfg(feature = "memory")]
    use crate::SubscriptionSource;
    #[cfg(feature = "memory")]
    use crate::memory::ConnectedMemoryBroker;
    use crate::runtime::dispatch::Workers;
    use crate::runtime::failure::{FailurePolicies, FailurePolicy};
    use crate::runtime::forms;
    use crate::runtime::input::Decoded;
    use crate::runtime::subscriber_def::SubscriberDef;
    use crate::{Name, Unnamed, nonzero};

    /// Stands in for a generated definition: the steps only move it around, so it carries the
    /// bare structural surface a definition has, and none of the settings.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Stub;

    impl SubscriberDef for Stub {
        type Input = Decoded<u32>;
        type Context = ();
        type Handler = ();
        type Source = Name;

        fn source(&self) -> Name {
            Name::new("stub")
        }

        fn into_handler(self) {}
    }

    impl Declared for Stub {
        type Form = forms::Subscribing;
        type Settings = SubscriberBuilder<Self, Unnamed<Name>, AllOpen>;

        fn declare(self) -> Self::Settings {
            SubscriberBuilder::new(self, Unnamed::new())
        }
    }

    #[test]
    fn the_steps_collect_the_settings_the_mount_reads_back() {
        let built = Stub
            .name("orders")
            .workers(nonzero!(4))
            .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip))
            .buffered(nonzero!(8), Duration::from_millis(5));

        assert_eq!(built.workers, Workers::pool(nonzero!(4)));
        assert_eq!(built.failures.decode, FailurePolicy::Skip);
        // The buffer wraps the named source, so the name survives the wrap.
        #[cfg(feature = "memory")]
        assert_eq!(
            SubscriptionSource::<ConnectedMemoryBroker>::name(&built.source),
            "orders",
        );
        // The definition rides along untouched: the builder only ever adds settings.
        assert_eq!(built.def, Stub);
    }

    #[test]
    fn keyed_lanes_are_the_same_slot_as_the_pool() {
        let built = Stub.name("orders").workers_by_key(nonzero!(2));
        assert_eq!(built.workers, Workers::keyed(nonzero!(2)));
    }

    #[test]
    fn a_builder_declares_itself_and_reports_its_settings() {
        let built = Stub.name("orders").map_source(|source| source);
        // Chaining from a builder goes through the same trait, which is what lets the attribute
        // and the mount site share one implementation.
        let same = built.declare();
        assert_eq!(SubscriberDef::workers(&same), Workers::sequential());
        assert_eq!(
            SubscriberDef::failure_policies(&same),
            FailurePolicies::default()
        );
        assert!(format!("{same:?}").contains("SubscriberBuilder"));
    }
}
