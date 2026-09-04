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
//! use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
//! use ruststream::subscriber;
//! # #[derive(serde::Deserialize)]
//! # struct Order;
//!
//! // The attribute fixes the worker policy; the name is left to the mount site.
//! #[subscriber(workers(4))]
//! async fn audit(order: &Order) -> HandlerOutcome {
//!     HandlerOutcome::ack()
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

use crate::codec::Codec;
use crate::{FromName, StartAt, Unnamed};

use super::dispatch::Workers;
use super::failure::FailurePolicies;
use super::input::{Decoded, DecodedPair, Provided};
use super::router::{IncludeDef, InputCodec};

/// A setting the attribute left out, still fillable at the mount site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Open;

/// A setting already named, in the attribute or by an earlier builder call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Fixed;

/// The settings state of a definition whose attribute named none of them.
pub type AllOpen = (Open, Open, Open, Open);

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
// what the mount machinery drives. The decoded forms additionally resolve their codec through
// the builder, so a bare definition serves the raw forms; a decoded one declares through
// `SubscriberBuilder::new` in its own `Declared` impl, exactly as the attribute expands.
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
/// `(workers, failure policies, start position, batch supply)` over [`Open`] / [`Fixed`] - the
/// last one recording whether [`batch`](SubscriberSettings::batch) named the batch size, which
/// a batch registration must and a single-message one cannot. The subscription's
/// name is recorded in `Src` instead: an unnamed definition carries [`Unnamed<S>`], which is no
/// [`SubscriptionSource`](crate::SubscriptionSource) at all, so mounting it is a compile error.
/// The `DefCodec` parameter is the decode codec [`codec`](Self::codec) named, `()` while the
/// surface's own applies.
pub struct SubscriberBuilder<Def, Src, State, DefCodec = ()> {
    def: Def,
    source: Src,
    workers: Workers,
    failures: FailurePolicies,
    /// The batch size, present exactly while the state's batch slot reads [`Fixed`] - which is
    /// the only way [`BatchStep`] hands it over.
    batch_size: Option<NonZeroUsize>,
    codec: DefCodec,
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
            batch_size: None,
            codec: (),
            _state: PhantomData,
        }
    }
}

impl<Def, Src, State> SubscriberBuilder<Def, Src, State> {
    /// Decodes this registration with `codec`, overriding the surface's codec: the top rung of
    /// the codec ladder (the [`DefaultCodec`](crate::codec::DefaultCodec), then the scope's or
    /// the chain's codec, then this).
    ///
    /// Available once per registration - the override has no open slot to fill twice - and only
    /// meaningful on the decoded forms: a raw registration reads no codec at all.
    #[must_use]
    pub fn codec<C: Codec>(self, codec: C) -> SubscriberBuilder<Def, Src, State, C> {
        SubscriberBuilder {
            def: self.def,
            source: self.source,
            workers: self.workers,
            failures: self.failures,
            batch_size: self.batch_size,
            codec,
            _state: PhantomData,
        }
    }
}

/// The settings a step moves across a rebuild, next to the definition and the source.
type Collected<DefCodec> = (Workers, FailurePolicies, Option<NonZeroUsize>, DefCodec);

impl<Def, Src, State, DefCodec> SubscriberBuilder<Def, Src, State, DefCodec> {
    /// The pieces a step rebuilds from: the source moves out, so a step can wrap it without
    /// demanding `Clone` of a broker's descriptor.
    fn into_parts(self) -> (Def, Src, Collected<DefCodec>) {
        (
            self.def,
            self.source,
            (self.workers, self.failures, self.batch_size, self.codec),
        )
    }

    /// Rebuilds a builder from moved-out pieces, at whatever source and settings state the step
    /// produced.
    fn from_parts<NewSrc, NewState>(
        def: Def,
        source: NewSrc,
        (workers, failures, batch_size, codec): Collected<DefCodec>,
    ) -> SubscriberBuilder<Def, NewSrc, NewState, DefCodec> {
        SubscriberBuilder {
            def,
            source,
            workers,
            failures,
            batch_size,
            codec,
            _state: PhantomData,
        }
    }

    /// The wrapped definition on its own, so the crate's own tests can call the mount
    /// machinery's accessors on it without a surface in the way.
    #[cfg(test)]
    pub(crate) fn into_def(self) -> Def {
        self.def
    }

    /// Replaces the wrapped definition, keeping the source and the collected settings: the hook
    /// the value-definition methods (`describe`, `documented`, `to`, ...) grow their
    /// definitions through.
    pub(crate) fn map_def<NewDef>(
        self,
        f: impl FnOnce(Def) -> NewDef,
    ) -> SubscriberBuilder<NewDef, Src, State, DefCodec> {
        let (def, source, (workers, failures, batch_size, codec)) = self.into_parts();
        SubscriberBuilder {
            def: f(def),
            source,
            workers,
            failures,
            batch_size,
            codec,
            _state: PhantomData,
        }
    }
}

/// The codec one registration decodes with, resolved from its input kind, the builder's
/// [`codec`](SubscriberBuilder::codec) override, and the surface: a named override wins on a
/// decoded input, a byte input asks for no codec at all, and `()` (nothing named) falls back to
/// the surface's own resolution. Machinery behind `include`; the `()`-vs-named split mirrors
/// the surface-codec fallback pattern of [`InputCodec`].
#[diagnostic::on_unimplemented(
    message = "no codec is available to decode this subscriber's input",
    label = "nothing in this chain names a codec",
    note = "enable a codec feature on `ruststream` (`json`, `cbor` or `msgpack`), name one for \
            the scope (`with_broker_codec(broker, JsonCodec, |b| ..)`) or the registration \
            (`.codec(JsonCodec)`), or give the input type its own decoding with \
            `#[derive(Deserialized)]` so no codec is needed"
)]
#[doc(hidden)]
pub trait DefinitionInputCodec<Input, Surface> {
    /// The resolved codec.
    type Codec: Clone + Send + Sync + 'static;

    /// Produces it, fresh per registration.
    fn resolve(&self, surface: &Surface) -> Self::Codec;
}

impl<Input, Surface: InputCodec<Input>> DefinitionInputCodec<Input, Surface> for () {
    type Codec = Surface::Codec;

    fn resolve(&self, surface: &Surface) -> Self::Codec {
        surface.input_codec()
    }
}

impl<T, Surface, C> DefinitionInputCodec<Decoded<T>, Surface> for C
where
    C: Codec + Clone + Send + Sync + 'static,
{
    type Codec = C;

    fn resolve(&self, _surface: &Surface) -> C {
        self.clone()
    }
}

// The override applies to a pair input the same way: the payload side decodes with it.
impl<H, P, Surface, C> DefinitionInputCodec<DecodedPair<H, P>, Surface> for C
where
    C: Codec + Clone + Send + Sync + 'static,
{
    type Codec = C;

    fn resolve(&self, _surface: &Surface) -> C {
        self.clone()
    }
}

// A self-deserializing input decodes with `()` whatever the chain named: the override has
// nothing to apply to.
impl<F, Surface, C: Codec> DefinitionInputCodec<Provided<F>, Surface> for C {
    type Codec = ();

    fn resolve(&self, _surface: &Surface) {}
}

/// What the mount machinery asks of a definition's settings: the codec its input decodes with,
/// override and surface fallback resolved in one place. Machinery behind `include`.
#[doc(hidden)]
pub trait MountsWith<Input, Surface> {
    /// The resolved codec.
    type Codec: Clone + Send + Sync + 'static;

    /// Produces it, fresh per registration.
    fn mounted_codec(&self, surface: &Surface) -> Self::Codec;
}

impl<Def, Src, State, DC, Input, Surface> MountsWith<Input, Surface>
    for SubscriberBuilder<Def, Src, State, DC>
where
    DC: DefinitionInputCodec<Input, Surface>,
{
    type Codec = DC::Codec;

    fn mounted_codec(&self, surface: &Surface) -> Self::Codec {
        self.codec.resolve(surface)
    }
}

/// The codec a definition `D` with input `I` mounts with on the surface `S`. Tames the
/// projection in the mount impls.
pub(crate) type DefMountCodec<D, I, S> = <D as MountsWith<I, S>>::Codec;

impl<Def, Src, State, DefCodec> Declared for SubscriberBuilder<Def, Src, State, DefCodec>
where
    Def: Declared,
{
    type Form = Def::Form;
    type Settings = Self;

    fn declare(self) -> Self {
        self
    }
}

impl<Def, Src, State, DefCodec> fmt::Debug for SubscriberBuilder<Def, Src, State, DefCodec> {
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

impl<Def, S: FromName, State, DC> NameStep for SubscriberBuilder<Def, Unnamed<S>, State, DC> {
    type Out = SubscriberBuilder<Def, S, State, DC>;

    fn apply_name(self, name: Cow<'static, str>) -> Self::Out {
        let (def, _unnamed, collected) = self.into_parts();
        Self::from_parts(def, S::from_name(name), collected)
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

impl<Def, Src, F, P, B, DC> WorkersStep for SubscriberBuilder<Def, Src, (Open, F, P, B), DC> {
    type Out = SubscriberBuilder<Def, Src, (Fixed, F, P, B), DC>;

    fn apply_workers(self, workers: Workers) -> Self::Out {
        let (def, source, (_default, failures, batch_size, codec)) = self.into_parts();
        Self::from_parts(def, source, (workers, failures, batch_size, codec))
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

impl<Def, Src, W, P, B, DC> FailureStep for SubscriberBuilder<Def, Src, (W, Open, P, B), DC> {
    type Out = SubscriberBuilder<Def, Src, (W, Fixed, P, B), DC>;

    fn apply_failures(self, policies: FailurePolicies) -> Self::Out {
        let (def, source, (workers, _defaults, batch_size, codec)) = self.into_parts();
        Self::from_parts(def, source, (workers, policies, batch_size, codec))
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

impl<Def, Src, W, F, P, B, DC> StartAtStep<P> for SubscriberBuilder<Def, Src, (W, F, Open, B), DC> {
    type Out = SubscriberBuilder<Def, StartAt<Src, P>, (W, F, Fixed, B), DC>;

    fn apply_start_at(self, position: P) -> Self::Out {
        let (def, source, collected) = self.into_parts();
        Self::from_parts(def, StartAt::new(source, position), collected)
    }
}

/// A definition whose deliveries are batches, so a batch size is its to name: every batch form
/// of the value definition, and the attribute's own slot-carrying batch definition.
///
/// Machinery behind [`batch`](SubscriberSettings::batch); never named in user code.
#[diagnostic::on_unimplemented(
    message = "this subscriber has no batches to size",
    label = "`batch(..)` sizes the batches a batch body is handed",
    note = "the batch size belongs to a batch body (`&[T]`, `&[F<'_>]`, `&[Message<H, P>]`), \
            with or without a reply and `Out` slots; a single-message body takes no batch, and \
            how many of those are in flight at once is `workers(n)` instead"
)]
#[doc(hidden)]
pub trait CapsBatches {}

/// Naming the batch size. See [`SubscriberSettings::batch`].
#[diagnostic::on_unimplemented(
    message = "this subscriber's batch size is already named",
    label = "the batch size is named once",
    note = "`batch(n)` is the one batching parameter the framework carries; the broker's own \
            batching options ride its subscription source"
)]
pub trait BatchStep: Sized {
    /// The builder with the batch size named.
    type Out;

    /// Fixes the size of the batches the broker delivers.
    fn apply_batch(self, size: NonZeroUsize) -> Self::Out;
}

impl<Def, Src, W, F, P, DC> BatchStep for SubscriberBuilder<Def, Src, (W, F, P, Open), DC>
where
    Def: CapsBatches,
{
    type Out = SubscriberBuilder<Def, Src, (W, F, P, Fixed), DC>;

    fn apply_batch(self, size: NonZeroUsize) -> Self::Out {
        let (def, source, (workers, failures, _open, codec)) = self.into_parts();
        Self::from_parts(def, source, (workers, failures, Some(size), codec))
    }
}

/// A registration carrying the batch size its subscription opens with: what the batch mounts ask
/// of a definition before they will drive [`BatchSubscriber::batches`](crate::BatchSubscriber).
///
/// The settings builder has it exactly while [`batch`](SubscriberSettings::batch) has been
/// named, which is what makes a batch registration without a size a compile error at the mount
/// rather than a default nobody chose. A hand-written definition mounted without the builder
/// implements this itself, naming the size it was built for.
#[diagnostic::on_unimplemented(
    message = "this batch subscriber has no batch size",
    label = "a batch handler needs one",
    note = "add `.batch(nonzero!(n))` at the mount site: the batch size is the one parameter the \
            framework passes to the broker, and the broker's own batching options (a block \
            timeout, a consumer group) ride its subscription source"
)]
pub trait BatchSized {
    /// The size each delivered batch is capped at.
    fn batch_size(&self) -> NonZeroUsize;
}

impl<Def, Src, W, F, P, DC> BatchSized for SubscriberBuilder<Def, Src, (W, F, P, Fixed), DC> {
    fn batch_size(&self) -> NonZeroUsize {
        // `Fixed` is reachable only through `apply_batch`, which puts the value here; the
        // typestate is the guarantee, and this names it rather than inventing a default.
        self.batch_size
            .expect("the fixed batch-size slot carries its size")
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

impl<Def, Src, State, F, NewSrc, DC> MapSourceStep<F> for SubscriberBuilder<Def, Src, State, DC>
where
    F: FnOnce(Src) -> NewSrc,
{
    type Out = SubscriberBuilder<Def, NewSrc, State, DC>;

    fn apply_map_source(self, f: F) -> Self::Out {
        let (def, source, collected) = self.into_parts();
        Self::from_parts(def, f(source), collected)
    }
}

/// The declarative settings, chainable on a `#[subscriber]` definition and on the builder alike.
///
/// Every method is available exactly while its setting is open: the attribute fixes what it
/// names, and the corresponding step trait is implemented only for the open state, so a second
/// naming fails to compile with a message of its own.
///
/// The order in a chain follows from what each step does to the source: [`name`](Self::name)
/// comes first because it constructs the source, and a broker's own settings then transform it
/// through [`map_source`](Self::map_source) - which is also where a broker's batching options
/// live, after the core's own [`batch`](Self::batch) has named the batch size.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::nonzero;
/// use ruststream::runtime::{FailurePolicies, FailurePolicy, HandlerOutcome, SubscriberSettings};
/// use ruststream::subscriber;
/// # #[derive(serde::Deserialize)]
/// # struct Order;
///
/// #[subscriber]
/// async fn audit(order: &Order) -> HandlerOutcome {
///     HandlerOutcome::ack()
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

    /// Opens the subscription in batches of at most `size` messages: the one parameter every
    /// batch handler names, and the only one the framework carries down to the broker.
    ///
    /// The broker builds the batch - `XREADGROUP COUNT`, a pull batch, a poll limit, or the
    /// framework's own client-side buffer where the transport has no batching of its own - and
    /// what the body sees is exactly what the broker delivered, never a slice of it. Everything
    /// else about how a batch forms (a block timeout, a consumer group, a prefetch window) is the
    /// broker's own vocabulary, chained after this on its subscription source.
    ///
    /// Mandatory on a batch body (`&[T]` and friends), whatever else its signature carries:
    /// mounting one without it does not compile. A single-message body takes no batch, so the
    /// step is not offered there at all - how many deliveries it handles at once is
    /// [`workers`](Self::workers).
    fn batch(self, size: NonZeroUsize) -> <Self::Settings as BatchStep>::Out
    where
        Self::Settings: BatchStep,
    {
        self.declare().apply_batch(size)
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
    /// use ruststream::runtime::{HandlerOutcome, SubscriberSettings};
    /// use ruststream::subscriber;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order;
    ///
    /// #[subscriber(MemorySource)]
    /// async fn audit(order: &Order) -> HandlerOutcome {
    ///     HandlerOutcome::ack()
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
    use super::{AllOpen, BatchSized, Declared, SubscriberBuilder, SubscriberSettings};
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
    use crate::runtime::router::IncludeDef;
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

    // The batch-size step is offered per definition, so the stub declares itself one for the
    // check below; the real gate lives on the value definitions' own impls.
    impl super::CapsBatches for Stub {}

    #[test]
    fn the_steps_collect_the_settings_the_mount_reads_back() {
        let built = Stub
            .name("orders")
            .workers(nonzero!(4))
            .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip));

        assert_eq!(built.workers, Workers::pool(nonzero!(4)));
        assert_eq!(built.failures.decode, FailurePolicy::Skip);
        #[cfg(feature = "memory")]
        assert_eq!(
            SubscriptionSource::<ConnectedMemoryBroker>::name(&built.source),
            "orders",
        );
        // The definition rides along untouched: the builder only ever adds settings.
        assert_eq!(built.def, Stub);
    }

    /// A batch definition carries the size the mount named, and only then: `BatchSized` is what
    /// the batch mounts read it back through.
    #[test]
    fn the_batch_size_reaches_the_mount_through_the_fixed_slot() {
        let built = Stub.name("orders").batch(nonzero!(16));
        assert_eq!(BatchSized::batch_size(&built), nonzero!(16));
    }

    #[test]
    fn keyed_lanes_are_the_same_slot_as_the_pool() {
        let built = Stub.name("orders").workers_by_key(nonzero!(2));
        assert_eq!(built.workers, Workers::keyed(nonzero!(2)));
    }

    /// A definition that names its own form instead of expanding into a settings builder: the
    /// shape a hand-written low-level definition has.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Handwritten;

    impl IncludeDef for Handwritten {
        type Form = forms::RawSubscribing;
    }

    #[test]
    fn a_hand_written_definition_is_its_own_settings() {
        // The blanket over `IncludeDef` is what lets `include` drive such a definition directly:
        // it carries its settings itself, so declaring it hands the definition back unchanged.
        assert_eq!(Handwritten.declare(), Handwritten);
    }

    #[test]
    fn the_wrapped_definition_keeps_its_own_surface() {
        // A step moves the definition around; it never replaces what the definition answers for.
        // The mount reads the source off the builder, but the definition still names its own,
        // and it is still the definition the handler comes out of.
        let built = Stub.name("orders");
        let source = SubscriberDef::source(&built.def);
        assert!(format!("{source:?}").contains("stub"));
        built.def.into_handler();
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
