//! What one registration owes its retries at startup: the publisher a copy of a delivery leaves
//! through, where such a copy goes, and the declaration the mount site made.
//!
//! A copy needs a destination, and a subscription's own name is not one: where a subscription and
//! a publish destination are separate resources (a Pub/Sub subscription and its topic), a copy
//! published under the subscription's name reaches nothing. Which of the two a descriptor is, and
//! whether it can name the destination at all, is its own type
//! ([`SubscriptionSource::Copies`](crate::SubscriptionSource::Copies)), so the mount site is told
//! at compile time and nothing here refuses to start over an address.

use alloc::borrow::Cow;
use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use core::any::type_name;
use core::fmt;
use core::future::ready;
use core::marker::PhantomData;
use core::num::NonZeroU32;

use thiserror::Error;
use tracing::info;

use crate::dispatch::Delivery;
use crate::lifecycle::BoxError;
use crate::metadata::{HandlerMetadata, PublishDescription};
#[cfg(feature = "poll-diagnostics")]
use crate::poll_diagnostics::PollDiagnostics;
#[cfg(test)]
use crate::publish::PublishIdentity;
use crate::publish::{
    ForReply, OutPipeline, PublishContext, PublishTransform, PublishTransformIdentity,
};
use crate::{
    AddressedCopies, Broker, BrokerMoves, Connected, ConnectedBroker, DeclareRetryError,
    DefaultPublish, NamedCopies, OutgoingMessage, PairError, PayloadForm, PublishPolicy, Publisher,
    RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, SubscriptionSource,
};

#[cfg(feature = "testing")]
use crate::testing::coordinator::{Origin, TestHooks, paired, publishing_to};

mod kept;

pub use kept::{
    AddressedCopy, AppliesDeclaration, CopyPublisher, DeclaredOn, KeepCopies, KeptAddress,
    KeptTransforms, LocalRetrySlot, NoCopies, NoCopyPath, PairCopies, PairSlot, PrepareCopy,
    SlotAddress, Undeclared,
};

/// The plain publish policy of a broker's connected form, which is what a retry copy leaves
/// through until a mount site names another.
type DefaultPolicy<B> = <Connected<B> as DefaultPublish>::Policy;

/// The live publisher that policy pairs into.
type DefaultLive<B> = <DefaultPolicy<B> as PublishPolicy<Connected<B>>>::Live;

/// The retry path of one subscription: the publisher a copy of a delivery leaves through, in its
/// concrete type, and where a copy meant for the subscription itself goes.
///
/// The destination is `None` where the mount site named none and the descriptor has none of its
/// own, which is a [`NamedCopies`] registration whose transforms name it per delivery.
#[doc(hidden)]
#[derive(Debug)]
pub struct RetryRoute<Retry> {
    /// The registration's retry slot, with the delivery context its transforms read left generic
    /// in the trait. A publish through it travels the mount site's transforms and the app's
    /// publish pipeline.
    pub(crate) publisher: Retry,
    pub(crate) destination: Option<Arc<str>>,
    /// The subscription's name, which a copy's context and its diagnostics carry; shared, so a
    /// copy takes a reference count rather than a string of its own.
    pub(crate) subscription: Arc<str>,
}

impl<Retry> RetryRoute<Retry> {
    /// A route publishing through `publisher` to `destination`, for the subscription of the
    /// same name. For tests.
    #[cfg(test)]
    pub(crate) fn to(publisher: Retry, destination: &str) -> Self {
        Self {
            publisher,
            destination: Some(Arc::from(destination)),
            subscription: Arc::from(destination),
        }
    }
}

/// The retry publisher of one registration, in its concrete type.
///
/// The copy carries the delivery's own bytes, so nothing here encodes; what the publisher needs
/// is the context, because a transform on this position reads the delivery being retried. The
/// publish future is the publisher's own, held by the task that waits out the delay: no box, no
/// dynamic call. Machinery; never named directly.
#[doc(hidden)]
pub trait RetryCopies<Cx>: Send + Sync + 'static {
    /// Sends one copy, with the delivery it is a copy of in hand.
    fn publish_copy<'a>(
        &'a self,
        msg: OutgoingMessage<'a>,
        cx: &'a PublishContext<'a, Cx>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a;
}

/// The retry publisher of a host that has none: there is none.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub enum NoRetry {}

// No value of the type exists, so no method is ever called: the body is the empty match that
// proves it, the idiom `core::convert::Infallible`'s own impls use.
// The signature names the future's `Send` bound, which an `async fn` cannot state for a context
// that is not known to be `Sync`.
#[allow(clippy::uninhabited_references, clippy::manual_async_fn)]
impl<Cx> RetryCopies<Cx> for NoRetry {
    fn publish_copy<'a>(
        &'a self,
        _msg: OutgoingMessage<'a>,
        _cx: &'a PublishContext<'a, Cx>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
        async move { match *self {} }
    }
}

/// The live retry publisher: the leaf the policy paired into, the mount site's transform stack,
/// and the app's publish pipeline underneath it. Machinery; never named directly.
#[doc(hidden)]
pub struct RetryLeaf<Live, Stack, Pipeline> {
    live: Live,
    stack: Stack,
    pipeline: Pipeline,
    /// The broker the retry publisher was paired against, which the copies go to.
    #[cfg(feature = "testing")]
    origin: Origin,
}

impl<Live, Stack, Pipeline> fmt::Debug for RetryLeaf<Live, Stack, Pipeline> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryLeaf").finish_non_exhaustive()
    }
}

impl<Cx, Live, Stack, Pipeline> RetryCopies<Cx> for RetryLeaf<Live, Stack, Pipeline>
where
    Cx: Send + Sync + 'static,
    Live: Publisher + Send + Sync + 'static,
    Stack: PublishTransform<ForReply<Cx>, Live::Options> + Send + Sync + 'static,
    Pipeline: OutPipeline<Live> + 'static,
{
    async fn publish_copy<'a>(
        &'a self,
        msg: OutgoingMessage<'a>,
        cx: &'a PublishContext<'a, Cx>,
    ) -> Result<(), BoxError> {
        // The publisher's own constants sit under the delivery's headers, as they do under
        // any publish through it; the transforms then see the message as it will be sent.
        let (name, payload, delivered) = msg.into_parts();
        let headers = if let Some(base) = self.live.base_headers() {
            let mut headers = base.clone();
            for (key, value) in delivered.iter() {
                headers.insert(key.to_owned(), value.to_owned());
            }
            headers
        } else {
            // With nothing to sit over, the delivery's map is the outgoing one.
            delivered
        };
        // The copy carries the delivery's own bytes: a transport that reads them is lent
        // them, and only one that keeps them is handed a buffer of its own.
        let mut out = <Live::Payload as PayloadForm>::rebuilt(
            name.into(),
            <Live::Payload as PayloadForm>::Form::from(payload),
            headers,
        );
        let mut options: Option<Live::Options> = None;
        self.stack.apply(&mut out, &mut options, cx);
        // The copy is spent once the leaf has it, so what the transforms wrote moves on
        // rather than being copied on.
        let mut kept = <Live::Payload as PayloadForm>::Spent::default();
        let sent = <Live::Payload as PayloadForm>::handed_on(out, &mut kept);
        let publish = self.pipeline.send(&self.live, sent, options.as_ref());
        #[cfg(feature = "testing")]
        let publish = publishing_to(self.origin, publish);
        publish.await.map_err(BoxError::from)
    }
}

/// The retry publisher of a registration that named neither a transform nor a middleware: what a
/// bare mount produces, for tests that drive the dispatch functions directly.
#[cfg(test)]
pub(crate) fn bare_retry_publisher<P>(
    live: P,
) -> RetryLeaf<P, PublishTransformIdentity, PublishIdentity>
where
    P: Publisher + Send + Sync + 'static,
{
    RetryLeaf {
        live,
        stack: PublishTransformIdentity,
        pipeline: PublishIdentity,
        #[cfg(feature = "testing")]
        origin: Origin::default(),
    }
}

/// Whether a mount site may name the publisher of this descriptor's retry copies.
///
/// Implemented by the two copy paths this process publishes on: where the broker moves the
/// delivery itself there is no publisher of this process's to customise. The descriptor rides the
/// trait's parameter so the compile error names it. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "subscription descriptor `{Source}` publishes no retry copy in this process",
    label = "`out_retry(..)` has no publisher to name on this registration",
    note = "`{Source}` declares `Copies = BrokerMoves`: the broker or its client library moves \
            the delivery itself (a delivery limit with a dead-letter exchange, a subscription's \
            dead-letter policy, a redrive policy), so nothing of this process's is published for \
            it and a publish policy has nothing to shape",
    note = "declare the cap and the destination instead - `.max_attempts(n).dead_letter(\"dlq\")` \
            - and that descriptor maps them onto the broker's own mechanism"
)]
pub trait PublishesCopiesHere<Source> {}

impl<Source> PublishesCopiesHere<Source> for AddressedCopies {}
impl<Source> PublishesCopiesHere<Source> for NamedCopies {}

/// Whether this registration knows where its retry copies go.
///
/// A descriptor that addresses its own copies answers for them, and one whose broker moves the
/// delivery publishes none; a [`NamedCopies`] descriptor answers neither, so the mount site has
/// to - and this is the bound that says so. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this registration does not say where its retry copies go",
    label = "subscription descriptor `{Source}` cannot address them",
    note = "`{Source}` declares `Copies = NamedCopies`: one such subscription reads many \
            destinations (a wildcard subject, a filter, a pattern, a list of topics), so it names \
            none of them and the mount site names one",
    note = "name a fixed destination - `.out_retry(policy).to(\"orders\")` - or compose a publish \
            transform that names one per delivery, which reads the delivery being retried"
)]
pub trait CopiesAddressed<Source> {}

impl<Source> CopiesAddressed<Source> for AddressedCopies {}
impl<Source> CopiesAddressed<Source> for BrokerMoves {}

/// The retry publisher a descriptor's copy path owes every registration mounted on it.
///
/// The two paths this process publishes on owe one, paired from the broker's [`DefaultPublish`]
/// policy the way the default reply publisher is, so a `retry_after` behaves the same whether or
/// not the mount site named a policy; [`BrokerMoves`] owes none. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "broker `{B}` names no default publish policy, so a retry copy has nothing to \
               leave through",
    label = "this subscription's retry copies are published by the runtime",
    note = "the descriptor declares a copy path this process publishes on, so every registration \
            on it gets a retry publisher paired from `DefaultPublish`; implement `DefaultPublish` \
            on the broker's connected form, or declare `Copies = BrokerMoves` on a descriptor \
            whose deliveries the broker moves itself"
)]
pub trait CopyPathPairing<B: Broker, Cx, Pipeline> {
    /// The pairing this copy path owes: the broker's default publisher, or none.
    type Pairing: BoundRetry<B, Cx>;

    /// The pairing, over the app's publish pipeline.
    fn pairing(pipeline: Pipeline) -> Self::Pairing;
}

/// Implements the default pairing for the copy paths this process publishes on.
macro_rules! impl_default_pairing {
    ($($path:ident),+ $(,)?) => {$(
        impl<B, Cx, Pipeline> CopyPathPairing<B, Cx, Pipeline> for $path
        where
            B: Broker + 'static,
            Cx: Send + Sync + 'static,
            Connected<B>: DefaultPublish,
            DefaultPolicy<B>: Send + 'static,
            DefaultLive<B>: Publisher + Send + Sync + 'static,
            Pipeline: OutPipeline<DefaultLive<B>> + 'static,
        {
            type Pairing = RetryParts<DefaultPolicy<B>, PublishTransformIdentity, Pipeline>;

            fn pairing(pipeline: Pipeline) -> Self::Pairing {
                RetryParts::new(
                    DefaultPolicy::<B>::default(),
                    PublishTransformIdentity,
                    pipeline,
                )
            }
        }
    )+};
}

impl_default_pairing!(AddressedCopies, NamedCopies);

impl<B: Broker + 'static, Cx, Pipeline> CopyPathPairing<B, Cx, Pipeline> for BrokerMoves {
    type Pairing = NoPairing;

    fn pairing(_pipeline: Pipeline) -> NoPairing {
        NoPairing
    }
}

/// Where a copy that goes back to the subscription itself is published, read off the descriptor's
/// copy path at startup.
///
/// [`AddressedCopies`] answers from the descriptor, which is why the copy path requires
/// [`RedeliveryAddressed`]; the other two answer nothing, and the mount site is what named the
/// destination. Machinery; never named directly.
#[doc(hidden)]
pub trait CopyPathAddress<C: ConnectedBroker, Source> {
    /// Asks the descriptor, once, at startup.
    fn address(
        source: &Source,
        connected: &C,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>>;
}

impl<C, Source> CopyPathAddress<C, Source> for AddressedCopies
where
    C: ConnectedBroker,
    Source: RedeliveryAddressed<C> + Sync,
{
    async fn address(
        source: &Source,
        connected: &C,
    ) -> Result<Option<RedeliveryAddress>, C::Error> {
        source.redelivery_address(connected).await.map(Some)
    }
}

/// Implements the "nothing to ask" half for the copy paths that name no address of their own.
macro_rules! impl_no_address {
    ($($path:ident),+ $(,)?) => {$(
        impl<C: ConnectedBroker, Source> CopyPathAddress<C, Source> for $path {
            fn address(
                _source: &Source,
                _connected: &C,
            ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> {
                ready(Ok(None))
            }
        }
    )+};
}

impl_no_address!(NamedCopies, BrokerMoves);

/// What one registration hands the runtime about its retries: the publisher a copy leaves
/// through, where the copies go, and what the mount site declared.
///
/// The publisher slot starts [`UnnamedRetry`] and is filled from one of two places - the pairing a
/// `.out_retry(policy)` named, or the descriptor's copy path, which pairs the broker's default -
/// so a registration whose subscription publishes its copies here always has one. Its type
/// follows the slot, so the retry publisher stays concrete from the mount site to the delivery.
#[doc(hidden)]
pub struct RetrySetup<B: Broker, Cx, Slot = UnnamedRetry> {
    publisher: Slot,
    destination: Option<Cow<'static, str>>,
    /// Whether the registration's transforms name the destination per delivery, so the
    /// subscription owes none of its own.
    named_per_delivery: bool,
    declaration: RetryDeclaration,
    _registration: PhantomData<fn() -> (B, Cx)>,
}

// The publisher is a pairing with nothing to print; the declaration is the part worth reading.
impl<B: Broker, Cx, Slot> fmt::Debug for RetrySetup<B, Cx, Slot> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetrySetup")
            .field("destination", &self.destination)
            .field("declaration", &self.declaration)
            .finish_non_exhaustive()
    }
}

impl<B: Broker, Cx> Default for RetrySetup<B, Cx> {
    fn default() -> Self {
        Self {
            publisher: UnnamedRetry,
            destination: None,
            named_per_delivery: false,
            declaration: RetryDeclaration::new(),
            _registration: PhantomData,
        }
    }
}

/// The retry slot of a registration whose mount site named no publisher: the descriptor's copy
/// path pairs the broker's default. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct UnnamedRetry;

/// The retry slot of a registration whose mount site named a publisher with `.out_retry(..)`.
/// Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct NamedRetry<Pairing>(Pairing);

/// What fills a registration's retry slot, once the descriptor's copy path is known: the pairing
/// the mount site named, or the copy path's default. Machinery; never named directly.
#[doc(hidden)]
pub trait RetrySlot<B: Broker, Cx, Copies, Pipeline> {
    /// The pairing the slot resolves to.
    type Resolved: BoundRetry<B, Cx>;

    /// Resolves the slot over the app's publish pipeline.
    fn resolve(self, pipeline: &Pipeline) -> Self::Resolved;
}

impl<B, Cx, Copies, Pipeline> RetrySlot<B, Cx, Copies, Pipeline> for UnnamedRetry
where
    B: Broker,
    Copies: CopyPathPairing<B, Cx, Pipeline>,
    Pipeline: Clone,
{
    type Resolved = Copies::Pairing;

    fn resolve(self, pipeline: &Pipeline) -> Self::Resolved {
        Copies::pairing(pipeline.clone())
    }
}

impl<B, Cx, Copies, Pipeline, Pairing> RetrySlot<B, Cx, Copies, Pipeline> for NamedRetry<Pairing>
where
    B: Broker,
    Pairing: BoundRetry<B, Cx>,
{
    type Resolved = Pairing;

    fn resolve(self, _pipeline: &Pipeline) -> Pairing {
        self.0
    }
}

impl<B: Broker + 'static, Cx, Slot> RetrySetup<B, Cx, Slot> {
    /// Records the pairing a `.out(Retry, policy)` named, replacing the broker's default, and
    /// the destination the `.to(name)` after it gave the copies.
    pub(crate) fn with_publisher<Pairing>(
        self,
        publisher: Pairing,
        destination: Option<Cow<'static, str>>,
        named_per_delivery: bool,
    ) -> RetrySetup<B, Cx, NamedRetry<Pairing>> {
        RetrySetup {
            publisher: NamedRetry(publisher),
            // A `.to(name)` ahead of the publisher already spoke; one after it names the same
            // thing, and the chain admits only one of the two.
            destination,
            named_per_delivery,
            declaration: self.declaration,
            _registration: PhantomData,
        }
    }

    /// Records what `max_attempts(..)` and `dead_letter(..)` declared.
    pub(crate) fn with_declaration(mut self, declaration: RetryDeclaration) -> Self {
        self.declaration = declaration;
        self
    }

    /// Fills in the broker's default publisher where the mount site named none, per the
    /// descriptor's copy path, and tells `meta` what that publisher says about the channels the
    /// copies go to.
    ///
    /// The description travels with the pairing, so a declared channel is described the same
    /// whether the mount site named a publisher or took the broker's default.
    #[must_use]
    pub(crate) fn resolve<Copies, Pipeline>(
        self,
        pipeline: &Pipeline,
        meta: &mut HandlerMetadata,
    ) -> RetrySetup<B, Cx, Slot::Resolved>
    where
        Slot: RetrySlot<B, Cx, Copies, Pipeline>,
    {
        let publisher = self.publisher.resolve(pipeline);
        publisher.describe_copies(meta);
        RetrySetup {
            publisher,
            destination: self.destination,
            named_per_delivery: self.named_per_delivery,
            declaration: self.declaration,
            _registration: PhantomData,
        }
    }
}

/// What one retry position bound: the policy, the mount site's transforms and the app's publish
/// pipeline, waiting for the connected form.
///
/// A policy is declaration and pairs only against a connected broker, which exists after the
/// synchronous builder has run. The policy itself is kept rather than a closure over it, because
/// the two things asked of it come at different times. The document asks what the policy says
/// about a channel once the registration has named it - a `dead_letter(..)` destination, or the
/// one a `.to(name)` gave the copies - which is after the position is bound; pairing consumes the
/// policy later still, at startup. Machinery; never named directly.
#[doc(hidden)]
pub trait BoundRetry<B: Broker, Cx>: Send + 'static {
    /// The live retry publisher the pairing produces, in its concrete type.
    type Leaf: RetryCopies<Cx>;

    /// Tells `meta` what the bound policy says about the channels the copies go to.
    fn describe_copies(&self, meta: &mut HandlerMetadata);

    /// Pairs it against the connected broker, producing the publisher a copy leaves through;
    /// `None` where this registration publishes no copy.
    ///
    /// # Errors
    ///
    /// Returns the policy's own [`PairError`].
    fn pair(
        self,
        connected: &Connected<B>,
    ) -> impl Future<Output = Result<Option<Self::Leaf>, PairError>>;
}

/// The pairing of a registration whose broker moves a spent delivery itself: it publishes no
/// copy. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct NoPairing;

impl<B: Broker + 'static, Cx> BoundRetry<B, Cx> for NoPairing {
    type Leaf = NoRetry;

    fn describe_copies(&self, _meta: &mut HandlerMetadata) {}

    fn pair(
        self,
        _connected: &Connected<B>,
    ) -> impl Future<Output = Result<Option<NoRetry>, PairError>> {
        ready(Ok(None))
    }
}

/// The bound retry position: the policy, the mount site's transforms and the app's publish
/// pipeline. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct RetryParts<Policy, Stack, Pipe> {
    policy: Policy,
    stack: Stack,
    pipeline: Pipe,
}

impl<Policy, Stack, Pipe> RetryParts<Policy, Stack, Pipe> {
    /// The position bound with `policy`, the transforms on it and the app's `pipeline`.
    pub(crate) const fn new(policy: Policy, stack: Stack, pipeline: Pipe) -> Self {
        Self {
            policy,
            stack,
            pipeline,
        }
    }
}

impl<B, Cx, Policy, Stack, Pipe> BoundRetry<B, Cx> for RetryParts<Policy, Stack, Pipe>
where
    B: Broker + 'static,
    Cx: Send + Sync + 'static,
    Policy: PublishPolicy<Connected<B>> + Send + 'static,
    Policy::Live: Publisher + Send + Sync + 'static,
    Stack: PublishTransform<ForReply<Cx>, <Policy::Live as Publisher>::Options>
        + Send
        + Sync
        + 'static,
    Pipe: OutPipeline<Policy::Live> + Send + 'static,
{
    type Leaf = RetryLeaf<Policy::Live, Stack, Pipe>;

    fn describe_copies(&self, meta: &mut HandlerMetadata) {
        // A retry copy carries the delivery's own bytes and goes where the registration
        // declared, so the position names neither a media type nor a destination of its own.
        meta.describe_copies(|channel| {
            PublishDescription::of::<Connected<B>, Policy>(&self.policy, None, false, channel)
        });
    }

    async fn pair(
        self,
        connected: &Connected<B>,
    ) -> Result<Option<RetryLeaf<Policy::Live, Stack, Pipe>>, PairError> {
        let Self {
            policy,
            stack,
            pipeline,
        } = self;
        let pairing = policy.pair(connected);
        #[cfg(feature = "testing")]
        let (live, origin) = paired(connected, pairing).await;
        #[cfg(not(feature = "testing"))]
        let live = pairing.await;
        Ok(Some(RetryLeaf {
            live: live?,
            stack,
            pipeline,
            #[cfg(feature = "testing")]
            origin,
        }))
    }
}

/// What a broker scope hands each subscription it starts.
///
/// The deferred retry is a registration's own, so it is not here: this is what the whole scope
/// shares with each of its subscriptions, plus, under the `testing` feature, which subscription
/// of the app it is.
#[doc(hidden)]
pub struct ScopeDelivery {
    /// The harness's recording-and-quiescence hooks for this scope.
    #[cfg(feature = "testing")]
    hooks: Arc<TestHooks>,
    /// This broker's registration index, scoping recorded deliveries per broker.
    #[cfg(feature = "testing")]
    scope_id: usize,
    /// The subscription of the app this context was built for, identifying its records apart
    /// from those of a subscription that reports the same name.
    #[cfg(feature = "testing")]
    subscription: usize,
    /// The app's poll-time diagnostics, which every subscription registers with as it opens.
    #[cfg(feature = "poll-diagnostics")]
    poll: PollDiagnostics,
}

impl fmt::Debug for ScopeDelivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopeDelivery").finish_non_exhaustive()
    }
}

impl Default for ScopeDelivery {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeDelivery {
    /// The scope context every subscription of one broker dispatches under, outside any harness
    /// and with diagnostics of its own; [`with_harness`](Self::with_harness) and
    /// [`with_poll`](Self::with_poll) attach the app's.
    ///
    /// The harness and the diagnostics are attached by methods rather than taken here, so the
    /// signature is the same whichever features of this crate a build unifies: a runner built
    /// without them still calls it.
    #[doc(hidden)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "testing")]
            hooks: Arc::new(TestHooks::detached()),
            #[cfg(feature = "testing")]
            scope_id: 0,
            #[cfg(feature = "testing")]
            subscription: 0,
            #[cfg(feature = "poll-diagnostics")]
            poll: PollDiagnostics::new(),
        }
    }

    /// The harness hooks this scope dispatches under, with the broker's registration index and
    /// the subscription's identity.
    #[doc(hidden)]
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn with_harness(
        mut self,
        hooks: Arc<TestHooks>,
        scope_id: usize,
        subscription: usize,
    ) -> Self {
        self.hooks = hooks;
        self.scope_id = scope_id;
        self.subscription = subscription;
        self
    }

    /// The app's poll-time diagnostics, which this scope's subscriptions register with.
    #[doc(hidden)]
    #[cfg(feature = "poll-diagnostics")]
    #[must_use]
    pub fn with_poll(mut self, poll: PollDiagnostics) -> Self {
        self.poll = poll;
        self
    }

    /// The app's poll-time diagnostics.
    #[cfg(feature = "poll-diagnostics")]
    pub(crate) const fn poll(&self) -> &PollDiagnostics {
        &self.poll
    }

    /// The harness hooks this scope dispatches under.
    #[cfg(feature = "testing")]
    pub(crate) fn hooks(&self) -> &Arc<TestHooks> {
        &self.hooks
    }

    /// This broker's registration index.
    #[cfg(feature = "testing")]
    pub(crate) fn scope_id(&self) -> usize {
        self.scope_id
    }

    /// The subscription of the app this context was built for.
    #[cfg(feature = "testing")]
    pub(crate) fn subscription(&self) -> usize {
        self.subscription
    }
}

/// A registration whose descriptor addresses no retry copies, and whose mount site named no
/// destination either.
///
/// Raised at startup, before the subscription opens, and only for that registration. A mount chain
/// that bound the retry position and named no destination does not compile; this is the
/// registration that bound nothing, whose broker's default publisher still has no address to
/// send its copies to.
// The field is named away from `source` so `thiserror` does not read it as the error's cause.
#[derive(Debug, Error)]
#[error(
    "subscription `{subscription}`: descriptor `{source_type}` declares `Copies = NamedCopies`, \
     so it addresses no retry copies, and this registration names no destination for them. Name \
     one at the mount site with `.out_retry(policy).to(\"name\")`, or compose a publish transform \
     declaring `Destination = Names` that names one per delivery"
)]
pub(crate) struct RetryDestinationError {
    /// The subscription as the registration names it.
    subscription: String,
    /// The subscription descriptor that addresses nothing.
    source_type: &'static str,
}

/// The broker refused a declaration a registration mounted by a bare name made.
///
/// Raised at startup, before the subscription opens. A name is a string: nothing at the mount
/// site says whether this broker maps a cap and a dead-letter destination onto the subscription
/// it opens under that name, so the answer comes from the broker at resolve time.
#[derive(Debug, Error)]
#[error("subscription `{subscription}`: {source}")]
pub(crate) struct RetryDeclareError {
    /// The subscription as the registration names it.
    subscription: String,
    #[source]
    source: DeclareRetryError,
}

/// The policy bound with `.out(Retry, policy)` could not be paired with the connected broker.
///
/// Raised at startup, before the subscription opens, the way a reply policy that fails to pair is.
#[derive(Debug, Error)]
#[error(
    "subscription `{subscription}`: the deferred-retry policy bound with `out_retry` failed to \
     pair: {source}"
)]
pub(crate) struct RetryPairError {
    /// The subscription as the registration names it.
    subscription: String,
    #[source]
    source: PairError,
}

/// Opens `source`'s subscription and builds the delivery context it dispatches under.
///
/// The retry publisher is paired and the descriptor's address resolved first, against the live
/// connection, because both may have to talk to the broker (a Pub/Sub subscription is looked up
/// to learn its topic) and because a registration that cannot pair must fail before it holds an
/// open subscription. Where the mount site named a destination with `.to(name)` that is what the
/// copies use, and a descriptor that addresses its own is asked only otherwise. The declaration
/// reaches the descriptor before it subscribes, so a broker that applies the cap and the
/// destination itself declares its topology with them.
///
/// # Errors
///
/// Returns the broker's error when the address lookup or the subscription fails, and
/// [`RetryPairError`] when the bound retry policy fails to pair.
pub(crate) async fn open_subscription<B, Source, Cx, Pairing>(
    source: Source,
    connected: &Connected<B>,
    scope: &ScopeDelivery,
    subscription: &str,
    setup: RetrySetup<B, Cx, Pairing>,
) -> Result<OpenedSubscription<Source::Subscriber, Cx, Pairing::Leaf>, BoxError>
where
    B: Broker + 'static,
    Cx: Send + Sync + 'static,
    Pairing: BoundRetry<B, Cx>,
    Source: SubscriptionSource<Connected<B>>,
    Source::Copies: CopyPathAddress<Connected<B>, Source>,
{
    let RetrySetup {
        publisher,
        destination,
        named_per_delivery,
        declaration,
        ..
    } = setup;
    // A descriptor takes the declaration into itself below; a bare name has nothing to take it
    // into, so the broker answers for it here, before anything subscribes.
    source
        .declare_retry_on(connected, &declaration)
        .map_err(|err| {
            BoxError::from(RetryDeclareError {
                subscription: subscription.to_owned(),
                source: err,
            })
        })?;
    let paired = publisher.pair(connected).await.map_err(|err| {
        BoxError::from(RetryPairError {
            subscription: subscription.to_owned(),
            source: err,
        })
    })?;
    let retry = match paired {
        Some(publisher) => {
            let destination = match destination {
                Some(named) => Some(Arc::from(named.as_ref())),
                None => <Source::Copies as CopyPathAddress<Connected<B>, Source>>::address(
                    &source, connected,
                )
                .await
                .map_err(BoxError::from)?
                .map(|address| Arc::from(address.as_str())),
            };
            // A descriptor that addresses its own copies answers here; one that does not leaves
            // the mount site to name them, statically or per delivery. Nothing left is a
            // registration whose copies would go nowhere: the chain refuses one that bound the
            // retry position at compile time, and one that bound nothing lands here.
            if destination.is_none() && !named_per_delivery {
                return Err(Box::new(RetryDestinationError {
                    subscription: subscription.to_owned(),
                    source_type: type_name::<Source>(),
                }));
            }
            Some(RetryRoute {
                publisher,
                destination,
                subscription: Arc::from(subscription),
            })
        }
        None => None,
    };
    announce(subscription, &declaration, retry.is_some());
    let subscriber = source
        .declare_retry(&declaration)
        .subscribe(connected)
        .await
        .map_err(BoxError::from)?;
    Ok((
        subscriber,
        Delivery::for_subscription(scope, subscription, declaration),
        retry,
    ))
}

/// What opening a registration's subscription yields: the subscriber, the context its deliveries
/// dispatch under and the retry route its copies leave through. Machinery; never named directly.
#[doc(hidden)]
pub type OpenedSubscription<Sub, Cx, Retry> = (Sub, Delivery<Cx>, Option<RetryRoute<Retry>>);

/// Says once, at startup, what a registration declared and who applies it, so an operator reading
/// the log knows whether the cap is this process's business or the broker's.
fn announce(subscription: &str, declaration: &RetryDeclaration, publishes_copies: bool) {
    if declaration.declares_nothing() {
        return;
    }
    let max_attempts = declaration.max_attempts().map(NonZeroU32::get);
    if publishes_copies {
        info!(
            target: "ruststream::retry",
            subscription = %subscription,
            max_attempts,
            dead_letter = declaration.dead_letter(),
            applied_by = "runtime",
            "retry declaration; a cap applies to the copies this process publishes, and to a \
             native delayed redelivery through the broker's delivery count where the transport \
             keeps one and the framework's retry-count header otherwise, never both",
        );
    } else {
        info!(
            target: "ruststream::retry",
            subscription = %subscription,
            max_attempts,
            dead_letter = declaration.dead_letter(),
            applied_by = "broker",
            "retry declaration; the broker moves a spent delivery itself and applies it",
        );
    }
}

/// Opens `source`'s subscription for a route the one-task fold opens under its broker's `scope`,
/// and builds the delivery context it dispatches under; returns the name the descriptor reports
/// beside the two.
///
/// The registration's `declaration` reaches the descriptor before it subscribes, as on the
/// multi-thread runner, so a broker that applies the cap and the destination itself declares its
/// topology with them; the delivery context carries it for the copies this process publishes.
/// Under a harness the subscription is noted as one more of that broker, so its deliveries are
/// recorded under the broker and the name it subscribed with.
///
/// # Errors
///
/// The broker's refusal of the declaration or of the subscription.
// `Send` exactly where the local descriptor's subscription is: the one-task fold never sends it.
#[allow(clippy::future_not_send)]
pub(crate) async fn open_local_subscription<B, Source, Cx>(
    source: Source,
    connected: &Connected<B>,
    scope: &ScopeDelivery,
    declaration: RetryDeclaration,
) -> Result<(Arc<str>, Source::Subscriber, Delivery<Cx>), BoxError>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
{
    let name: Arc<str> = Arc::from(source.name());
    source
        .declare_retry_on(connected, &declaration)
        .map_err(|err| {
            BoxError::from(RetryDeclareError {
                subscription: name.as_ref().to_owned(),
                source: err,
            })
        })?;
    let source = source.declare_retry(&declaration);
    #[cfg(feature = "testing")]
    let routed = ScopeDelivery {
        hooks: Arc::clone(&scope.hooks),
        scope_id: scope.scope_id,
        subscription: scope.hooks.subscribed(scope.scope_id, &name),
        #[cfg(feature = "poll-diagnostics")]
        poll: scope.poll.clone(),
    };
    #[cfg(feature = "testing")]
    let scope = &routed;
    let delivery = Delivery::for_subscription(scope, &name, declaration);
    let subscriber = source
        .subscribe(connected)
        .await
        .map_err(|err| -> BoxError { Box::new(err) })?;
    Ok((name, subscriber, delivery))
}

/// The delivery context for a subscriber mounted without a source: nothing describes where a
/// redelivery of it would be published, and no mount chain can bind the retry position on one, so
/// there is no retry path to build.
#[doc(hidden)]
#[must_use]
pub fn open_mounted_subscriber<Cx>(scope: &ScopeDelivery, subscription: &str) -> Delivery<Cx> {
    Delivery::for_subscription(scope, subscription, RetryDeclaration::new())
}
