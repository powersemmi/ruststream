//! What one registration owes its retries at startup: the publisher a copy of a delivery leaves
//! through, where such a copy goes, and the declaration the mount site made.
//!
//! A copy needs a destination, and a subscription's own name is not one: where a subscription and
//! a publish destination are separate resources (a Pub/Sub subscription and its topic), a copy
//! published under the subscription's name reaches nothing. Which of the two a descriptor is, and
//! whether it can name the destination at all, is its own type
//! ([`SubscriptionSource::Copies`](crate::SubscriptionSource::Copies)), so the mount site is told
//! at compile time and nothing here refuses to start over an address.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::future::{Future, ready};
use std::num::NonZeroU32;
use std::sync::Arc;

use thiserror::Error;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::runtime::dispatch::Delivery;
use crate::runtime::lifecycle::{BoxError, BoxFuture};
use crate::runtime::metadata::{HandlerMetadata, PublishDescription};
use crate::runtime::publish::{
    ForReply, OutPipeline, PublishContext, PublishTransform, PublishTransformIdentity,
};
use crate::{
    AddressedCopies, Broker, BrokerMoves, Connected, ConnectedBroker, DeclareRetryError,
    DefaultPublish, NamedCopies, OutgoingMessage, PairError, PayloadForm, PublishPolicy, Publisher,
    RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, SubscriptionSource,
};

#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;

/// The plain publish policy of a broker's connected form, which is what a retry copy leaves
/// through until a mount site names another.
type DefaultPolicy<B> = <Connected<B> as DefaultPublish>::Policy;

/// The live publisher that policy pairs into.
type DefaultLive<B> = <DefaultPolicy<B> as PublishPolicy<Connected<B>>>::Live;

/// The retry path of one subscription: the publisher a copy of a delivery leaves through, and
/// where a copy meant for the subscription itself goes.
///
/// The destination is `None` where the mount site named none and the descriptor has none of its
/// own, which is a [`NamedCopies`] registration whose transforms name it per delivery.
pub(crate) struct DeferredRetry<Cx> {
    /// The registration's retry slot, erased against the delivery context its transforms read. A
    /// publish through it travels the mount site's transforms and the app's publish pipeline.
    pub(crate) publisher: Arc<dyn ErasedRetryPublisher<Cx>>,
    pub(crate) destination: Option<Arc<str>>,
}

/// The retry publisher of one registration, erased against everything but the delivery context
/// its transforms read.
///
/// The copy carries the delivery's own bytes, so nothing here encodes; what the erasure has to
/// keep is the context, because a transform on this position reads the delivery being retried.
pub(crate) trait ErasedRetryPublisher<Cx>: Send + Sync {
    /// Sends one copy, with the delivery it is a copy of in hand.
    fn publish_copy<'a>(
        &'a self,
        msg: OutgoingMessage<'a>,
        cx: &'a PublishContext<'a, Cx>,
    ) -> BoxFuture<'a, Result<(), BoxError>>;
}

/// The live retry publisher: the leaf the policy paired into, the mount site's transform stack,
/// and the app's publish pipeline underneath it.
struct RetryLeaf<Live, Stack, Pipeline> {
    live: Live,
    stack: Stack,
    pipeline: Pipeline,
}

impl<Cx, Live, Stack, Pipeline> ErasedRetryPublisher<Cx> for RetryLeaf<Live, Stack, Pipeline>
where
    Cx: Send + Sync + 'static,
    Live: Publisher + Send + Sync + 'static,
    Stack: PublishTransform<ForReply<Cx>, Live::Options> + Send + Sync + 'static,
    Pipeline: OutPipeline<Live> + 'static,
{
    fn publish_copy<'a>(
        &'a self,
        msg: OutgoingMessage<'a>,
        cx: &'a PublishContext<'a, Cx>,
    ) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
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
            // The copy is dead once the leaf has it, so what the transforms wrote moves on rather
            // than being copied on.
            let sent = <Live::Payload as PayloadForm>::leaving(&mut out);
            self.pipeline
                .send(&self.live, sent, options.as_ref())
                .await
                .map_err(|err| Box::new(err) as BoxError)
        })
    }
}

/// The retry publisher of a registration that named neither a transform nor a middleware: what a
/// bare mount produces, for tests that drive the dispatch functions directly.
#[cfg(test)]
pub(crate) fn bare_retry_publisher<Cx, P>(live: P) -> Arc<dyn ErasedRetryPublisher<Cx>>
where
    Cx: Send + Sync + 'static,
    P: Publisher + Send + Sync + 'static,
{
    Arc::new(RetryLeaf {
        live,
        stack: PublishTransformIdentity,
        pipeline: crate::runtime::publish::PublishIdentity,
    })
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
    /// The pairing, or `None` where this process publishes nothing for the subscription.
    fn pairing(pipeline: Pipeline) -> Option<RetryPairing<B, Cx>>;
}

/// Implements the default pairing for the copy paths this process publishes on.
macro_rules! impl_default_pairing {
    ($($path:ident),+ $(,)?) => {$(
        impl<B, Cx, Pipeline> CopyPathPairing<B, Cx, Pipeline> for $path
        where
            B: Broker + 'static,
            Cx: Send + Sync + 'static,
            Connected<B>: DefaultPublish,
            DefaultLive<B>: Publisher + Send + Sync + 'static,
            Pipeline: OutPipeline<DefaultLive<B>> + 'static,
        {
            fn pairing(pipeline: Pipeline) -> Option<RetryPairing<B, Cx>> {
                Some(RetryPairing::new(
                    DefaultPolicy::<B>::default(),
                    PublishTransformIdentity,
                    pipeline,
                ))
            }
        }
    )+};
}

impl_default_pairing!(AddressedCopies, NamedCopies);

impl<B: Broker, Cx, Pipeline> CopyPathPairing<B, Cx, Pipeline> for BrokerMoves {
    fn pairing(_pipeline: Pipeline) -> Option<RetryPairing<B, Cx>> {
        None
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
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> + Send;
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
            ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> + Send {
                ready(Ok(None))
            }
        }
    )+};
}

impl_no_address!(NamedCopies, BrokerMoves);

/// What one registration hands the runtime about its retries: the publisher a copy leaves
/// through, where the copies go, and what the mount site declared.
///
/// The publisher starts empty and is filled from one of two places - the policy a
/// `.out_retry(policy)` named, or the descriptor's copy path, which pairs the broker's default -
/// so a registration whose subscription publishes its copies here always has one.
#[doc(hidden)]
pub struct RetrySetup<B: Broker, Cx> {
    publisher: Option<RetryPairing<B, Cx>>,
    destination: Option<Cow<'static, str>>,
    /// Whether the registration's transforms name the destination per delivery, so the
    /// subscription owes none of its own.
    named_per_delivery: bool,
    declaration: RetryDeclaration,
}

// The publisher is a closure with nothing to print; the declaration is the part worth reading.
impl<B: Broker, Cx> fmt::Debug for RetrySetup<B, Cx> {
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
            publisher: None,
            destination: None,
            named_per_delivery: false,
            declaration: RetryDeclaration::new(),
        }
    }
}

impl<B: Broker + 'static, Cx> RetrySetup<B, Cx> {
    /// Records the publisher a `.out(Retry, policy)` named, replacing the broker's default, and
    /// the destination the `.to(name)` after it gave the copies.
    pub(crate) fn with_publisher(
        mut self,
        publisher: RetryPairing<B, Cx>,
        destination: Option<Cow<'static, str>>,
        named_per_delivery: bool,
    ) -> Self {
        self.publisher = Some(publisher);
        self.named_per_delivery = named_per_delivery;
        // A `.to(name)` ahead of the publisher already spoke; one after it names the same thing,
        // and the chain admits only one of the two.
        self.destination = destination;
        self
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
        mut self,
        pipeline: &Pipeline,
        meta: &mut HandlerMetadata,
    ) -> Self
    where
        Copies: CopyPathPairing<B, Cx, Pipeline>,
        Pipeline: Clone,
    {
        if self.publisher.is_none() {
            self.publisher = Copies::pairing(pipeline.clone());
        }
        if let Some(publisher) = &self.publisher {
            meta.describe_copies(|channel| publisher.describe(channel));
        }
        self
    }
}

/// The pairing one registration's `.out(Retry, policy)` owes: the retry slot, erased against the
/// broker the mount site named, waiting for the connected form.
///
/// A policy is declaration and pairs only against a connected broker, which exists after the
/// synchronous builder has run. Erasing the whole slot here - the policy, the codec and the
/// pipeline the mount composed - is what keeps the retry position out of the route's type: a
/// route carries this one type whatever the mount site named.
#[doc(hidden)]
pub struct RetryPairing<B: Broker, Cx>(Box<dyn BoundRetry<B, Cx>>);

// The pairing is the bound policy erased, with nothing of its own to print.
impl<B: Broker, Cx> fmt::Debug for RetryPairing<B, Cx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPairing").finish_non_exhaustive()
    }
}

/// What one retry position bound, erased: the policy, the mount site's transforms and the app's
/// publish pipeline.
///
/// The policy itself is erased rather than a closure over it, because the two things asked of it
/// come at different times. The document asks what the policy says about a channel once the
/// registration has named it - a `dead_letter(..)` destination, or the one a `.to(name)` gave the
/// copies - which is after the position is bound; pairing consumes the policy later still, at
/// startup.
trait BoundRetry<B: Broker, Cx>: Send {
    /// What the bound policy says about a channel a copy of the delivery leaves for.
    fn describe(&self, channel: &str) -> PublishDescription;

    /// Pairs it against the connected broker, producing the publisher a copy leaves through.
    fn pair(
        self: Box<Self>,
        connected: &Connected<B>,
    ) -> BoxFuture<'_, Result<Arc<dyn ErasedRetryPublisher<Cx>>, PairError>>;
}

/// The bound retry position in its typed form, before erasure.
struct RetryParts<Policy, Stack, Pipe> {
    policy: Policy,
    stack: Stack,
    pipeline: Pipe,
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
    fn describe(&self, channel: &str) -> PublishDescription {
        // A retry copy carries the delivery's own bytes and goes where the registration
        // declared, so the position names neither a media type nor a destination of its own.
        PublishDescription::of::<Connected<B>, Policy>(&self.policy, None, false, channel)
    }

    fn pair(
        self: Box<Self>,
        connected: &Connected<B>,
    ) -> BoxFuture<'_, Result<Arc<dyn ErasedRetryPublisher<Cx>>, PairError>> {
        Box::pin(async move {
            let Self {
                policy,
                stack,
                pipeline,
            } = *self;
            let live = policy.pair(connected).await?;
            Ok(Arc::new(RetryLeaf {
                live,
                stack,
                pipeline,
            }) as Arc<dyn ErasedRetryPublisher<Cx>>)
        })
    }
}

// Asked from the registration's commit, where the delivery context carries no bounds yet, so it
// stands apart from the pairing block below.
impl<B: Broker, Cx> RetryPairing<B, Cx> {
    /// What the bound policy says about a channel this registration declared for its copies.
    ///
    /// Asked once the registration has named the channel, which is what a binding that carries
    /// the destination's own name is filled from.
    pub(crate) fn describe(&self, channel: &str) -> PublishDescription {
        self.0.describe(channel)
    }
}

impl<B: Broker + 'static, Cx: Send + Sync + 'static> RetryPairing<B, Cx> {
    /// The pairing the retry position owes against this broker's connected form.
    ///
    /// What the pairing yields is the live publisher with the mount site's transforms and the
    /// app's publish pipeline around it, erased against everything but the delivery context those
    /// transforms read.
    pub(crate) fn new<Policy, Stack, Pipe>(policy: Policy, stack: Stack, pipeline: Pipe) -> Self
    where
        Policy: PublishPolicy<Connected<B>> + Send + 'static,
        Policy::Live: Publisher + Send + Sync + 'static,
        Stack: PublishTransform<ForReply<Cx>, <Policy::Live as Publisher>::Options>
            + Send
            + Sync
            + 'static,
        Pipe: OutPipeline<Policy::Live> + Send + 'static,
    {
        Self(Box::new(RetryParts {
            policy,
            stack,
            pipeline,
        }))
    }

    /// Takes the pairing, producing the publisher a copy leaves through.
    ///
    /// # Errors
    ///
    /// Returns the policy's own [`PairError`].
    async fn pair(
        self,
        connected: &Connected<B>,
    ) -> Result<Arc<dyn ErasedRetryPublisher<Cx>>, PairError> {
        self.0.pair(connected).await
    }
}

/// What a broker scope hands every subscription it starts.
///
/// The deferred retry is a registration's own, so it is not here: this is what the whole scope
/// shares with each of its subscriptions.
pub(crate) struct ScopeDelivery {
    /// App-wide tracker for post-settle continuations, so a graceful shutdown drains them.
    tasks: TaskTracker,
    /// The harness's recording-and-quiescence hooks for this scope.
    #[cfg(feature = "testing")]
    hooks: Arc<TestHooks>,
    /// This broker's registration index, scoping recorded deliveries per broker.
    #[cfg(feature = "testing")]
    scope_id: usize,
}

impl ScopeDelivery {
    /// The scope context every subscription of one broker dispatches under.
    pub(crate) fn new(
        tasks: TaskTracker,
        #[cfg(feature = "testing")] hooks: Arc<TestHooks>,
        #[cfg(feature = "testing")] scope_id: usize,
    ) -> Self {
        Self {
            tasks,
            #[cfg(feature = "testing")]
            hooks,
            #[cfg(feature = "testing")]
            scope_id,
        }
    }

    /// The tracker post-settle continuations are spawned onto.
    pub(crate) fn tasks(&self) -> &TaskTracker {
        &self.tasks
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
}

/// A registration whose descriptor addresses no retry copies, and whose mount site named no
/// destination either.
///
/// Raised at startup, before the subscription opens, and only for that registration. The mount
/// chain refuses the same mistake at compile time wherever it can - a router chain, a
/// slot-carrying form - but a scope's guard commits when the statement ends, so the chain there
/// has no uncommittable state to fail on and this is where the refusal lands.
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
pub(crate) async fn open_subscription<B, Source, Cx>(
    source: Source,
    connected: &Connected<B>,
    scope: &ScopeDelivery,
    subscription: &str,
    setup: RetrySetup<B, Cx>,
) -> Result<(Source::Subscriber, Arc<Delivery<Cx>>), BoxError>
where
    B: Broker + 'static,
    Cx: Send + Sync + 'static,
    Source: SubscriptionSource<Connected<B>>,
    Source::Copies: CopyPathAddress<Connected<B>, Source>,
{
    let RetrySetup {
        publisher,
        destination,
        named_per_delivery,
        declaration,
    } = setup;
    // A descriptor takes the declaration into itself below; a bare name has nothing to take it
    // into, so the broker answers for it here, before anything subscribes.
    source
        .declare_retry_on(connected, &declaration)
        .map_err(|err| {
            Box::new(RetryDeclareError {
                subscription: subscription.to_owned(),
                source: err,
            }) as BoxError
        })?;
    let retry = match publisher {
        Some(pairing) => {
            let publisher = pairing.pair(connected).await.map_err(|err| {
                Box::new(RetryPairError {
                    subscription: subscription.to_owned(),
                    source: err,
                }) as BoxError
            })?;
            let destination = match destination {
                Some(named) => Some(Arc::from(named.as_ref())),
                None => <Source::Copies as CopyPathAddress<Connected<B>, Source>>::address(
                    &source, connected,
                )
                .await
                .map_err(|err| Box::new(err) as BoxError)?
                .map(|address| Arc::from(address.as_str())),
            };
            // A descriptor that addresses its own copies answers here; one that does not leaves
            // the mount site to name them, statically or per delivery. Nothing left is a
            // registration whose copies would go nowhere. A `Router` chain refuses that at
            // `.build()`; a scope's guard cannot, because it commits when the statement ends and
            // `.to(name)` is written after the publisher, so the refusal lands here.
            if destination.is_none() && !named_per_delivery {
                return Err(Box::new(RetryDestinationError {
                    subscription: subscription.to_owned(),
                    source_type: type_name::<Source>(),
                }));
            }
            Some(DeferredRetry {
                publisher,
                destination,
            })
        }
        None => None,
    };
    announce(subscription, &declaration, retry.as_ref());
    let subscriber = source
        .declare_retry(&declaration)
        .subscribe(connected)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    Ok((
        subscriber,
        Arc::new(Delivery::for_subscription(scope, retry, declaration)),
    ))
}

/// Says once, at startup, what a registration declared and who applies it, so an operator reading
/// the log knows whether the cap is this process's business or the broker's.
fn announce<Cx>(
    subscription: &str,
    declaration: &RetryDeclaration,
    retry: Option<&DeferredRetry<Cx>>,
) {
    if declaration.declares_nothing() {
        return;
    }
    let max_attempts = declaration.max_attempts().map(NonZeroU32::get);
    if retry.is_some() {
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

/// The delivery context for a subscriber mounted without a source: nothing describes where a
/// redelivery of it would be published, and no mount chain can bind the retry position on one, so
/// there is no retry path to build.
pub(crate) fn open_mounted_subscriber<Cx>(scope: &ScopeDelivery) -> Arc<Delivery<Cx>> {
    Arc::new(Delivery::for_subscription(
        scope,
        None,
        RetryDeclaration::new(),
    ))
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::pin::pin;

    use futures::StreamExt as _;

    use super::*;
    use crate::memory::{MemoryBroker, MemoryPublish};
    use crate::runtime::publish::PublishIdentity;
    use crate::{HeaderMap, IncomingMessage, OutgoingMessage, Subscriber};

    /// The retry position of a mount that named no transform: the app's bare pipeline, which is
    /// what the wire produces there.
    fn bare_position() -> RetryPairing<MemoryBroker, ()> {
        RetryPairing::<MemoryBroker, ()>::new(
            MemoryPublish,
            PublishTransformIdentity,
            PublishIdentity,
        )
    }

    /// A scope with the harness pieces it holds under the `testing` feature.
    fn scope() -> ScopeDelivery {
        ScopeDelivery::new(
            TaskTracker::new(),
            #[cfg(feature = "testing")]
            Arc::new(TestHooks::detached()),
            #[cfg(feature = "testing")]
            0,
        )
    }

    /// A subscriber mounted without a source describes no subscription, so nothing reports where
    /// a copy of one of its deliveries would go: the mount carries no retry path at all, and the
    /// chain offers no way to bind one.
    #[test]
    fn a_sourceless_mount_carries_no_retry_path() {
        let delivery = open_mounted_subscriber::<()>(&scope());
        assert!(delivery.retry.is_none());
        assert!(delivery.declaration.declares_nothing());
    }

    /// The bound position pairs against the connected broker, and the publisher it yields reaches
    /// that broker: what a copy travels through.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_bound_position_pairs_against_the_connected_broker() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("retry.fallback");
        let connected = broker.connect().await.expect("connect");

        let publisher = bare_position()
            .pair(&connected)
            .await
            .expect("the bound position pairs");
        let headers = HeaderMap::new();
        let cx = ();
        publisher
            .publish_copy(
                OutgoingMessage::new("retry.fallback", b"deferred"),
                &PublishContext::new("retry.fallback", &headers, &cx),
            )
            .await
            .expect("the erased publish failed");

        let mut stream = pin!(subscriber.stream());
        let msg = stream
            .next()
            .await
            .expect("the publish must reach the broker")
            .expect("delivery");
        assert_eq!(msg.payload(), b"deferred");
    }
}
