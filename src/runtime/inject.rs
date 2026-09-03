//! Startup injections: handler parameters resolved once per subscription at startup.
//!
//! `#[subscriber("in")] async fn f(msg: &T, Out(out): Out<impl Publisher>)` declares parameters
//! the runtime prepares before the first delivery: an injected publisher pairs against the
//! connected broker from the source attached at the include site (`b.include(f).publisher(..)`,
//! or `.out(marker, ..)` per named slot). Every such parameter implements [`FromStartup`], the
//! definition carries them as one tuple ([`InjectDef::Injections`]) resolved
//! element-by-element against a matching extra tuple, and a single handler adapter serves
//! every combination - fully monomorphized, nothing to check on the hot path.

use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;

use tracing::warn;

use crate::{Broker, Connected, IncomingMessage, PairError};

use super::context::Context;
use super::dispatch::Workers;
use super::failure::{FailurePolicies, FailurePolicy};
use super::handler::{Handler, HandlerOutcome};
use super::input::{DecodeWith, InputKind};
use super::metadata::{HandlerMetadata, OutgoingMessageMetadata};
use super::slot::DefaultSlot;

/// The marker a handler signature uses to receive an injected publisher:
/// `Out(out): Out<impl Publisher>` binds `out` to a live publisher inside the body.
///
/// The publisher type is not named in the signature: the handler states the broker capability it
/// needs (`impl Publisher`, `impl TransactionalPublisher`, `impl OwnedTransactions`,
/// `impl RequestReply`, or a broker-defined trait) and the concrete type is inferred from the
/// policy attached at the include site (`b.include(f).publisher(..)`), so the same handler mounts
/// on a production broker and on its in-process test transport unchanged. A handler taking
/// several publishers names a slot marker per parameter (`Out<impl Publisher, MySlot>`, see
/// [`OutSlot`](super::OutSlot)) and the include site binds each with `.out(marker, policy)`,
/// in any order.
///
/// An optional third position declares the message set the handler publishes, which the publish
/// builder ([`message`](super::Slot::message), destinations from each type's
/// `#[derive(Outgoing)]` declaration) is checked against:
///
/// - `Out<impl Publisher>` / `Out<impl Publisher, Events>` / `Out<impl Publisher, Events, ()>`
///   - unrestricted: any declared message the handler names;
/// - `Out<impl Publisher, Events, ChunkDone>` - one declared type (a `#[derive(Outgoing)]` type
///   declares itself);
/// - `Out<impl Publisher, Events, (ChunkDone, Progress)>` - a list of declared types;
/// - `Out<impl Publisher, Events, SendSet>` - a `#[derive(OutMessages)]` enum whose variants'
///   models are the declared set.
///
/// The value the body receives is the arena entry for the parameter's marker
/// ([`Slot`](super::Slot)), paired by the runtime at startup, so it is live by construction;
/// handlers never see a "not connected" state. The type itself is pure signature vocabulary:
/// the `#[subscriber]` expansion reads the declaration and binds the entry, and no value of
/// this type is ever constructed.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::runtime::{HandlerOutcome, Out};
/// use ruststream::{Outgoing, Publisher, subscriber};
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[derive(Outgoing, serde::Serialize)]
/// #[outgoing(name = "out")]
/// struct Forwarded {
///     id: u64,
/// }
///
/// #[subscriber("ingress")]
/// async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerOutcome {
///     if out.message(&Forwarded { id: event.id }).publish().await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct Out<P, M = DefaultSlot, Body = ()>(OutVocabulary<P, M, Body>);

/// The phantom carrying the [`Out`] marker's declared positions.
type OutVocabulary<P, M, Body> = PhantomData<fn() -> (P, M, Body)>;

/// A handler parameter resolved once per subscription at startup.
///
/// `Extra` is the include-site attachment for this element: a publish policy for [`Out`], any
/// placeholder for a parameter that needs nothing from the include site. The injection tuple
/// resolves element-by-element against a matching extra tuple, so
/// each slot pairs with its own attachment. The runtime resolves the whole
/// [`InjectDef::Injections`] tuple after the subscription opens and before the first delivery,
/// so injected values are live by construction.
pub trait FromStartup<B: Broker, Sub, Extra>: Sized {
    /// Resolves the injected value against the connected broker and the opened subscriber.
    ///
    /// Runs once per subscription, so the attachment arrives by value: a publish policy is
    /// consumed by its pairing, and no `Clone` is demanded of it.
    ///
    /// # Errors
    ///
    /// Returns [`PairError`] when the value cannot be prepared (a publish policy the broker
    /// refuses to pair); startup then fails, exactly like a failing subscription.
    fn resolve(
        extra: Extra,
        connected: &Connected<B>,
        subscriber: &Sub,
    ) -> impl Future<Output = Result<Self, PairError>> + Send;
}

/// A definition with no injected parameters still resolves: to nothing.
impl<B: Broker, Sub: Sync, Extra: Send> FromStartup<B, Sub, Extra> for () {
    fn resolve(
        _extra: Extra,
        _connected: &Connected<B>,
        _subscriber: &Sub,
    ) -> impl Future<Output = Result<Self, PairError>> {
        ready(Ok(()))
    }
}

/// Implements [`FromStartup`] for injection tuples: each element resolves in declaration
/// order, consuming its own extra (the two tuples are zipped positionally).
macro_rules! impl_from_startup_for_tuples {
    ($(($($name:ident: $extra:ident),+))+) => {$(
        impl<B, Sub, $($name, $extra),+> FromStartup<B, Sub, ($($extra,)+)> for ($($name,)+)
        where
            B: Broker,
            Sub: Sync,
            $(
                $name: FromStartup<B, Sub, $extra> + Send,
                $extra: Send,
            )+
        {
            async fn resolve(
                extra: ($($extra,)+),
                connected: &Connected<B>,
                subscriber: &Sub,
            ) -> Result<Self, PairError> {
                #[allow(non_snake_case)]
                let ($($extra,)+) = extra;
                Ok(($($name::resolve($extra, connected, subscriber).await?,)+))
            }
        }
    )+};
}

impl_from_startup_for_tuples! {
    (T1: E1)
    (T1: E1, T2: E2)
    (T1: E1, T2: E2, T3: E3)
    (T1: E1, T2: E2, T3: E3, T4: E4)
}

/// A subscriber definition whose handler takes startup-injected parameters.
///
/// Generated by `#[subscriber(..)]` when the signature carries [`Out`] parameters;
/// [`Self::Injections`] is their tuple, in declaration order.
pub trait InjectDef: Send + Sync {
    /// The input kind the handler consumes ([`Decoded<T>`](super::Decoded) for a typed `&T`
    /// parameter, [`RawBytes`](super::RawBytes) for a raw `&[u8]` one).
    type Input: InputKind;

    /// The broker's typed per-delivery context (see
    /// [`SubscriberDef::Context`](super::SubscriberDef::Context)).
    type Context;

    /// The subscription source this handler binds to.
    type Source;

    /// The tuple of startup-injected parameters ([`Out`], ...).
    type Injections;

    /// Builds the subscription source (fresh each call).
    fn source(&self) -> Self::Source;

    /// The concurrency policy for this subscriber's dispatch loop.
    fn workers(&self) -> Workers {
        Workers::sequential()
    }

    /// The failure policy for a handler panic and a decode failure.
    fn failure_policies(&self) -> FailurePolicies {
        FailurePolicies::default()
    }

    /// An optional human description (from the handler's doc comment), for `AsyncAPI`.
    fn description(&self) -> Option<&str> {
        None
    }

    /// The input type's serialized JSON Schema, when available.
    fn input_schema(&self) -> Option<String> {
        None
    }

    /// The serialized JSON Schema of the handler's typed header contract (its
    /// [`Headers<T>`](super::Headers) parameter), when available.
    fn headers_schema(&self) -> Option<String> {
        None
    }

    /// The messages this handler publishes, for the `AsyncAPI` `send` operations: every `Out`
    /// slot dictionary entry. The macro fills this in; the default declares nothing. Called
    /// once at registration.
    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }

    /// The input type's [`Message`](crate::MessageInfo) name, when it implements that trait.
    fn message_name(&self) -> Option<&'static str> {
        None
    }

    /// The input type's [`Message`](crate::MessageInfo) description, when it implements that trait.
    fn message_description(&self) -> Option<&'static str> {
        None
    }
}

/// Runs an [`InjectDef`]'s handler body over an app state of type `S` (the same state-generic
/// shape as [`Handler`](super::Handler); see
/// [`PublishingCall`](super::PublishingCall) for the rationale).
pub trait InjectCall<S>: InjectDef {
    /// Runs the handler body with the resolved injections.
    fn call(
        &self,
        input: &<Self::Input as InputKind>::Target,
        injections: &Self::Injections,
        ctx: &mut Context<'_, Self::Context, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send;
}

/// Builds the registration metadata for an injected definition mounted under `name`.
pub(crate) fn inject_metadata<D: InjectDef>(name: String, def: &D) -> HandlerMetadata {
    let mut meta = HandlerMetadata::raw(name).with_def_details(
        def.description(),
        def.input_schema(),
        def.headers_schema(),
        def.message_name(),
        def.message_description(),
    );
    meta.input_type = <D::Input as InputKind>::input_label();
    meta.deserialized = <D::Input as InputKind>::DESERIALIZED;
    meta.outgoing = def.outgoing();
    meta
}

/// The [`Handler`] built from an [`InjectDef`] once its injections resolved: decode, then run
/// the body with them.
pub struct InjectHandler<Def: InjectDef, DecodeCodec> {
    pub(crate) def: Def,
    pub(crate) codec: DecodeCodec,
    pub(crate) injections: Def::Injections,
    pub(crate) decode: FailurePolicy,
}

impl<Def: InjectDef, DecodeCodec> fmt::Debug for InjectHandler<Def, DecodeCodec> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InjectHandler").finish_non_exhaustive()
    }
}

impl<Msg, Def, DecodeCodec, State> Handler<Msg, Def::Context, State>
    for InjectHandler<Def, DecodeCodec>
where
    Msg: IncomingMessage,
    Def: InjectCall<State>,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Context: Send + Sync,
    Def::Injections: Send + Sync,
    // See the same relaxation on `PublishingHandler`: `DecodeWith` carries whatever the input
    // needs of the codec, and a raw input needs nothing.
    DecodeCodec: Send + Sync,
    State: Send + Sync,
{
    async fn handle(
        &self,
        msg: &Msg,
        ctx: &mut Context<'_, Def::Context, State>,
    ) -> HandlerOutcome {
        // The decode product lives on this stack frame and the handler borrows its view, so
        // the input path allocates nothing of its own (a raw input borrows the payload
        // straight out of the broker's buffer).
        let owned = match <Def::Input as DecodeWith<DecodeCodec>>::decode(
            &self.codec,
            msg.payload(),
            msg.headers(),
        ) {
            Ok(value) => value,
            Err(err) => {
                warn!(
                    target: "ruststream::dispatch",
                    subscription = %ctx.name(),
                    message_type = <Def::Input as InputKind>::input_label(),
                    error = %err,
                    "codec decode failed",
                );
                #[cfg(any(feature = "testing", feature = "otel"))]
                ctx.mark_decode_failed();
                return match self.decode {
                    FailurePolicy::FailFast => {
                        ctx.fail_fast(&format!("decode failed: {err}"));
                        HandlerOutcome::drop()
                    }
                    other => other
                        .settlement()
                        .map_or_else(HandlerOutcome::drop, Into::into),
                };
            }
        };
        let view = <Def::Input as InputKind>::view(&owned, msg.payload());
        self.def.call(view, &self.injections, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use super::{InjectCall, InjectDef, InjectHandler};
    use crate::Name;
    use crate::runtime::context::Context;
    use crate::runtime::dispatch::Workers;
    use crate::runtime::failure::{FailurePolicies, FailurePolicy};
    use crate::runtime::handler::HandlerOutcome;
    use crate::runtime::input::Decoded;

    /// A hand-written injected def overriding nothing optional, pinning the trait defaults that
    /// the macro always fills in.
    struct ManualInject;

    impl InjectDef for ManualInject {
        type Input = Decoded<u32>;
        type Context = ();
        type Source = Name;
        type Injections = ();

        fn source(&self) -> Name {
            Name::new("in")
        }
    }

    // Ignores the app state, so it is generic over it (mounts on any app).
    impl<S: Send + Sync> InjectCall<S> for ManualInject {
        fn call(
            &self,
            input: &u32,
            (): &(),
            _ctx: &mut Context<'_, (), S>,
        ) -> impl Future<Output = HandlerOutcome> {
            let _ = *input;
            ready(HandlerOutcome::ack())
        }
    }

    #[test]
    fn the_defaults_declare_nothing_the_macro_would_have_filled_in() {
        let def = ManualInject;
        assert_eq!(def.workers(), Workers::sequential());
        assert_eq!(def.failure_policies(), FailurePolicies::default());
        assert!(def.description().is_none());
        assert!(def.input_schema().is_none());
        assert!(def.headers_schema().is_none());
        assert!(def.outgoing().is_empty());
        assert!(def.message_name().is_none());
        assert!(def.message_description().is_none());
        assert!(format!("{:?}", def.source()).contains("in"));
    }

    #[test]
    fn the_handler_names_itself() {
        let handler = InjectHandler {
            def: ManualInject,
            codec: (),
            injections: (),
            decode: FailurePolicy::Drop,
        };
        assert!(format!("{handler:?}").contains("InjectHandler"));
    }

    /// The decode diagnostic of the injected path. It is asserted on the handler itself because
    /// the subject is the warning's content, and a field value is only evaluated while a
    /// subscriber listens.
    #[cfg(all(feature = "memory", feature = "json", feature = "logging"))]
    mod diagnostics {
        use futures::StreamExt;

        use super::ManualInject;
        use crate::codec::JsonCodec;
        use crate::memory::{MemoryBroker, MemoryMessage};
        use crate::runtime::context::Context;
        use crate::runtime::dispatch::Delivery;
        use crate::runtime::failure::FailurePolicy;
        use crate::runtime::handler::Handler;
        use crate::runtime::inject::InjectHandler;
        use crate::testkit::log_capture::{find, start};
        use crate::{HeaderMap, OutgoingMessage, Publisher, Subscriber};

        /// Publishes `payload` to `name` and pulls the delivery back off the bus.
        async fn one_delivery(broker: &MemoryBroker, name: &str, payload: &[u8]) -> MemoryMessage {
            let mut subscriber = broker.subscribe(name);
            broker
                .publisher()
                .publish(OutgoingMessage::new(name, payload))
                .await
                .expect("publish failed");
            let mut stream = std::pin::pin!(subscriber.stream());
            stream
                .next()
                .await
                .expect("delivery missing")
                .expect("memory subscriber never errors")
        }

        /// The diagnostic names the subscription and the type that was expected, so the
        /// offending producer is findable from the logs; `fail_fast` still settles the message
        /// out of the way (the teardown is what makes the failure loud).
        #[tokio::test]
        async fn a_decode_failure_names_the_subscription_and_the_expected_type() {
            let broker = MemoryBroker::new();
            let msg = one_delivery(&broker, "in", b"not json").await;
            let handler = InjectHandler {
                def: ManualInject,
                codec: JsonCodec,
                injections: (),
                decode: FailurePolicy::FailFast,
            };

            let state = ();
            let delivery = Delivery::empty();
            let headers = HeaderMap::new();
            let mut ctx = Context::new("in", &headers, &state, (), &delivery);

            let (events, guard) = start();
            let settle = handler.handle(&msg, &mut ctx).await;
            drop(guard);

            let failure = find(&events, "codec decode failed");
            assert_eq!(failure.get("subscription").map(String::as_str), Some("in"));
            assert_eq!(failure.get("message_type").map(String::as_str), Some("u32"));
            assert!(
                failure
                    .get("error")
                    .is_some_and(|err| err.contains("decode")),
                "the diagnostic must carry the codec error: {failure:?}",
            );
            assert!(settle.is_drop());
        }
    }
}
