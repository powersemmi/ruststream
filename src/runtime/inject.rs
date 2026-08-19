//! Startup injections: handler parameters resolved once per subscription at startup.
//!
//! `#[subscriber("in")] async fn f(msg: &T, Out(out): Out<impl Publisher>, Seek(seeker):
//! Seek<K>)` declares parameters the runtime prepares before the first delivery: an injected
//! publisher pairs against the connected broker from the source attached at the include site
//! (`b.include(f).publisher(..)`, or `.out(marker, ..)` per named slot), an injected seeker is
//! minted off the subscription's own subscriber. Every such parameter implements
//! [`FromStartup`], the definition carries them as one tuple ([`InjectDef::Injections`])
//! resolved element-by-element against a matching extra tuple, and a single handler adapter
//! serves every combination - fully monomorphized, nothing to check on the hot path. Adding a
//! new injected parameter kind is one `FromStartup` impl, not a new definition form.

use std::fmt;
use std::future::Future;

use tracing::warn;

use crate::codec::Codec;
use crate::{Broker, Connected, IncomingMessage, PairError, PublishPolicy, Seekable};

use super::context::Context;
use super::dispatch::Workers;
use super::failure::{FailurePolicies, FailurePolicy};
use super::handler::{Handler, HandlerResult, Settle};
use super::input::{DecodeWith, InputKind};
use super::metadata::{HandlerMetadata, OutgoingMessageMetadata};
use super::slot::{DefaultSlot, OutSlot, SlotPublisher, TypedSlot};

/// The marker a handler signature uses to receive an injected publisher:
/// `Out(out): Out<impl Publisher>` binds `out` to a live publisher inside the body.
///
/// The publisher type is not named in the signature: the handler states the capability it needs
/// (`impl Publisher`, `impl OwnedTransactions`, ...) and the concrete type is inferred from the
/// policy attached at the include site (`b.include(f).publisher(..)`), so the same handler
/// mounts on a production broker and on its in-process test transport unchanged. A handler
/// taking several publishers names a slot marker per parameter
/// (`Out<impl Publisher, MySlot>`, see [`OutSlot`](super::OutSlot)) and the include site binds
/// each with `.out(marker, policy)`, in any order.
///
/// An optional third position declares the message set the handler publishes, which the publish
/// builder ([`message`](super::TypedSlot::message), destinations from each type's
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
/// The value is paired by the runtime at startup, so it is live by construction; handlers never
/// see a "not connected" state. See the module docs.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::runtime::{HandlerResult, Out};
/// use ruststream::{OutgoingMessage, Publisher, subscriber};
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber("ingress")]
/// async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerResult {
///     let payload = event.id.to_be_bytes();
///     if out
///         .publish(OutgoingMessage::new("out", payload.as_slice()))
///         .await
///         .is_err()
///     {
///         return HandlerResult::retry();
///     }
///     HandlerResult::Ack
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct Out<P, M = DefaultSlot, Body = (), EncodeCodec = ()>(
    /// The live slot: the paired publisher plus the scope codec, pinned to the declared
    /// message set.
    pub TypedSlot<P, Body, M, EncodeCodec>,
);

/// The marker a handler signature uses to receive its subscription's seeker:
/// `Seek(seeker): Seek<K>` binds `seeker` to `&K` inside the body.
///
/// The value is minted by the runtime off the subscription's own subscriber right after it
/// opens, so it is live by construction; handlers never see a "not opened" state. On a broker
/// without the [`Seekable`] capability the mount fails to compile.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::{MemoryPosition, MemorySeeker, MemorySource};
/// use ruststream::runtime::{HandlerResult, Seek};
/// use ruststream::{Seeker, subscriber};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64, poisoned_until: Option<usize> }
///
/// /// Skips forward past a region the producer marked poisoned.
/// #[subscriber(MemorySource::new("jobs"))]
/// async fn work(job: &Job, Seek(seeker): Seek<MemorySeeker>) -> HandlerResult {
///     if let Some(resume_at) = job.poisoned_until {
///         if seeker.seek(MemoryPosition::sequence(resume_at)).await.is_err() {
///             return HandlerResult::retry();
///         }
///     }
///     HandlerResult::Ack
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct Seek<K>(pub K);

/// A handler parameter resolved once per subscription at startup.
///
/// `Extra` is the include-site attachment for this element: a publish policy for [`Out`], any
/// placeholder for parameters that need nothing (a [`Seek`] resolves off the subscription
/// itself). The injection tuple resolves element-by-element against a matching extra tuple, so
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

/// The injected publisher: pairs the slot's attached policy against the connected broker and
/// wraps it with the slot identity, the include site's scope codec (what
/// [`message`](super::TypedSlot::message) encodes with), and the declared message
/// set. A failing pair surfaces at startup with the slot's name (an unbound slot never gets
/// this far: the include site does not compile without a policy per slot).
impl<B, Sub, Policy, EncodeCodec, Body, M> FromStartup<B, Sub, (Policy, EncodeCodec)>
    for Out<SlotPublisher<Policy::Live, M>, M, Body, EncodeCodec>
where
    B: Broker,
    Sub: Sync,
    Policy: PublishPolicy<Connected<B>> + Send,
    EncodeCodec: Send,
    M: OutSlot,
{
    async fn resolve(
        (policy, codec): (Policy, EncodeCodec),
        connected: &Connected<B>,
        _subscriber: &Sub,
    ) -> Result<Self, PairError> {
        let live = policy.pair(connected).await.map_err(|err| {
            PairError::from_boxed(Box::from(format!(
                "pairing the publisher for Out slot `{}` failed: {err}",
                M::NAME,
            )))
        })?;
        Ok(Self(TypedSlot::new(SlotPublisher::new(live), codec)))
    }
}

/// The injected seeker: minted off the subscription's own subscriber.
impl<B, Sub, Extra> FromStartup<B, Sub, Extra> for Seek<Sub::Seeker>
where
    B: Broker,
    Sub: Seekable + Sync,
    Extra: Send,
{
    async fn resolve(
        _extra: Extra,
        _connected: &Connected<B>,
        subscriber: &Sub,
    ) -> Result<Self, PairError> {
        Ok(Self(subscriber.seeker()))
    }
}

/// A definition with no injected parameters still resolves: to nothing.
impl<B: Broker, Sub: Sync, Extra: Send> FromStartup<B, Sub, Extra> for () {
    async fn resolve(
        _extra: Extra,
        _connected: &Connected<B>,
        _subscriber: &Sub,
    ) -> Result<Self, PairError> {
        Ok(())
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
/// Generated by `#[subscriber(..)]` when the signature carries [`Out`] / [`Seek`] parameters;
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

    /// The tuple of startup-injected parameters ([`Out`], [`Seek`], ...).
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
    /// [`FromHeaders<T>`](super::FromHeaders) parameter), when available.
    fn headers_schema(&self) -> Option<String> {
        None
    }

    /// The messages this handler publishes, for the `AsyncAPI` `send` operations: every `Out`
    /// slot dictionary entry. The macro fills this in; the default declares nothing. Called
    /// once at registration.
    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }

    /// The input type's [`Message`](crate::Message) name, when it implements that trait.
    fn message_name(&self) -> Option<&'static str> {
        None
    }

    /// The input type's [`Message`](crate::Message) description, when it implements that trait.
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
    ) -> impl Future<Output = Settle> + Send;
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
    DecodeCodec: Codec,
    State: Send + Sync,
{
    async fn handle(&self, msg: &Msg, ctx: &mut Context<'_, Def::Context, State>) -> Settle {
        // The decode product lives on this stack frame and the handler borrows its view, so
        // the input path allocates nothing of its own (a raw input borrows the payload
        // straight out of the broker's buffer).
        let owned =
            match <Def::Input as DecodeWith<DecodeCodec>>::decode(&self.codec, msg.payload()) {
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
                            HandlerResult::drop().into()
                        }
                        other => other
                            .settlement()
                            .unwrap_or_else(HandlerResult::drop)
                            .into(),
                    };
                }
            };
        let view = <Def::Input as InputKind>::view(&owned, msg.payload());
        self.def.call(view, &self.injections, ctx).await
    }
}
