//! Per-delivery [`Context`], generic over the broker's typed per-delivery context `C` and the
//! application's typed shared state `S`.
//!
//! A `Context` is built for each delivery and threaded (as `&mut`) through the middleware chain
//! into the handler. It carries the channel the message arrived on, a working copy of the
//! headers (middleware may enrich them), the typed shared application state ([`Context::state`]),
//! and the broker's typed per-delivery context read by key ([`Context::context`] /
//! [`Context::set`]). Both steps are lazy: the delivery is asked for its header map the first time
//! anything reads it, and the map is borrowed until the first
//! [`headers_mut`](Context::headers_mut), so a delivery whose handler and middleware never touch
//! the headers pays neither the broker's accessor nor a clone.

use std::cell::OnceCell;
use std::future::Future;
use std::pin::Pin;

use bytes::BytesMut;

use crate::{Field, FieldMut, HeaderMap, IncomingMessage};

use super::dispatch::Delivery;
use super::failure::FailurePolicy;
use super::handler::{HandlerOutcome, HandlerResult};
use super::shutdown::Shutdown;

/// Where one delivery's header map comes from.
///
/// The map is a broker accessor, and answering it is work on most transports: a shadow buffer
/// over the record's header block, a parse, and the buffer's destruction. Most deliveries never
/// read the answer, so the context holds the source rather than the map and asks it at most once.
pub(crate) trait HeaderSource {
    /// This delivery's headers as the broker reports them.
    fn headers(&self) -> &HeaderMap;
}

/// A map that is already built: the batch path, which carries no single delivery's headers, and
/// the tests that drive a context directly.
impl HeaderSource for HeaderMap {
    fn headers(&self) -> &HeaderMap {
        self
    }
}

/// A delivery's headers as a publish reads them: a map already in hand, or the source that
/// answers it, asked only when a transform reads it.
///
/// The broker's accessor is a parse and a buffer on most transports, so a reply whose transforms
/// read no header never reaches it.
#[derive(Clone, Copy)]
pub(crate) enum DeliveryHeaders<'a> {
    /// The map is already resolved: the handler read or modified it, or there is no delivery.
    Ready(&'a HeaderMap),
    /// Nothing has asked yet; the broker answers on the first read.
    Source(&'a (dyn HeaderSource + Sync)),
}

impl<'a> DeliveryHeaders<'a> {
    /// The map, asking the source where nothing has yet.
    pub(crate) fn get(self) -> &'a HeaderMap {
        match self {
            Self::Ready(headers) => headers,
            Self::Source(source) => source.headers(),
        }
    }
}

impl<'a> From<&'a HeaderMap> for DeliveryHeaders<'a> {
    fn from(headers: &'a HeaderMap) -> Self {
        Self::Ready(headers)
    }
}

/// The delivery itself, which is what the dispatch path hands the context.
pub(crate) struct FromDelivery<'m, M>(pub(crate) &'m M);

impl<M: IncomingMessage> HeaderSource for FromDelivery<'_, M> {
    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }
}

/// A post-settle continuation: a boxed `Send` future the dispatcher runs after the message (or
/// batch) has been settled.
type Continuation = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// The settlement kind a post-settle hook is gated on. Drop and retry are distinct settlements
/// (`nack` without vs with requeue), so they gate separately: [`HandlerResult::drop`] gates on
/// [`Drop`](Self::Drop), [`HandlerResult::retry`] on [`Retry`](Self::Retry), and
/// [`HandlerResult::retry_after`] on [`RetryAfter`](Self::RetryAfter) regardless of the delay
/// value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutcomeKind {
    Ack,
    Drop,
    Retry,
    RetryAfter,
}

impl OutcomeKind {
    /// The settlement kind of `outcome`: the `requeue` flag splits a `Nack` into drop vs retry; the
    /// `NackAfter` delay value is discarded.
    fn of(outcome: HandlerResult) -> Self {
        match outcome {
            HandlerResult::Ack => Self::Ack,
            HandlerResult::Nack { requeue: false } => Self::Drop,
            HandlerResult::Nack { requeue: true } => Self::Retry,
            HandlerResult::NackAfter { .. } => Self::RetryAfter,
        }
    }
}

/// One registered post-settle hook: the future to run, and the outcome variant it is gated on
/// (`None` means it runs regardless of how the message settled).
struct AfterHook {
    gate: Option<OutcomeKind>,
    fut: Continuation,
}

/// Per-delivery context, threaded through middleware and into the handler.
///
/// Carries the channel ([`name`](Self::name)), a working copy of the message
/// [`headers`](Self::headers) (middleware may enrich them for the handler; the broker message
/// itself is untouched), the typed shared application [state](Self::state) (where a publisher to
/// publish from a handler lives), and the broker's typed per-delivery context read by key
/// ([`context`](Self::context) / [`set`](Self::set)). The delivery's map is taken off the broker on
/// the first read of it, and the working copy is made on the first
/// [`headers_mut`](Self::headers_mut) call. Outgoing messages do not inherit it: replies and manual
/// publishes start from fresh headers, shaped by the publish pipeline.
pub struct Context<'a, C = (), S = ()> {
    name: &'a str,
    source: &'a (dyn HeaderSource + Sync),
    /// The delivery's own map, resolved off [`Context::source`] the first time anything reads it.
    original: OnceCell<&'a HeaderMap>,
    modified: Option<HeaderMap>,
    state: &'a S,
    cx: C,
    delivery: &'a Delivery<C>,
    after: Vec<AfterHook>,
    failfast: Option<&'a Shutdown>,
    /// The dispatch loop's encode buffer, lent to this delivery's reply. `None` where the
    /// context was built without one, and then the reply encodes into a buffer of its own.
    encode: Option<&'a mut BytesMut>,
    /// The subscriber's materialization policy, set by the dispatcher from the definition's
    /// failure policies. It reaches the handler body because a `Headers` contract is parsed
    /// there rather than in the decode adapter.
    decode: FailurePolicy,
    /// Set by the [`Typed`](super::typed::Typed) decode adapter when the payload fails to decode,
    /// so the dispatcher can record the outcome as a decode failure (otherwise indistinguishable
    /// from a handler drop). Present under the `testing` feature (harness classification) and the
    /// `otel` feature (the consume layer's decode-failure counter).
    #[cfg(any(feature = "testing", feature = "otel"))]
    decode_failed: bool,
}

impl<C, S> std::fmt::Debug for Context<'_, C, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("name", &self.name)
            .field("after_hooks", &self.after.len())
            .finish_non_exhaustive()
    }
}

impl<'a, C, S> Context<'a, C, S> {
    /// Creates a context for one delivery, taking the headers off `source` when something first
    /// asks for them and carrying the typed per-delivery context `cx` (built by
    /// [`BuildContext`](crate::BuildContext) from the broker message).
    pub(crate) fn new(
        name: &'a str,
        source: &'a (dyn HeaderSource + Sync),
        state: &'a S,
        cx: C,
        delivery: &'a Delivery<C>,
    ) -> Self {
        Self {
            name,
            source,
            original: OnceCell::new(),
            modified: None,
            state,
            cx,
            delivery,
            after: Vec::new(),
            failfast: None,
            encode: None,
            decode: FailurePolicy::Drop,
            #[cfg(any(feature = "testing", feature = "otel"))]
            decode_failed: false,
        }
    }

    /// Records that the payload failed to decode for this delivery. Called by the decode adapter so
    /// the harness and the otel consume layer can classify the outcome as a decode failure.
    #[cfg(any(feature = "testing", feature = "otel"))]
    pub(crate) fn mark_decode_failed(&mut self) {
        self.decode_failed = true;
    }

    /// Reads the decode-failure flag without clearing it. The otel consume layer runs inside the
    /// dispatch (before the harness's clearing [`took_decode_failed`](Self::took_decode_failed)
    /// read), so its read must leave the flag in place.
    #[cfg(feature = "otel")]
    pub(crate) fn decode_failed(&self) -> bool {
        self.decode_failed
    }

    /// Returns and clears the decode-failure flag for this delivery.
    #[cfg(feature = "testing")]
    pub(crate) fn took_decode_failed(&mut self) -> bool {
        std::mem::take(&mut self.decode_failed)
    }

    /// Attaches the runtime's error-shutdown handle, so a fail-fast decode policy can tear the
    /// service down from inside the handler. The dispatch loop sets this; contexts built in tests
    /// leave it unset (a fail-fast there logs but cannot reach the run loop).
    #[must_use]
    pub(crate) fn with_failfast(mut self, failfast: &'a Shutdown) -> Self {
        self.failfast = Some(failfast);
        self
    }

    /// Lends this delivery the dispatch loop's encode buffer.
    ///
    /// One buffer per sequential loop, per keyed lane and per pooled worker task, so a reply
    /// encoded into it allocates only while it is growing - which it stops doing after the
    /// subscription's first message. What the buffer is worth to the publish depends on the
    /// broker: a publisher that reads the payload is lent it, and one that keeps the payload is
    /// handed a buffer of its own regardless.
    pub(crate) fn with_encode_buffer(mut self, buf: &'a mut BytesMut) -> Self {
        self.encode = Some(buf);
        self
    }

    /// Takes the loop's encode buffer out for this delivery's reply.
    ///
    /// Taken rather than borrowed, so the rest of the context stays readable while the reply is
    /// being built: what the publish needs of the context (the subscription's name, the
    /// delivery's headers) is read next to the buffer, not through it.
    pub(crate) fn take_encode_buffer(&mut self) -> Option<&'a mut BytesMut> {
        self.encode.take()
    }

    /// Attaches the subscriber's effective materialization policy, so a handler-side contract
    /// (a [`Headers`](super::Headers) parameter) settles by the same policy as the
    /// payload codec no matter where the policy was named - in the attribute, or on the builder
    /// at the mount site.
    pub(crate) fn with_decode_policy(mut self, decode: FailurePolicy) -> Self {
        self.decode = decode;
        self
    }

    /// The materialization policy in force for this delivery: what happens to a payload that
    /// does not decode, or a header contract that does not parse.
    ///
    /// The `#[subscriber]` expansion reads it to settle a failed
    /// [`Headers`](super::Headers) extraction; a hand-written handler can read it for the
    /// same reason.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{Context, FailurePolicy};
    ///
    /// fn policy(ctx: &Context<'_>) -> FailurePolicy {
    ///     ctx.decode_policy()
    /// }
    /// ```
    #[must_use]
    pub fn decode_policy(&self) -> FailurePolicy {
        self.decode
    }

    /// Triggers a fail-fast shutdown for `reason` if a handle is attached, naming this delivery's
    /// subscription. Used by the decode path when its policy is
    /// [`FailFast`](super::FailurePolicy::FailFast).
    pub(crate) fn fail_fast(&self, reason: &str) {
        if let Some(failfast) = self.failfast {
            failfast.signal(self.name, reason);
        }
    }

    /// The channel (name / subject) the message arrived on.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name
    }

    /// The delivery's own headers, taken off the broker on the first call and kept for the rest
    /// of the delivery.
    fn original(&self) -> &'a HeaderMap {
        self.original.get_or_init(|| self.source.headers())
    }

    /// The working copy of the message headers.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        self.modified
            .as_ref()
            .map_or_else(|| self.original(), |modified| modified)
    }

    /// The headers a publish made from inside this delivery reads, without asking the broker
    /// for them until a transform does.
    pub(crate) fn delivery_headers(&self) -> DeliveryHeaders<'_> {
        if let Some(modified) = &self.modified {
            return DeliveryHeaders::Ready(modified);
        }
        self.original
            .get()
            .map_or(DeliveryHeaders::Source(self.source), |original| {
                DeliveryHeaders::Ready(original)
            })
    }

    /// The working copy of the message headers, mutably. The first call clones the message
    /// headers; later calls return the same copy.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        let original = self.original();
        self.modified.get_or_insert_with(|| original.clone())
    }

    /// Returns the shared application state: the typed `S` the app's `on_startup` produced (or
    /// `()` when the app declares none), borrowed for the delivery. Read its fields directly.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// struct AppState {
    ///     prefix: String,
    /// }
    ///
    /// async fn handle<M: IncomingMessage>(
    ///     _msg: &M,
    ///     ctx: &mut Context<'_, (), AppState>,
    /// ) -> HandlerOutcome {
    ///     let _prefix = &ctx.state().prefix;
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    #[must_use]
    pub fn state(&self) -> &S {
        self.state
    }

    /// Reads a broker-supplied per-delivery field off the typed context by compile-time `key`.
    ///
    /// The key is a zero-sized selector the broker exports; resolution is a direct field read off
    /// the typed context (no hashing, boxing, or downcasting). The default `()` context carries no
    /// fields, so keys exist only for brokers that expose a context type. For shared app state use
    /// [`state`](Self::state) instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{Field, IncomingMessage};
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// // A broker context with one field and the key that reads it.
    /// struct Delivery {
    ///     offset: u64,
    /// }
    /// #[derive(Clone, Copy)]
    /// struct Offset;
    /// impl Field<Delivery> for Offset {
    ///     type Value<'a> = u64;
    ///     fn get(self, d: &Delivery) -> u64 {
    ///         d.offset
    ///     }
    /// }
    ///
    /// async fn handle<M: IncomingMessage>(_m: &M, ctx: &mut Context<'_, Delivery>) -> HandlerOutcome {
    ///     let _offset = ctx.context(Offset);
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    pub fn context<K: Field<C>>(&self, key: K) -> K::Value<'_> {
        key.get(&self.cx)
    }

    /// The broker's per-delivery context, borrowed for the publish path.
    ///
    /// A reply published from this handler carries the delivery's typed context to its static
    /// [`PublishTransform`](super::PublishTransform) as a [`PublishContext`](super::PublishContext); this is
    /// the accessor the runtime uses to build that read-only view.
    pub(crate) fn cx_ref(&self) -> &C {
        &self.cx
    }

    /// Writes a per-delivery scratch value downstream handlers read by `key`.
    ///
    /// Middleware uses this to hand typed data to downstream handlers (an authenticated user, a
    /// correlation id) without serializing it into the headers, when the context type exposes a
    /// writable ([`FieldMut`](crate::FieldMut)) key.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{Field, FieldMut, IncomingMessage};
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// #[derive(Default)]
    /// struct Scratch {
    ///     user: Option<u64>,
    /// }
    /// #[derive(Clone, Copy)]
    /// struct User;
    /// impl Field<Scratch> for User {
    ///     type Value<'a> = Option<&'a u64>;
    ///     fn get(self, s: &Scratch) -> Option<&u64> {
    ///         s.user.as_ref()
    ///     }
    /// }
    /// impl FieldMut<Scratch> for User {
    ///     type Owned = u64;
    ///     fn set(self, s: &mut Scratch, value: u64) {
    ///         s.user = Some(value);
    ///     }
    /// }
    ///
    /// async fn handle<M: IncomingMessage>(_m: &M, ctx: &mut Context<'_, Scratch>) -> HandlerOutcome {
    ///     ctx.set(User, 7);
    ///     assert_eq!(ctx.context(User), Some(&7));
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    pub fn set<K: FieldMut<C>>(&mut self, key: K, value: K::Owned) {
        key.set(&mut self.cx, value);
    }

    /// Begins registering a post-settle hook gated on `outcome`.
    ///
    /// The returned builder's [`then`](After::then) registers a future that the dispatcher runs
    /// once the message has been settled, but only if the actual settlement matches `outcome` by
    /// kind. The four kinds are distinct: [`HandlerOutcome::ack`], [`HandlerOutcome::drop`] (nack
    /// without requeue), [`HandlerOutcome::retry`] (nack with requeue), and
    /// [`HandlerOutcome::retry_after`] (which matches regardless of the delay). So a hook gated
    /// on `drop()` does not fire on a `retry()` settlement, and vice versa. Multiple hooks
    /// accumulate and every matching one runs. Any continuation already attached to the gate
    /// value itself is ignored: the hook future is what `then` registers.
    ///
    /// The hook is scoped to the whole delivery. On the batch path a `Context` is one per batch,
    /// so a hook registered here runs after the entire batch settles; because a batch has
    /// per-element outcomes, the outcome gate is ignored there and only [`after_settle`](Self::after_settle)
    /// hooks (which run regardless) fire (see that method).
    ///
    /// # Cancel safety
    ///
    /// Post-settle hooks are at-most-once: the message is already settled before any hook runs, so
    /// a hook that panics, or that is lost when the process crashes, never causes a redelivery and
    /// never blocks the next delivery. A graceful shutdown drains in-flight hooks (bounded by the
    /// app's [`shutdown_timeout`](super::RustStream::shutdown_timeout)); an aborted shutdown may
    /// drop them.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, Handler, HandlerOutcome};
    ///
    /// fn use_after<M: IncomingMessage + 'static>() {
    ///     let _handler = |_msg: &M, ctx: &mut Context| {
    ///         ctx.after(HandlerOutcome::ack())
    ///             .then(async move { /* runs only after this message is acked */ });
    ///         async { HandlerOutcome::ack() }
    ///     };
    /// }
    /// ```
    // The gate is a just-built outcome token (`after(HandlerOutcome::ack())`); a reference
    // parameter would force `&` noise at every call site.
    #[allow(clippy::needless_pass_by_value)]
    pub fn after(&mut self, outcome: HandlerOutcome) -> After<'_, 'a, C, S> {
        After {
            ctx: self,
            gate: Some(OutcomeKind::of(outcome.outcome())),
        }
    }

    /// Registers a post-settle hook that runs only after the message is acked.
    ///
    /// Sugar for `self.after(HandlerOutcome::ack()).then(fut)`; see [`after`](Self::after) for
    /// the gating and cancel-safety semantics.
    ///
    /// # Cancel safety
    ///
    /// At-most-once, as for [`after`](Self::after): the ack has already happened, so a lost hook
    /// never redelivers.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// fn use_after_ack<M: IncomingMessage + 'static>() {
    ///     let _handler = |_msg: &M, ctx: &mut Context| {
    ///         ctx.after_ack(async move { /* fire-and-forget once acked */ });
    ///         async { HandlerOutcome::ack() }
    ///     };
    /// }
    /// ```
    pub fn after_ack(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        self.after.push(AfterHook {
            gate: Some(OutcomeKind::Ack),
            fut: Box::pin(fut),
        });
    }

    /// Registers a post-settle hook that runs after the message settles, whatever the outcome.
    ///
    /// Unlike [`after`](Self::after) this has no outcome gate, so it fires on `Ack`, `Drop`,
    /// `Retry`, and `RetryAfter` alike. It is the only post-settle form honoured on the batch path, where the
    /// per-element outcomes make an outcome gate ill-defined; there it runs once after the whole
    /// batch has been settled.
    ///
    /// # Cancel safety
    ///
    /// At-most-once, as for [`after`](Self::after).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// fn use_after_settle<M: IncomingMessage + 'static>() {
    ///     let _handler = |_msg: &M, ctx: &mut Context| {
    ///         ctx.after_settle(async move { /* runs once the message is settled, any outcome */ });
    ///         async { HandlerOutcome::retry() }
    ///     };
    /// }
    /// ```
    pub fn after_settle(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        self.after.push(AfterHook {
            gate: None,
            fut: Box::pin(fut),
        });
    }

    /// Whether this delivery registered any post-settle hook at all. The dispatch path reads it
    /// first so a delivery that registered none never builds the drained list.
    pub(crate) fn has_hooks(&self) -> bool {
        !self.after.is_empty()
    }

    /// Drains the registered hooks whose gate matches `outcome` (and the ungated ones), in
    /// registration order. Used by the single-message dispatch path after it settles the message.
    pub(crate) fn take_hooks_for(&mut self, outcome: HandlerResult) -> Vec<Continuation> {
        let kind = OutcomeKind::of(outcome);
        let mut runnable = Vec::new();
        let mut kept = Vec::new();
        for hook in self.after.drain(..) {
            if hook.gate.is_none_or(|gate| gate == kind) {
                runnable.push(hook.fut);
            } else {
                kept.push(hook);
            }
        }
        self.after = kept;
        runnable
    }

    /// Drains the ungated hooks (registered via [`after_settle`](Self::after_settle)), in
    /// registration order. Used by the batch dispatch path, where per-element outcomes make the
    /// outcome gate ill-defined, so only ungated hooks run.
    pub(crate) fn take_settle_hooks(&mut self) -> Vec<Continuation> {
        let mut runnable = Vec::new();
        let mut kept = Vec::new();
        for hook in self.after.drain(..) {
            if hook.gate.is_none() {
                runnable.push(hook.fut);
            } else {
                kept.push(hook);
            }
        }
        self.after = kept;
        runnable
    }

    /// Returns the subscription's delivery context: the task tracker post-settle
    /// [`HandlerResult::and_after`] continuations are spawned onto, and the publisher and address
    /// the `retry_after` fallback re-publishes through.
    ///
    /// Borrowed for the scope's lifetime rather than the context's, so the batch path can settle
    /// through it after handing `&mut self` to the handler - and so no borrow of the context is
    /// held across an await, which would demand `Context: Sync`.
    pub(crate) fn delivery(&self) -> &'a Delivery<C> {
        self.delivery
    }

    /// The subscription's name, borrowed for the scope's lifetime rather than the context's.
    ///
    /// [`name`](Self::name) borrows through `&self`, so a batch path that hands `&mut self` to
    /// the handler and still has to name the subscription afterwards had to copy the name into a
    /// `String` once per batch. The name outlives every delivery of the subscription, so the
    /// longer borrow is what it always was.
    pub(crate) fn subscription(&self) -> &'a str {
        self.name
    }
}

/// A builder for an outcome-gated post-settle hook, returned by [`Context::after`].
///
/// Call [`then`](Self::then) to register the continuation. Holding it without calling `then`
/// registers nothing.
#[must_use = "call `.then(fut)` to register the post-settle hook"]
pub struct After<'ctx, 'a, C = (), S = ()> {
    ctx: &'ctx mut Context<'a, C, S>,
    gate: Option<OutcomeKind>,
}

impl<C, S> std::fmt::Debug for After<'_, '_, C, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("After").field("gate", &self.gate).finish()
    }
}

impl<C, S> After<'_, '_, C, S> {
    /// Registers `fut` to run after the message settles, if the settlement matches the gate this
    /// builder was created with (see [`Context::after`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// fn use_then<M: IncomingMessage + 'static>() {
    ///     let _handler = |_msg: &M, ctx: &mut Context| {
    ///         ctx.after(HandlerOutcome::drop())
    ///             .then(async move { /* runs only if the message is dropped (nack, no requeue) */ });
    ///         async { HandlerOutcome::drop() }
    ///     };
    /// }
    /// ```
    pub fn then(self, fut: impl Future<Output = ()> + Send + 'static) {
        self.ctx.after.push(AfterHook {
            gate: self.gate,
            fut: Box::pin(fut),
        });
    }
}

#[cfg(test)]
mod tests;
