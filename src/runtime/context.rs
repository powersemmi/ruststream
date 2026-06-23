//! Per-delivery [`Context`] and the app-level [`State`] type-map.
//!
//! A `Context` is built for each delivery and threaded (as `&mut`) through the middleware chain
//! into the handler. It carries the channel the message arrived on, a working copy of the
//! headers (middleware may enrich them), shared application state ([`Context::state`]), and the
//! broker's typed per-delivery context read by key ([`Context::context`] / [`Context::set`]). The
//! copy is lazy: the message headers are borrowed until the first [`headers_mut`](Context::headers_mut),
//! so a delivery whose middleware never touches them pays no clone.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use crate::{Field, FieldMut, Headers};

use super::dispatch::Delivery;
use super::failure::ErrorShutdown;
use super::handler::HandlerResult;
use super::publish::ScopedPublisher;

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

/// App-level shared state: a type-map holding one value per type.
///
/// Put shared resources (database pools, HTTP clients, configuration) in here with
/// [`RustStream::insert_state`](super::RustStream::insert_state); handlers and middleware read them
/// from the [`Context`] with [`Context::state`] then [`State::get`]. For data scoped to a single
/// delivery (broker fields, or middleware scratch), use the typed per-delivery context instead,
/// reached with [`Context::context`] / [`Context::set`].
#[derive(Default)]
pub struct State {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl State {
    /// Inserts `value`, replacing any previous value of the same type.
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(value));
    }

    /// Returns the stored value of type `T`, if any.
    #[must_use]
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.map.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("entries", &self.map.len())
            .finish_non_exhaustive()
    }
}

/// Per-delivery context, threaded through middleware and into the handler.
///
/// Carries the channel ([`name`](Self::name)), a working copy of the message
/// [`headers`](Self::headers) (middleware may enrich them for the handler; the broker message
/// itself is untouched), shared application [state](Self::state), the broker's typed per-delivery
/// context read by key ([`context`](Self::context) / [`set`](Self::set)), and access to named
/// [`publisher`](Self::publisher)s for publishing from inside a handler. The headers copy is made
/// lazily on the first [`headers_mut`](Self::headers_mut) call. Outgoing messages do not inherit
/// it: replies and manual publishes start from fresh headers, shaped by the publish pipeline.
pub struct Context<'a, C = ()> {
    name: &'a str,
    original: &'a Headers,
    modified: Option<Headers>,
    state: &'a State,
    cx: C,
    delivery: &'a Delivery,
    after: Vec<AfterHook>,
    failfast: Option<&'a ErrorShutdown>,
}

impl<C> std::fmt::Debug for Context<'_, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("name", &self.name)
            .field("after_hooks", &self.after.len())
            .finish_non_exhaustive()
    }
}

impl<'a, C> Context<'a, C> {
    /// Creates a context for one delivery, borrowing the message headers until first mutation and
    /// carrying the typed per-delivery context `cx` (built by
    /// [`BuildContext`](crate::BuildContext) from the broker message).
    pub(crate) fn new(
        name: &'a str,
        headers: &'a Headers,
        state: &'a State,
        cx: C,
        delivery: &'a Delivery,
    ) -> Self {
        Self {
            name,
            original: headers,
            modified: None,
            state,
            cx,
            delivery,
            after: Vec::new(),
            failfast: None,
        }
    }

    /// Attaches the runtime's error-shutdown handle, so a fail-fast decode policy can tear the
    /// service down from inside the handler. The dispatch loop sets this; contexts built in tests
    /// leave it unset (a fail-fast there logs but cannot reach the run loop).
    #[must_use]
    pub(crate) fn with_failfast(mut self, failfast: &'a ErrorShutdown) -> Self {
        self.failfast = Some(failfast);
        self
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

    /// Resolves a named publisher (registered with
    /// [`RustStream::publisher`](super::RustStream::publisher)) to publish from this handler.
    ///
    /// Sends through it run the scope's publish middleware (envelope, metrics) - the same chain as a
    /// macro reply - so a manual publish is not a hole in the pipeline. Returns `None` if no
    /// publisher is registered under `name`.
    #[must_use]
    pub fn publisher(&self, name: &str) -> Option<ScopedPublisher<'_>> {
        let publisher = self.delivery.publishers.get(name)?;
        Some(ScopedPublisher::new(
            publisher.as_ref(),
            &self.delivery.pipeline,
        ))
    }

    /// The working copy of the message headers.
    #[must_use]
    pub fn headers(&self) -> &Headers {
        self.modified.as_ref().unwrap_or(self.original)
    }

    /// The working copy of the message headers, mutably. The first call clones the message
    /// headers; later calls return the same copy.
    pub fn headers_mut(&mut self) -> &mut Headers {
        self.modified.get_or_insert_with(|| self.original.clone())
    }

    /// Returns the shared application [`State`], the type-map set once at build with
    /// [`RustStream::insert_state`](super::RustStream::insert_state). Read a value from it with
    /// [`State::get`].
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerResult};
    ///
    /// async fn handle<M: IncomingMessage>(_msg: &M, ctx: &mut Context<'_>) -> HandlerResult {
    ///     if let Some(prefix) = ctx.state().get::<String>() {
    ///         let _ = prefix;
    ///     }
    ///     HandlerResult::Ack
    /// }
    /// ```
    #[must_use]
    pub fn state(&self) -> &State {
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
    /// use ruststream::runtime::{Context, HandlerResult};
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
    /// async fn handle<M: IncomingMessage>(_m: &M, ctx: &mut Context<'_, Delivery>) -> HandlerResult {
    ///     let _offset = ctx.context(Offset);
    ///     HandlerResult::Ack
    /// }
    /// ```
    pub fn context<K: Field<C>>(&self, key: K) -> K::Value<'_> {
        key.get(&self.cx)
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
    /// use ruststream::runtime::{Context, HandlerResult};
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
    /// async fn handle<M: IncomingMessage>(_m: &M, ctx: &mut Context<'_, Scratch>) -> HandlerResult {
    ///     ctx.set(User, 7);
    ///     assert_eq!(ctx.context(User), Some(&7));
    ///     HandlerResult::Ack
    /// }
    /// ```
    pub fn set<K: FieldMut<C>>(&mut self, key: K, value: K::Owned) {
        key.set(&mut self.cx, value);
    }

    /// Begins registering a post-settle hook gated on `outcome`.
    ///
    /// The returned builder's [`then`](After::then) registers a future that the dispatcher runs
    /// once the message has been settled, but only if the actual settlement matches `outcome` by
    /// kind. The four kinds are distinct: [`HandlerResult::Ack`], [`HandlerResult::drop`] (nack
    /// without requeue), [`HandlerResult::retry`] (nack with requeue), and
    /// [`HandlerResult::retry_after`] (which matches regardless of the delay). So a hook gated on
    /// `drop()` does not fire on a `retry()` settlement, and vice versa. Multiple hooks accumulate
    /// and every matching one runs.
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
    /// use ruststream::runtime::{Context, Handler, HandlerResult};
    ///
    /// fn use_after<M: IncomingMessage + 'static>() {
    ///     let _handler = |_msg: &M, ctx: &mut Context| {
    ///         ctx.after(HandlerResult::Ack)
    ///             .then(async move { /* runs only after this message is acked */ });
    ///         async { HandlerResult::Ack }
    ///     };
    /// }
    /// ```
    pub fn after(&mut self, outcome: HandlerResult) -> After<'_, 'a, C> {
        After {
            ctx: self,
            gate: Some(OutcomeKind::of(outcome)),
        }
    }

    /// Registers a post-settle hook that runs only after the message is acked.
    ///
    /// Sugar for `self.after(HandlerResult::Ack).then(fut)`; see [`after`](Self::after) for the
    /// gating and cancel-safety semantics.
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
    /// use ruststream::runtime::{Context, HandlerResult};
    ///
    /// fn use_after_ack<M: IncomingMessage + 'static>() {
    ///     let _handler = |_msg: &M, ctx: &mut Context| {
    ///         ctx.after_ack(async move { /* fire-and-forget once acked */ });
    ///         async { HandlerResult::Ack }
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
    /// use ruststream::runtime::{Context, HandlerResult};
    ///
    /// fn use_after_settle<M: IncomingMessage + 'static>() {
    ///     let _handler = |_msg: &M, ctx: &mut Context| {
    ///         ctx.after_settle(async move { /* runs once the message is settled, any outcome */ });
    ///         async { HandlerResult::retry() }
    ///     };
    /// }
    /// ```
    pub fn after_settle(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        self.after.push(AfterHook {
            gate: None,
            fut: Box::pin(fut),
        });
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

    /// Returns the app-wide task tracker for post-settle [`HandlerResult::and_after`]
    /// continuations. The dispatcher spawns each element's continuation onto it after settling,
    /// and the single-message path uses it the same way, so a graceful shutdown drains in-flight
    /// continuations.
    pub(crate) fn tasks(&self) -> &tokio_util::task::TaskTracker {
        &self.delivery.tasks
    }
}

/// A builder for an outcome-gated post-settle hook, returned by [`Context::after`].
///
/// Call [`then`](Self::then) to register the continuation. Holding it without calling `then`
/// registers nothing.
#[must_use = "call `.then(fut)` to register the post-settle hook"]
pub struct After<'ctx, 'a, C = ()> {
    ctx: &'ctx mut Context<'a, C>,
    gate: Option<OutcomeKind>,
}

impl<C> std::fmt::Debug for After<'_, '_, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("After").field("gate", &self.gate).finish()
    }
}

impl<C> After<'_, '_, C> {
    /// Registers `fut` to run after the message settles, if the settlement matches the gate this
    /// builder was created with (see [`Context::after`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerResult};
    ///
    /// fn use_then<M: IncomingMessage + 'static>() {
    ///     let _handler = |_msg: &M, ctx: &mut Context| {
    ///         ctx.after(HandlerResult::drop())
    ///             .then(async move { /* runs only if the message is dropped (nack, no requeue) */ });
    ///         async { HandlerResult::drop() }
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
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use futures::future::join_all;

    use super::{Context, State};
    use crate::Headers;
    use crate::runtime::dispatch::Delivery;
    use crate::runtime::handler::HandlerResult;

    fn run_all(continuations: Vec<super::Continuation>) {
        futures::executor::block_on(async {
            join_all(continuations).await;
        });
    }

    #[test]
    fn outcome_kind_distinguishes_drop_retry_and_retry_after() {
        use super::OutcomeKind;
        assert_eq!(OutcomeKind::of(HandlerResult::Ack), OutcomeKind::Ack);
        // drop (nack, no requeue) and retry (nack, requeue) are distinct kinds.
        assert_eq!(OutcomeKind::of(HandlerResult::drop()), OutcomeKind::Drop);
        assert_eq!(OutcomeKind::of(HandlerResult::retry()), OutcomeKind::Retry);
        assert_ne!(
            OutcomeKind::of(HandlerResult::drop()),
            OutcomeKind::of(HandlerResult::retry()),
        );
        // retry_after is its own kind, distinct from retry, and matches regardless of the delay.
        assert_eq!(
            OutcomeKind::of(HandlerResult::retry_after(Duration::from_secs(1))),
            OutcomeKind::RetryAfter,
        );
        assert_ne!(
            OutcomeKind::of(HandlerResult::retry_after(Duration::ZERO)),
            OutcomeKind::of(HandlerResult::retry()),
        );
        assert_eq!(
            OutcomeKind::of(HandlerResult::retry_after(Duration::from_secs(1))),
            OutcomeKind::of(HandlerResult::retry_after(Duration::from_secs(9))),
        );
    }

    #[test]
    fn take_hooks_runs_only_the_matching_gate() {
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("t", &headers, &state, (), &delivery);

        let acked = Arc::new(AtomicU32::new(0));
        let dropped = Arc::new(AtomicU32::new(0));
        let retried = Arc::new(AtomicU32::new(0));
        let settled = Arc::new(AtomicU32::new(0));

        let bump = |c: &Arc<AtomicU32>| {
            let c = Arc::clone(c);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        };

        ctx.after(HandlerResult::Ack).then(bump(&acked));
        ctx.after(HandlerResult::drop()).then(bump(&dropped));
        ctx.after(HandlerResult::retry()).then(bump(&retried));
        ctx.after_ack(bump(&acked));
        ctx.after_settle(bump(&settled));

        // Settling with Ack runs both ack-gated hooks and the ungated one, not drop or retry.
        run_all(ctx.take_hooks_for(HandlerResult::Ack));
        assert_eq!(acked.load(Ordering::SeqCst), 2);
        assert_eq!(settled.load(Ordering::SeqCst), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_eq!(retried.load(Ordering::SeqCst), 0);

        // A retry settle runs only the retry-gated hook: drop and retry are distinct mechanics.
        run_all(ctx.take_hooks_for(HandlerResult::retry()));
        assert_eq!(retried.load(Ordering::SeqCst), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);

        // The drop-gated hook is still registered: a later drop settle runs it.
        run_all(ctx.take_hooks_for(HandlerResult::drop()));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(retried.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn take_settle_hooks_drops_outcome_gated_ones() {
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("t", &headers, &state, (), &delivery);

        let gated = Arc::new(AtomicU32::new(0));
        let ungated = Arc::new(AtomicU32::new(0));

        let gated_clone = Arc::clone(&gated);
        ctx.after(HandlerResult::Ack).then(async move {
            gated_clone.fetch_add(1, Ordering::SeqCst);
        });
        let ungated_clone = Arc::clone(&ungated);
        ctx.after_settle(async move {
            ungated_clone.fetch_add(1, Ordering::SeqCst);
        });

        // The batch path drops outcome-gated hooks (per-element outcomes), keeps ungated ones.
        run_all(ctx.take_settle_hooks());
        assert_eq!(ungated.load(Ordering::SeqCst), 1);
        assert_eq!(gated.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn context_reads_typed_field_by_key() {
        use crate::Field;

        struct Meta {
            offset: u64,
        }
        #[derive(Clone, Copy)]
        struct Offset;
        impl Field<Meta> for Offset {
            type Value<'a> = u64;
            fn get(self, m: &Meta) -> u64 {
                m.offset
            }
        }

        let mut state = State::default();
        state.insert(String::from("app"));
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let ctx = Context::new("test", &headers, &state, Meta { offset: 42 }, &delivery);

        // The typed broker field is read by key, straight off the context.
        assert_eq!(ctx.context(Offset), 42);
        // App state is reached only through state(), independent of the per-delivery context.
        assert_eq!(ctx.state().get::<String>().map(String::as_str), Some("app"));
    }

    #[test]
    fn set_writes_scratch_and_reads_it_back() {
        use crate::{Field, FieldMut};

        #[derive(Default)]
        struct Scratch {
            user: Option<u64>,
        }
        #[derive(Clone, Copy)]
        struct User;
        impl Field<Scratch> for User {
            type Value<'a> = Option<&'a u64>;
            fn get(self, s: &Scratch) -> Option<&u64> {
                s.user.as_ref()
            }
        }
        impl FieldMut<Scratch> for User {
            type Owned = u64;
            fn set(self, s: &mut Scratch, value: u64) {
                s.user = Some(value);
            }
        }

        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("test", &headers, &state, Scratch::default(), &delivery);

        assert_eq!(ctx.context(User), None);
        ctx.set(User, 9);
        assert_eq!(ctx.context(User), Some(&9));
    }

    #[test]
    fn headers_clone_only_on_first_mutation() {
        let mut original = Headers::new();
        original.insert("k", "v");
        let state = State::default();
        let delivery = Delivery::empty();
        let mut ctx = Context::new("test", &original, &state, (), &delivery);

        // Untouched: the context borrows the message headers, no copy exists.
        assert!(std::ptr::eq(ctx.headers(), &raw const original));

        ctx.headers_mut().insert("added", "1");
        ctx.headers_mut().insert("added2", "2");

        // Mutations land in one lazily-made copy; the original is untouched.
        assert!(!std::ptr::eq(ctx.headers(), &raw const original));
        assert_eq!(ctx.headers().get("added"), Some(&b"1"[..]));
        assert_eq!(ctx.headers().get("k"), Some(&b"v"[..]));
        assert_eq!(original.get("added"), None);
    }
}
