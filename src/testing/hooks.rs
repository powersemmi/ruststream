//! The hooks a test registers on its assertion handles
//! ([`SubscriberAssertions::on_call`](super::SubscriberAssertions::on_call),
//! [`PublishedAssertions::on_publish`](super::PublishedAssertions::on_publish)), and how the
//! harness runs them at the event.

use std::any::Any;
use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use tokio::runtime::Handle;
use tokio::sync::oneshot;

use super::assertions::{PublishedAssertions, SubscriberAssertions};
use super::coordinator::Coordinator;

/// A hook's future. Not `Send`: stable Rust cannot require it of an `AsyncFn`'s future, which is
/// why the harness polls it on a thread of its own rather than inside the task that saw the event.
type HookFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

/// An `on_call` hook with its closure type erased.
trait CallHook: Send + Sync {
    fn call<'a>(&'a self, handle: SubscriberAssertions<'a>) -> HookFuture<'a>;
}

impl<Hook> CallHook for Hook
where
    Hook: AsyncFn(SubscriberAssertions<'_>) + Send + Sync,
{
    fn call<'a>(&'a self, handle: SubscriberAssertions<'a>) -> HookFuture<'a> {
        Box::pin(self(handle))
    }
}

/// An `on_publish` hook with its closure and payload types erased: it builds the handle it is
/// handed, so the registry needs neither.
trait PublishHook: Send + Sync {
    fn call<'a>(&'a self, coordinator: &Coordinator, target: &PublishTarget) -> HookFuture<'a>;
}

/// An `on_publish` hook over `PublishedAssertions<Payload>`.
struct TypedPublishHook<Payload, Hook> {
    hook: Hook,
    _payload: PhantomData<fn() -> Payload>,
}

impl<Payload, Hook> PublishHook for TypedPublishHook<Payload, Hook>
where
    Hook: AsyncFn(PublishedAssertions<Payload>) + Send + Sync,
{
    fn call<'a>(&'a self, coordinator: &Coordinator, target: &PublishTarget) -> HookFuture<'a> {
        Box::pin((self.hook)(PublishedAssertions::at(coordinator, target)))
    }
}

/// What a [`PublishedAssertions`] reads, and so what its hooks are keyed by: a destination on
/// one broker of the app, or an `Out` slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishTarget {
    /// The destination `name` on the broker registered at `scope_id`.
    Channel { scope_id: usize, name: String },
    /// The `Out` slot named this.
    Slot(&'static str),
}

/// The hooks registered on one coordinator, and what they still owe.
#[derive(Default)]
pub(crate) struct Hooks {
    /// Whether any hook was ever registered: a test that registers none pays this load alone.
    registered: AtomicBool,
    calls: Mutex<Vec<OnCall>>,
    publishes: Mutex<Vec<OnPublish>>,
    /// Calls recorded with hooks to run, held until the dispatch that made the call settled it.
    due_calls: Mutex<Vec<DueCall>>,
    /// The first panic a hook raised, held for the test's task to raise again.
    failed: Mutex<Option<Box<dyn Any + Send>>>,
}

/// An `on_call` hook, with the handle it was registered on.
struct OnCall {
    scope_id: usize,
    name: String,
    hook: Arc<dyn CallHook>,
}

/// An `on_publish` hook, with what its handle reads.
struct OnPublish {
    target: PublishTarget,
    hook: Arc<dyn PublishHook>,
}

/// One recorded call and the hooks it runs: those registered on its handle when it was recorded.
struct DueCall {
    /// The subscription of the app the call came through, whose dispatch runs the hooks.
    subscription: usize,
    scope_id: usize,
    name: String,
    hooks: Vec<Arc<dyn CallHook>>,
}

/// One recorded publish and the hooks it runs, or nothing when no hook matched it.
#[must_use]
pub(crate) struct DuePublish(Option<PublishRun>);

/// The hooks one publish runs, and what their handle reads.
struct PublishRun {
    coordinator: Coordinator,
    target: PublishTarget,
    hooks: Vec<Arc<dyn PublishHook>>,
}

impl DuePublish {
    /// No hook to run.
    pub(crate) const NONE: Self = Self(None);
}

impl Hooks {
    /// Registers `hook` on the calls of the subscription `name` of the broker at `scope_id`.
    pub(crate) fn on_call<Hook>(&self, scope_id: usize, name: String, hook: Hook)
    where
        Hook: AsyncFn(SubscriberAssertions<'_>) + Send + Sync + 'static,
    {
        lock(&self.calls).push(OnCall {
            scope_id,
            name,
            hook: Arc::new(hook),
        });
        self.registered.store(true, Ordering::SeqCst);
    }

    /// Registers `hook` on the publishes `target` reads.
    pub(crate) fn on_publish<Payload, Hook>(&self, target: PublishTarget, hook: Hook)
    where
        Payload: 'static,
        Hook: AsyncFn(PublishedAssertions<Payload>) + Send + Sync + 'static,
    {
        let hook = TypedPublishHook {
            hook,
            _payload: PhantomData,
        };
        lock(&self.publishes).push(OnPublish {
            target,
            hook: Arc::new(hook),
        });
        self.registered.store(true, Ordering::SeqCst);
    }

    /// Notes a call just recorded on the subscription numbered `subscription`, if any hook is
    /// registered on its handle; [`Coordinator::run_due_calls`] runs them.
    pub(crate) fn called(&self, subscription: usize, scope_id: usize, name: &str) {
        if !self.registered.load(Ordering::SeqCst) {
            return;
        }
        let hooks: Vec<_> = lock(&self.calls)
            .iter()
            .filter(|on| on.scope_id == scope_id && on.name == name)
            .map(|on| Arc::clone(&on.hook))
            .collect();
        if !hooks.is_empty() {
            lock(&self.due_calls).push(DueCall {
                subscription,
                scope_id,
                name: name.to_owned(),
                hooks,
            });
        }
    }

    /// The hooks a publish just recorded runs: those registered on `target`, the record it went
    /// into.
    /// The target is built only once a hook is registered, so a test with none pays for no name.
    pub(crate) fn published(
        &self,
        coordinator: &Coordinator,
        target: impl FnOnce() -> PublishTarget,
    ) -> DuePublish {
        if !self.registered.load(Ordering::SeqCst) {
            return DuePublish::NONE;
        }
        let target = target();
        let hooks: Vec<_> = lock(&self.publishes)
            .iter()
            .filter(|on| on.target == target)
            .map(|on| Arc::clone(&on.hook))
            .collect();
        DuePublish((!hooks.is_empty()).then(|| PublishRun {
            coordinator: coordinator.clone(),
            target,
            hooks,
        }))
    }

    /// Takes the calls of the subscription numbered `subscription` whose hooks are still to run.
    fn take_due_calls(&self, subscription: usize) -> Vec<DueCall> {
        if !self.registered.load(Ordering::SeqCst) {
            return Vec::new();
        }
        let mut due = lock(&self.due_calls);
        let (taken, kept) = due
            .drain(..)
            .partition(|call| call.subscription == subscription);
        *due = kept;
        drop(due);
        taken
    }

    /// Raises again, in the caller's task, the first panic a hook raised.
    pub(crate) fn raise_failure(&self) {
        let failed = lock(&self.failed).take();
        if let Some(payload) = failed {
            panic::resume_unwind(payload);
        }
    }
}

impl Coordinator {
    /// Runs the hooks of the calls the subscription numbered `subscription` recorded, once the
    /// dispatch that made them has settled them.
    pub(crate) async fn run_due_calls(&self, subscription: usize) {
        for call in self.hooks().take_due_calls(subscription) {
            on_hook_thread(self, move |coordinator| {
                Box::pin(async move {
                    for hook in &call.hooks {
                        let handle = SubscriberAssertions::new(
                            coordinator,
                            call.scope_id,
                            call.name.clone(),
                        );
                        hook.call(handle).await;
                    }
                })
            })
            .await;
        }
    }
}

impl DuePublish {
    /// Runs the hooks, if any matched the publish.
    pub(crate) async fn run(self) {
        let Some(PublishRun {
            coordinator,
            target,
            hooks,
        }) = self.0
        else {
            return;
        };
        on_hook_thread(&coordinator, move |coordinator| {
            Box::pin(async move {
                for hook in &hooks {
                    hook.call(coordinator, &target).await;
                }
            })
        })
        .await;
    }
}

/// Runs `hooks` to completion on a thread of their own, inside the test's runtime, and waits for
/// them. A panic they raise is kept for the test's task to raise again
/// ([`Hooks::raise_failure`]).
///
/// Why a thread: a hook's future is not `Send` (see [`HookFuture`]), and every task that sees an
/// event is. `Handle::block_on` on a plain thread keeps the hook on the test's runtime, its clock
/// included, and lets a paused clock advance while the hook waits on a timer.
async fn on_hook_thread<Run>(coordinator: &Coordinator, hooks: Run)
where
    Run: for<'c> FnOnce(&'c Coordinator) -> HookFuture<'c> + Send + 'static,
{
    let runtime = Handle::current();
    let owned = coordinator.clone();
    let (done, finished) = oneshot::channel();
    thread::spawn(move || {
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| runtime.block_on(hooks(&owned))));
        let _ = done.send(outcome);
    });
    if let Ok(Err(payload)) = finished.await {
        lock(&coordinator.hooks().failed).get_or_insert(payload);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("test hooks mutex poisoned")
}
