//! The hooks a test registers on its assertion handles
//! ([`SubscriberAssertions::on_call`](super::SubscriberAssertions::on_call),
//! [`PublishedAssertions::on_publish`](super::PublishedAssertions::on_publish)), and how the
//! harness runs them at the event.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::any::Any;
use core::marker::PhantomData;
use core::mem;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use futures::FutureExt;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::assertions::{PublishedAssertions, SubscriberAssertions};
use super::coordinator::Coordinator;

/// A hook's future. Not `Send`: stable Rust cannot require it of an `AsyncFn`'s future, which is
/// why the tokio harness polls it on a thread of its own rather than inside the task that saw the
/// event.
type HookFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

/// An `on_call` hook with its closure type erased.
pub(crate) trait CallHook: Send + Sync {
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
    fn call<'a>(
        &'a self,
        coordinator: &Coordinator,
        target: &PublishTarget,
        within: Within,
    ) -> HookFuture<'a>;
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
    fn call<'a>(
        &'a self,
        coordinator: &Coordinator,
        target: &PublishTarget,
        within: Within,
    ) -> HookFuture<'a> {
        Box::pin((self.hook)(PublishedAssertions::at(
            coordinator,
            target,
            within,
        )))
    }
}

/// How much of a record a handle reads: the entries recorded up to an event, the event's own
/// included, so a hook reads the record as it stood at its event whatever came after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Within(usize);

impl Within {
    /// The whole record, as it stands whenever it is read.
    pub const ALL: Self = Self(usize::MAX);

    /// The first `len` entries of the record.
    pub(crate) const fn first(len: usize) -> Self {
        Self(len)
    }

    /// How many entries from the start of the record this reads.
    pub(crate) const fn len(self) -> usize {
        self.0
    }
}

/// What a [`PublishedAssertions`] reads, and so what its hooks are keyed by: a destination on
/// one broker of the app, or an `Out` slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishTarget {
    /// The destination `name` on the broker registered at `scope_id`.
    Channel {
        /// The scope of the broker the destination is on.
        scope_id: usize,
        /// The destination's name.
        name: String,
    },
    /// The `Out` slot named this.
    Slot(&'static str),
}

/// Where the hooks of one coordinator run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum HookRunner {
    /// On a thread of their own inside the test's tokio runtime: the app's tasks are `Send`,
    /// and a hook's future is not.
    #[default]
    Thread,
    /// Inside the future that recorded the event: an app that runs as one future, polled by the
    /// test's own future, has no task a hook would block and no thread to send it to.
    InPlace,
}

/// The hooks registered on one coordinator, and what they still owe.
#[derive(Default)]
// Machinery the harness crate drives, hidden from the docs; it holds erased closures and
// futures that have no useful `Debug`.
#[allow(missing_debug_implementations)]
pub struct Hooks {
    /// Where the hooks run, fixed by the harness that made the coordinator.
    runner: HookRunner,
    /// Whether any hook was ever registered: a test that registers none pays this load alone.
    registered: AtomicBool,
    calls: Mutex<Vec<OnCall>>,
    publishes: Mutex<Vec<OnPublish>>,
    /// The first panic a hook raised, held for the test's task to raise again.
    failed: Mutex<Option<Box<dyn Any + Send>>>,
    /// The hook threads still running, so a shutdown returns only once they are gone.
    running: TaskTracker,
    /// Cancelled once the test ends: every hook still running is dropped where it waits.
    stopped: CancellationToken,
    /// The hooks of events a dispatch waits on, for the harness that polls the app to run
    /// ([`HookRunner::InPlace`]).
    pending: Mutex<Vec<PendingHooks>>,
}

/// The hooks of one event, handed to the harness that polls the app
/// ([`HookRunner::InPlace`]): the harness turns them into a future and polls it beside the app.
// Machinery the harness crate drives, hidden from the docs; it holds an erased closure.
#[allow(missing_debug_implementations)]
pub struct PendingHooks {
    run: Box<dyn for<'c> FnOnce(&'c Coordinator) -> HookFuture<'c> + Send>,
    done: oneshot::Sender<()>,
}

impl PendingHooks {
    /// The hooks as a future the harness polls: it runs them against `coordinator`, keeps a panic
    /// they raise for the test to raise again, and lets the dispatch that waits for them go on.
    #[doc(hidden)]
    pub fn into_future(self, coordinator: Coordinator) -> Pin<Box<dyn Future<Output = ()>>> {
        let Self { run, done } = self;
        Box::pin(async move {
            let outcome = AssertUnwindSafe(run(&coordinator)).catch_unwind().await;
            if let Err(payload) = outcome {
                lock(&coordinator.hooks().failed).get_or_insert(payload);
            }
            let _ = done.send(());
        })
    }
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
/// The dispatch that made the call holds it, and runs it once it settled the call.
#[must_use]
pub(crate) struct DueCall {
    scope_id: usize,
    name: String,
    within: Within,
    hooks: Vec<Arc<dyn CallHook>>,
}

/// One recorded publish and the hooks it runs, or nothing when no hook matched it.
#[must_use]
// Machinery the harness crate drives, hidden from the docs; it holds erased closures and
// futures that have no useful `Debug`.
#[allow(missing_debug_implementations)]
pub struct DuePublish(Option<PublishRun>);

/// The hooks one publish runs, and what their handle reads.
struct PublishRun {
    coordinator: Coordinator,
    target: PublishTarget,
    within: Within,
    hooks: Vec<Arc<dyn PublishHook>>,
}

impl DuePublish {
    /// No hook to run.
    pub(crate) const NONE: Self = Self(None);
}

impl Hooks {
    /// No hook yet, run where `runner` says.
    pub(crate) fn new(runner: HookRunner) -> Self {
        Self {
            runner,
            ..Self::default()
        }
    }

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

    /// The hooks registered on the calls of the subscription `name` of the broker at
    /// `scope_id`, or `None` when there are none.
    pub(crate) fn on_calls_of(
        &self,
        scope_id: usize,
        name: &str,
    ) -> Option<Vec<Arc<dyn CallHook>>> {
        if !self.registered.load(Ordering::SeqCst) {
            return None;
        }
        let hooks: Vec<_> = lock(&self.calls)
            .iter()
            .filter(|on| on.scope_id == scope_id && on.name == name)
            .map(|on| Arc::clone(&on.hook))
            .collect();
        (!hooks.is_empty()).then_some(hooks)
    }

    /// The hooks a publish just recorded runs: those registered on `target`, the record it went
    /// into.
    /// The target is built only once a hook is registered, so a test with none pays for no name.
    pub(crate) fn published(
        &self,
        coordinator: &Coordinator,
        target: impl FnOnce() -> PublishTarget,
        within: Within,
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
            within,
            hooks,
        }))
    }

    /// Stops every hook still running and waits until each is dropped, so nothing of a hook runs
    /// once the harness is shut down.
    async fn stop(&self) {
        self.stopped.cancel();
        self.running.close();
        self.running.wait().await;
    }

    /// Takes the hooks dispatches handed over since the last call, for the harness that polls the
    /// app to run beside it ([`PendingHooks::into_future`]).
    #[doc(hidden)]
    pub fn take_pending(&self) -> Vec<PendingHooks> {
        mem::take(&mut *lock(&self.pending))
    }

    /// Raises again, in the caller's task, the first panic a hook raised.
    #[doc(hidden)]
    pub fn raise_failure(&self) {
        let failed = lock(&self.failed).take();
        if let Some(payload) = failed {
            panic::resume_unwind(payload);
        }
    }
}

impl DueCall {
    /// The call recorded at `scope_id` and `name`, which `hooks` answer, read up to `within`.
    pub(crate) const fn new(
        scope_id: usize,
        name: String,
        within: Within,
        hooks: Vec<Arc<dyn CallHook>>,
    ) -> Self {
        Self {
            scope_id,
            name,
            within,
            hooks,
        }
    }
}

impl Coordinator {
    /// Runs the hooks of `calls`, which the caller's dispatch recorded and has settled.
    pub(crate) async fn run_calls(&self, calls: impl IntoIterator<Item = DueCall>) {
        for call in calls {
            run_hooks(self, move |coordinator| {
                Box::pin(async move {
                    for hook in &call.hooks {
                        let handle = SubscriberAssertions::within(
                            coordinator,
                            call.scope_id,
                            call.name.clone(),
                            call.within,
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
    #[doc(hidden)]
    pub async fn run(self) {
        let Some(PublishRun {
            coordinator,
            target,
            within,
            hooks,
        }) = self.0
        else {
            return;
        };
        run_hooks(&coordinator, move |coordinator| {
            Box::pin(async move {
                for hook in &hooks {
                    hook.call(coordinator, &target, within).await;
                }
            })
        })
        .await;
    }
}

/// Runs `hooks` to completion where the coordinator's harness runs them, and keeps a panic they
/// raise for the test to raise again ([`Hooks::raise_failure`]).
async fn run_hooks<Run>(coordinator: &Coordinator, hooks: Run)
where
    Run: for<'c> FnOnce(&'c Coordinator) -> HookFuture<'c> + Send + 'static,
{
    match coordinator.hooks().runner {
        HookRunner::Thread => on_hook_thread(coordinator, hooks).await,
        HookRunner::InPlace => in_place(coordinator, hooks).await,
    }
}

/// Hands `hooks` to the harness that polls the app, and waits until it ran them: the dispatch that
/// recorded the event waits here, and the test's step that polls the app polls the hooks beside
/// it ([`Hooks::take_pending`]).
async fn in_place<Run>(coordinator: &Coordinator, hooks: Run)
where
    Run: for<'c> FnOnce(&'c Coordinator) -> HookFuture<'c> + Send + 'static,
{
    let (done, finished) = oneshot::channel();
    lock(&coordinator.hooks().pending).push(PendingHooks {
        run: Box::new(hooks),
        done,
    });
    // The harness answers once the hooks are done, or drops the answer with the hooks when the
    // test ends; either way the dispatch goes on.
    let _ = finished.await;
}

/// Runs `hooks` to completion on a thread of their own, inside the test's runtime, and waits for
/// them. A panic they raise is kept for the test's task to raise again
/// ([`Hooks::raise_failure`]).
///
/// Why a thread: a hook's future is not `Send` (see [`HookFuture`]), and every task that sees an
/// event is. `Handle::block_on` on a plain thread keeps the hook on the test's runtime, its clock
/// included, and lets a paused clock advance while the hook waits on a timer.
///
/// The hooks are dropped where they wait as soon as the caller stops waiting for them (a shutdown
/// timeout aborted the dispatch that ran them) or the test ends ([`HookFailures::stop`]), so no
/// hook outlives the harness.
async fn on_hook_thread<Run>(coordinator: &Coordinator, hooks: Run)
where
    Run: for<'c> FnOnce(&'c Coordinator) -> HookFuture<'c> + Send + 'static,
{
    let runtime = Handle::current();
    let owned = coordinator.clone();
    let (mut done, finished) = oneshot::channel();
    let running = coordinator.hooks().running.token();
    let stopped = coordinator.hooks().stopped.clone();
    thread::spawn(move || {
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                tokio::select! {
                    () = hooks(&owned) => true,
                    () = done.closed() => false,
                    () = stopped.cancelled() => false,
                }
            })
        }));
        match outcome {
            Ok(true) => {
                let _ = done.send(Ok(()));
            }
            Ok(false) => {}
            Err(payload) => {
                let _ = done.send(Err(payload));
            }
        }
        // Released last, once the hooks' future and what it captured are dropped.
        drop(running);
    });
    if let Ok(Err(payload)) = finished.await {
        lock(&coordinator.hooks().failed).get_or_insert(payload);
    }
}

/// The harness's hold on the failures of its hooks.
///
/// It serves the ends of a test that drive nothing further: a shutdown raises what it holds once
/// the handlers are done, and a harness dropped without one raises it as it goes.
#[derive(Debug)]
pub struct HookFailures(Coordinator);

impl HookFailures {
    /// The hold on the failures of the hooks `coordinator` runs.
    #[must_use]
    pub const fn new(coordinator: Coordinator) -> Self {
        Self(coordinator)
    }

    /// Stops the hooks still running and waits until they are gone.
    pub async fn stop(&self) {
        self.0.hooks().stop().await;
    }

    /// Raises the failure a hook left, if any.
    pub fn raise(self) {
        self.0.hooks().raise_failure();
    }
}

// Why a destructor that panics: a test that ends by dropping the harness has no other place left
// where a hook's failed assertion can fail it, and passing a test whose check failed is the
// defect. It raises only a failure that is pending and only when no panic is unwinding already.
// The hooks still running are told to stop first; a destructor cannot wait for them to go.
impl Drop for HookFailures {
    fn drop(&mut self) {
        self.0.hooks().stopped.cancel();
        if !thread::panicking() {
            self.0.hooks().raise_failure();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("test hooks mutex poisoned")
}
