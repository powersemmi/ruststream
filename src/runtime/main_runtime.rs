//! [`MainRuntime`]: the app's runtime as a handler reaches it, and the explicit way to send work
//! there from a dedicated thread.

use std::future::Future;

use tokio::runtime::Handle;
use tokio::task::{JoinError, JoinHandle};

/// The runtime the app runs on, as a handler reaches it: `Ctx(main): Ctx<MainRuntime>` as a
/// parameter, or [`Context::main_runtime`](super::Context::main_runtime).
///
/// A handler on dedicated threads (`threads(n)`) runs on its own thread, and so does everything
/// it leaves behind: a plain `tokio::spawn`, a timer, an `and_after` continuation, an `after(..)`
/// hook and the timer of a `retry_after` copy. `MainRuntime` is the one explicit way to send work
/// to the app's runtime instead: [`spawn`](Self::spawn) starts it there and moves on,
/// [`run`](Self::run) runs it there and hands its output back. What is sent must be `Send`.
/// A plain `tokio::spawn` on a dedicated thread lives as long as that thread: a task still
/// pending when the subscription ends is dropped with the thread's runtime.
/// On any other placement the handler already runs on this runtime.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::prelude::*;
///
/// #[derive(serde::Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// async fn notify(_id: u64) {}
///
/// async fn lookup(id: u64) -> u64 {
///     id * 2
/// }
///
/// #[subscriber("jobs", threads(4))]
/// async fn index(job: &Job, Ctx(main): Ctx<MainRuntime>) -> HandlerOutcome {
///     // The notification leaves the thread; the handler does not wait for it.
///     main.spawn(notify(job.id));
///     // The lookup runs on the app's runtime, and its answer comes back here.
///     match main.run(lookup(job.id)).await {
///         Ok(_answer) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MainRuntime(Handle);

impl MainRuntime {
    /// The runtime the calling task runs on: a subscription captures it when it opens, inside
    /// the app's startup.
    pub(crate) fn current() -> Self {
        Self(Handle::current())
    }

    /// Wraps `handle`.
    #[cfg(test)]
    pub(crate) const fn new(handle: Handle) -> Self {
        Self(handle)
    }

    /// Starts `future` as a task of the app's runtime and returns its handle; the handler does
    /// not wait for it unless it awaits the handle.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// async fn notify(_id: u64) {}
    ///
    /// async fn handle<M: IncomingMessage>(_msg: &M, ctx: &mut Context<'_>) -> HandlerOutcome {
    ///     ctx.main_runtime().spawn(notify(7));
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    pub fn spawn<Work>(&self, future: Work) -> JoinHandle<Work::Output>
    where
        Work: Future + Send + 'static,
        Work::Output: Send + 'static,
    {
        self.0.spawn(future)
    }

    /// Runs `future` as a task of the app's runtime and resolves to its output.
    ///
    /// # Errors
    ///
    /// Returns the [`JoinError`] of a task that panicked, or that the runtime cancelled because
    /// it is shutting down.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned future stops waiting, not the work: the task runs to its end on the
    /// app's runtime.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// async fn lookup(id: u64) -> u64 {
    ///     id * 2
    /// }
    ///
    /// async fn handle<M: IncomingMessage>(_msg: &M, ctx: &mut Context<'_>) -> HandlerOutcome {
    ///     match ctx.main_runtime().run(lookup(7)).await {
    ///         Ok(14) => HandlerOutcome::ack(),
    ///         _ => HandlerOutcome::retry(),
    ///     }
    /// }
    /// ```
    pub fn run<Work>(
        &self,
        future: Work,
    ) -> impl Future<Output = Result<Work::Output, JoinError>> + Send + 'static
    where
        Work: Future + Send + 'static,
        Work::Output: Send + 'static,
    {
        self.0.spawn(future)
    }

    /// The Tokio handle of the app's runtime, for what the two methods above do not cover
    /// (`block_on` from a thread outside any runtime, the runtime's metrics).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::IncomingMessage;
    /// use ruststream::runtime::{Context, HandlerOutcome};
    ///
    /// async fn handle<M: IncomingMessage>(_msg: &M, ctx: &mut Context<'_>) -> HandlerOutcome {
    ///     let _workers = ctx.main_runtime().as_handle().metrics().num_workers();
    ///     HandlerOutcome::ack()
    /// }
    /// ```
    #[must_use]
    pub const fn as_handle(&self) -> &Handle {
        &self.0
    }
}
