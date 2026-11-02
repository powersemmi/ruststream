//! The dedicated threads behind `threads(n)`: a subscription's deliveries handled on `n` threads
//! of its own, each running a current-thread runtime, fed through the rings of
//! [`ruststream_runtime::open_rings`] (their hand-off is described there).
//!
//! This half starts the threads, runs each thread's work on its runtime, and joins them; the
//! subscription's loop stays on the app's runtime and reads the stream through the rings' feed.

use std::fmt::Display;
use std::io;
use std::thread;

use futures::Stream;
use thiserror::Error;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot;
use tokio::task::coop::consume_budget;
use tokio_util::task::TaskTracker;
use ruststream_core::diag::error;

use ruststream_runtime::{Feed, open_rings};

use super::Shutdown;

/// A thread of a `threads(n)` subscription could not be started: the subscription does not open.
#[derive(Debug, Error)]
#[error("subscription `{subscription}` could not start its dedicated thread {index}")]
pub(crate) struct StartThreadError {
    subscription: String,
    index: usize,
    #[source]
    source: io::Error,
}

/// One started thread. Dropping it cancels the thread's work where it stands, the way aborting a
/// task would; joining it waits until the thread has let go of its runtime.
struct Member {
    done: oneshot::Receiver<()>,
    abort: Option<oneshot::Sender<()>>,
}

impl Member {
    async fn join(&mut self) {
        // The abort side stays held until the thread reports, so joining cancels nothing.
        if (&mut self.done).await.is_err() {
            error!(target: "ruststream::dispatch", "a dedicated thread ended abnormally");
        }
        self.abort = None;
    }
}

/// The threads a loop started. Dropping it cancels them: a loop aborted by the shutdown timeout
/// takes its threads' work down with it.
struct Crew(Vec<Member>);

impl Crew {
    async fn join(&mut self) {
        for member in &mut self.0 {
            member.join().await;
        }
    }
}

/// Starts thread `index` of `subscription` on `runtime`, running what `work` builds there. The
/// work's future is built on the thread and never leaves it.
fn start_thread<Work, Fut>(
    subscription: &str,
    index: usize,
    runtime: Runtime,
    work: Work,
) -> Result<Member, StartThreadError>
where
    Work: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let (done_side, done) = oneshot::channel();
    let (abort, mut aborted) = oneshot::channel::<()>();
    // A thread name is a C string: a subscription name with a NUL in it would fail the spawn.
    let name = format!("{}/{index}", subscription.replace('\0', ""));
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            runtime.block_on(async move {
                let work = work();
                tokio::select! {
                    biased;
                    _ = &mut aborted => {}
                    () = work => {}
                }
            });
            // The runtime goes before the report, so a joined thread has let go of everything it
            // spawned.
            drop(runtime);
            let _ = done_side.send(());
        })
        .map_err(|source| StartThreadError {
            subscription: subscription.to_owned(),
            index,
            source,
        })?;
    Ok(Member {
        done,
        abort: Some(abort),
    })
}

/// The rings of a `threads(n)` subscription and the threads that drain them, started.
pub(super) struct Threads<Item> {
    feed: Feed<Item>,
    crew: Crew,
}

impl<Item: Send + 'static> Threads<Item> {
    /// Starts `count` threads for `subscription`, each draining its ring through the handler
    /// `each` builds for it, handed the thread's own tracker: what the handler spawns through it
    /// the thread waits for before it ends. Everything that can fail happens here, before the
    /// subscription's loop runs: a runtime or a thread that cannot start refuses the subscription.
    pub(super) fn start<Each, Handle>(
        subscription: &str,
        count: usize,
        keyed: bool,
        mut each: Each,
    ) -> Result<Self, StartThreadError>
    where
        Each: FnMut(TaskTracker) -> Handle,
        Handle: AsyncFnMut(Item) + Send + 'static,
    {
        let mut crew = Crew(Vec::with_capacity(count));
        let mut index = 0;
        let feed = open_rings(count, keyed, |thread| {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|source| StartThreadError {
                    subscription: subscription.to_owned(),
                    index,
                    source,
                })?;
            let own = TaskTracker::new();
            let mut handle = each(own.clone());
            crew.0.push(start_thread(
                subscription,
                index,
                runtime,
                async move || {
                    // A thread whose ring never stays empty never parks, and a handler that
                    // completes without waiting never yields: without the budget, an `and_after`
                    // continuation, an `after(..)` hook or a `retry_after` timer on this runtime
                    // would wait for the whole burst. Each delivery takes one unit of the task's
                    // cooperative budget, and the thread yields once it runs out.
                    thread.work(async move |item| {
                        handle(item).await;
                        consume_budget().await;
                    })
                    .await;
                    // The ring is drained; what its deliveries left behind (a continuation, a
                    // hook, a retry timer) finishes here before the thread and its runtime go.
                    own.close();
                    own.wait().await;
                },
            )?);
            index += 1;
            Ok(())
        })?;
        Ok(Self { feed, crew })
    }

    /// Reads `stream` into the rings until it ends or `shutdown` fires, then lets the threads
    /// drain what their rings hold and waits for them.
    pub(super) async fn feed<Items, Failure>(
        mut self,
        stream: Items,
        name: &str,
        shutdown: &Shutdown,
        key: fn(&Item) -> Option<&[u8]>,
    ) where
        Items: Stream<Item = Result<Item, Failure>>,
        Failure: Display,
    {
        self.feed.read(stream, name, shutdown, key).await;
        self.feed.let_go();
        self.crew.join().await;
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::hint::spin_loop;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    use futures::StreamExt;
    use futures::stream;

    use super::*;

    /// The busy thread's handler: the first item spawns a task on the thread's own runtime, every
    /// later one records the first item that saw it run.
    fn handle_busy(item: u32, ran: &Arc<AtomicBool>, first_seen: &AtomicU32) {
        if item == 0 {
            let ran = Arc::clone(ran);
            tokio::spawn(async move { ran.store(true, Ordering::SeqCst) });
        } else if ran.load(Ordering::SeqCst) {
            let _ = first_seen.compare_exchange(u32::MAX, item, Ordering::SeqCst, Ordering::SeqCst);
        }
        // Computation that never waits, long enough for the loop to refill the ring: the thread
        // never finds it empty, so it never parks.
        let until = Instant::now() + Duration::from_micros(50);
        while Instant::now() < until {
            spin_loop();
        }
    }

    /// A thread that never runs out of work still lets the tasks of its own runtime run: a task
    /// its handler spawns runs within a bounded run of deliveries, not after the whole burst.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_busy_thread_lets_its_own_tasks_run() {
        const ITEMS: u32 = 1000;
        let ran = Arc::new(AtomicBool::new(false));
        let first_seen = Arc::new(AtomicU32::new(u32::MAX));
        let threads = Threads::start("busy", 1, false, |_own| {
            let (ran, first_seen) = (Arc::clone(&ran), Arc::clone(&first_seen));
            async move |item: u32| handle_busy(item, &ran, &first_seen)
        })
        .expect("the threads start");
        let items = stream::iter(0..ITEMS).map(Ok::<_, Infallible>);
        threads
            .feed(items, "busy", &Shutdown::new(), |_| None)
            .await;
        let seen = first_seen.load(Ordering::SeqCst);
        assert!(
            seen < ITEMS / 2,
            "the spawned task ran only at delivery {seen} of {ITEMS}"
        );
    }
}
