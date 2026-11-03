//! Where the runner starts a task of the core's runtime.
//!
//! Every task the runner spawns goes through [`spawn`]. In a build without the `testing` feature it
//! is `compio::runtime::spawn` itself. With the feature, a thread that holds a
//! [`Quiescence`](crate::testing::Quiescence) has its tasks followed, so the harness can tell a
//! reaction that has run out of work from one that is still going.

use compio::runtime::{self, JoinHandle};

#[cfg(feature = "testing")]
use crate::quiescence::follow;

/// Spawns `work` on the runtime of the calling thread.
///
/// # Panics
///
/// Panics when the thread runs no compio runtime, as `compio::runtime::spawn` does.
#[track_caller]
pub(crate) fn spawn<Work>(work: Work) -> JoinHandle<Work::Output>
where
    Work: Future + 'static,
{
    #[cfg(feature = "testing")]
    let work = match follow(work) {
        Ok(followed) => return runtime::spawn(followed),
        Err(work) => work,
    };
    runtime::spawn(work)
}
