//! The log subscriber the suites of this binary share, for the tests that read what the runtime
//! logs.
//!
//! The global slot takes one subscriber per process, so every suite that reads the log goes
//! through this one, installed once, instead of installing its own. It is process-wide rather
//! than a per-thread default: tracing caches a callsite's interest globally, and a thread with no
//! subscriber of its own could register a callsite as never enabled and silence it for every
//! test. A global subscriber is the same on every thread, so the cached interest is always the
//! right one; only where its lines go varies.
//!
//! Every event lands in the shared log, which every thread writes to, and in the capture of the
//! thread that wrote it, when that thread installed one. Each event is appended as a unit, so
//! lines written by tests running side by side never interleave.

// A suite uses the part of this module it needs: a capture of its own thread, or the shared log.
#![allow(dead_code)]

use std::cell::RefCell;
use std::io;
use std::sync::{Arc, Mutex, Once, PoisonError};

use tracing::Level;
use tracing::subscriber::set_global_default;
use tracing_subscriber::fmt::MakeWriter;

/// The lines every thread wrote since the subscriber was installed.
static SHARED: Mutex<Vec<u8>> = Mutex::new(Vec::new());

thread_local! {
    /// The capture the current thread's log lines go to, if a test installed one.
    static CURRENT: RefCell<Option<Captured>> = const { RefCell::new(None) };
}

/// The log lines written on one thread while its [`Installed`] guard lives.
#[derive(Clone, Debug, Default)]
pub(crate) struct Captured(Arc<Mutex<Vec<u8>>>);

/// Clears the current thread's capture when dropped.
pub(crate) struct Installed;

impl Drop for Installed {
    fn drop(&mut self) {
        CURRENT.with(|current| current.borrow_mut().take());
    }
}

impl Captured {
    /// Routes the current thread's log lines into this capture until the guard drops.
    pub(crate) fn install(&self) -> Installed {
        install_subscriber();
        CURRENT.with(|current| *current.borrow_mut() = Some(self.clone()));
        Installed
    }

    /// What the thread logged, as text.
    pub(crate) fn text(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }

    /// The lines that contain `needle`.
    pub(crate) fn lines_with(&self, needle: &str) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(needle))
            .map(str::to_owned)
            .collect()
    }
}

/// Installs the subscriber, if no test did yet, so the shared log records from here on.
pub(crate) fn record_shared() {
    install_subscriber();
}

/// What every thread logged since the subscriber was installed, as text. Other tests write here
/// too, so a test looks for the lines that carry its own markers.
pub(crate) fn shared_text() -> String {
    let bytes = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
    String::from_utf8_lossy(&bytes).into_owned()
}

fn install_subscriber() {
    static GLOBAL: Once = Once::new();
    GLOBAL.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(Destinations)
            .with_ansi(false)
            .with_max_level(Level::DEBUG)
            .finish();
        set_global_default(subscriber).expect("no other global subscriber in this test binary");
    });
}

/// The global subscriber's writer: the shared log, and the current thread's capture.
struct Destinations;

impl<'a> MakeWriter<'a> for Destinations {
    type Writer = EventWriter;

    fn make_writer(&'a self) -> Self::Writer {
        EventWriter {
            thread: CURRENT.with(|current| current.borrow().clone()),
            event: Vec::new(),
        }
    }
}

/// One event's bytes, appended to its destinations as a unit when the formatter is done with it:
/// the formatter writes an event in several calls.
struct EventWriter {
    thread: Option<Captured>,
    event: Vec<u8>,
}

impl io::Write for EventWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.event.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        SHARED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(&self.event);
        if let Some(Captured(bytes)) = &self.thread {
            bytes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(&self.event);
        }
    }
}
