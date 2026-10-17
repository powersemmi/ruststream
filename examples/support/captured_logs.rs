//! The log lines a test reads back: a `tracing` subscriber that writes formatted events into a
//! buffer instead of the terminal, installed for the current thread only.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use tracing::Level;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::fmt::MakeWriter;

/// A buffer the installed subscriber writes into; clones share it.
#[derive(Clone, Default)]
pub(crate) struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    /// Installs a subscriber that writes every event up to `level` into this buffer, for as long
    /// as the guard lives. The harness drives the app on the test's own thread, so a thread-local
    /// subscriber sees every event the handlers and their layers emit.
    pub(crate) fn install(&self, level: Level) -> DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(self.clone())
            .with_ansi(false)
            .with_max_level(level)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    /// Everything written so far.
    pub(crate) fn text(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
