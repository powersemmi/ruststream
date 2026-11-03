//! What the logger tests read the framework's diagnostics with: a `tracing` subscriber whose level
//! the test moves, and a `log` logger that keeps its records.

// A suite uses the part of this module it needs.
#![allow(dead_code)]

use std::fmt::{Debug, Write as _};
use std::mem::take;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use log::{Log, Metadata, Record};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::{Event, Level, Metadata as TracingMetadata, Subscriber};

/// The `tracing` events of the framework a scope saw, filtered at `level`.
#[derive(Clone)]
pub(crate) struct Traced {
    pub(crate) level: Arc<AtomicU8>,
    lines: Arc<Mutex<Vec<String>>>,
}

impl Traced {
    pub(crate) fn at(level: Level) -> Self {
        Self {
            level: Arc::new(AtomicU8::new(rank(level))),
            lines: Arc::default(),
        }
    }

    pub(crate) fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Orders levels so that a larger number is more verbose.
pub(crate) fn rank(level: Level) -> u8 {
    match level {
        Level::ERROR => 1,
        Level::WARN => 2,
        Level::INFO => 3,
        Level::DEBUG => 4,
        Level::TRACE => 5,
    }
}

/// Writes an event's fields as ` name=value`.
struct Fields(String);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            let _ = write!(self.0, "{value:?}");
        } else {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }
}

impl Subscriber for Traced {
    fn enabled(&self, metadata: &TracingMetadata<'_>) -> bool {
        rank(*metadata.level()) <= self.level.load(Ordering::Relaxed)
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &SpanRecord<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut text = Fields(String::new());
        event.record(&mut text);
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!(
                "{} {}: {}",
                event.metadata().level(),
                event.metadata().target(),
                text.0
            ));
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// The `log` records of the process, as text.
pub(crate) struct Records(pub(crate) Mutex<Vec<String>>);

impl Log for Records {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!(
                "{} {}: {}",
                record.level(),
                record.target(),
                record.args()
            ));
    }

    fn flush(&self) {}
}

pub(crate) static RECORDS: Records = Records(Mutex::new(Vec::new()));

/// The records written since the last call.
pub(crate) fn take_records() -> Vec<String> {
    take(&mut *RECORDS.0.lock().unwrap_or_else(PoisonError::into_inner))
}
