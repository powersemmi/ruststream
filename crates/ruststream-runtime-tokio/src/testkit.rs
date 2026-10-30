//! Fixtures shared by the crate's own unit tests: a tracing subscriber that records the fields of
//! the events a dispatch loop emits.
//!
//! This module is `#[cfg(test)]` and everything in it is `pub(crate)`: nothing here is part of the
//! published surface.

/// Records the fields of every tracing event emitted while the guard is alive, so a test can
/// assert on a diagnostic's content rather than on its mere existence.
///
/// It compiles where its callers do: with a tracing subscriber and the in-memory broker their
/// modules need.
pub(crate) mod log_capture {
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex, Once};

    use tracing::field::{Field, Visit};
    use tracing::subscriber::{DefaultGuard, Interest};
    use tracing::{Metadata, Subscriber, span, subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt as _};

    /// The captured events, each one a map from field name to its rendered value.
    pub(crate) type Events = Arc<Mutex<Vec<HashMap<String, String>>>>;

    #[derive(Default)]
    struct FieldGrab(HashMap<String, String>);

    impl Visit for FieldGrab {
        // Without this, a string field arrives through `record_debug` and is captured in its
        // quoted Debug form, which no assertion is written against.
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().to_owned(), value.to_string());
        }

        // First writer wins, so a value already recorded in its typed form is not overwritten by
        // a later Debug rendering of the same field.
        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.0
                .entry(field.name().to_owned())
                .or_insert_with(|| format!("{value:?}"));
        }
    }

    struct Capture(Events);

    impl<S: Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut grab = FieldGrab::default();
            event.record(&mut grab);
            self.0.lock().unwrap().push(grab.0);
        }
    }

    /// Installs the capturing subscriber for the current thread. Events are collected until the
    /// returned guard is dropped.
    /// The process-wide subscriber: it records nothing, and keeps every callsite's interest at
    /// "sometimes". Without it the only global dispatcher is the no-op one, and a callsite a test
    /// on another thread hits first, while no capture is installed anywhere, can be cached as
    /// never interested: a capture installed a moment later then misses that event.
    struct Undecided;

    impl Subscriber for Undecided {
        fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
            Interest::sometimes()
        }

        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            false
        }

        fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }

        fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

        fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

        fn event(&self, _event: &tracing::Event<'_>) {}

        fn enter(&self, _span: &span::Id) {}

        fn exit(&self, _span: &span::Id) {}
    }

    pub(crate) fn start() -> (Events, DefaultGuard) {
        static UNDECIDED: Once = Once::new();
        UNDECIDED.call_once(|| {
            // Another test binary's global subscriber is not this process's concern: a failure
            // here means one is already installed, which keeps the interest open just as well.
            let _ = subscriber::set_global_default(Undecided);
        });
        let events: Events = Arc::new(Mutex::new(Vec::new()));
        let guard = subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&events))),
        );
        (events, guard)
    }

    /// Returns the fields of the first captured event whose message is `message`.
    ///
    /// # Panics
    ///
    /// Panics when no such event was captured.
    pub(crate) fn find(events: &Events, message: &str) -> HashMap<String, String> {
        let captured = events.lock().unwrap();
        captured
            .iter()
            .find(|fields| fields.get("message").is_some_and(|m| m == message))
            .cloned()
            .unwrap_or_else(|| panic!("no `{message}` event was emitted"))
    }
}
