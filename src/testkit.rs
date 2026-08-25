//! Fixtures shared by the crate's own unit tests.
//!
//! Some scenarios need the same scaffolding in several modules: a tracing subscriber that records
//! the fields of the events a handler emits, and a few helpers that drive the in-memory broker to
//! produce a batch. Copying them per module is how they drift, and one such copy had already
//! diverged into a different capture behaviour, so they live here once.
//!
//! This module is `#[cfg(test)]` and everything in it is `pub(crate)`: nothing here is part of the
//! published surface. The user-facing test harness is [`crate::testing`].

/// Records the fields of every tracing event emitted while the guard is alive, so a test can
/// assert on a diagnostic's content rather than on its mere existence.
///
/// Needs a tracing subscriber, hence the `logging` feature gate.
#[cfg(feature = "logging")]
pub(crate) mod log_capture {
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex};

    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing::subscriber::DefaultGuard;
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
    pub(crate) fn start() -> (Events, DefaultGuard) {
        let events: Events = Arc::new(Mutex::new(Vec::new()));
        let guard = tracing::subscriber::set_default(
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

/// Drives the in-memory broker for the batch handler tests: publish a few deliveries, then pull
/// them back as one batch.
#[cfg(all(feature = "memory", feature = "json"))]
pub(crate) mod batch {
    use futures::StreamExt;

    use crate::BatchSubscriber;
    use crate::memory::{MemoryBroker, MemoryMessage, MemorySubscriber};
    use crate::runtime::PublishExt;

    /// Publishes each number as its JSON encoding, so a `Decoded<u32>` handler sees them.
    pub(crate) async fn publish_numbers(broker: &MemoryBroker, name: &str, numbers: &[u32]) {
        let publisher = broker.publisher();
        for n in numbers {
            publisher
                .raw(&serde_json::to_vec(n).unwrap())
                .to(name)
                .publish()
                .await
                .unwrap();
        }
    }

    /// Publishes raw payloads, for the cases where the bytes must not be valid JSON.
    pub(crate) async fn publish_payloads(broker: &MemoryBroker, name: &str, payloads: &[&[u8]]) {
        let publisher = broker.publisher();
        for payload in payloads {
            publisher.raw(payload).to(name).publish().await.unwrap();
        }
    }

    /// Pulls the next batch off the subscriber.
    pub(crate) async fn pull_batch(sub: &mut MemorySubscriber) -> Vec<MemoryMessage> {
        let mut stream = std::pin::pin!(sub.batches());
        stream.next().await.unwrap().unwrap()
    }
}
