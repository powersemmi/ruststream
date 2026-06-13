//! [`TestClient`] for the in-memory broker.
//!
//! The in-process broker doubles as its own test client: `start` is a plain constructor,
//! `expect_published` reads the broker's published-message log, and the rest delegate to the
//! inherent broker operations. There is no separate in-memory simulation to stand up.

use std::{convert::Infallible, sync::Arc, time::Duration};

use tokio::time::timeout;

use super::{MemoryBroker, MemoryPublisher, MemorySubscriber};
use crate::{Broker, OutgoingMessage, Publisher, RawMessage, testing::TestClient};

impl TestClient for MemoryBroker {
    type Broker = Self;
    type Subscriber = MemorySubscriber;
    type Publisher = MemoryPublisher;
    type Error = Infallible;

    async fn start() -> Result<Self, Self::Error> {
        Ok(Self::new())
    }

    fn broker(&self) -> &Self::Broker {
        self
    }

    async fn publish(&self, name: &str, payload: &[u8]) -> Result<(), Self::Error> {
        let publisher = Self::publisher(self);
        publisher.publish(OutgoingMessage::new(name, payload)).await
    }

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        Ok(Self::subscribe(self, name))
    }

    async fn publisher(&self) -> Result<Self::Publisher, Self::Error> {
        Ok(Self::publisher(self))
    }

    async fn expect_published(
        &self,
        name: &str,
        count: usize,
        timeout_duration: Duration,
    ) -> Result<Vec<RawMessage>, Self::Error> {
        let name_for_wait = name.to_owned();
        let name_for_fallback = name_for_wait.clone();
        let state = Arc::clone(&self.state);

        let wait = async move {
            loop {
                {
                    let log = state
                        .published
                        .lock()
                        .expect("memory broker mutex poisoned");
                    if let Some(messages) = log.get(&name_for_wait) {
                        if messages.len() >= count {
                            return messages.iter().take(count).cloned().collect::<Vec<_>>();
                        }
                    }
                }
                state.notify.notified().await;
            }
        };

        let result = timeout(timeout_duration, wait).await;
        let messages = result.unwrap_or_else(|_| {
            self.state
                .published
                .lock()
                .expect("memory broker mutex poisoned")
                .get(&name_for_fallback)
                .map(|m| m.iter().take(count).cloned().collect())
                .unwrap_or_default()
        });
        Ok(messages)
    }

    async fn shutdown(self) -> Result<(), Self::Error> {
        <Self as Broker>::shutdown(&self).await
    }
}
