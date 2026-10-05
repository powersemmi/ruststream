//! A subscription whose deliveries carry a typed value, over the in-memory broker.
//!
//! This is what a broker whose client decodes a delivery itself looks like to the runtime: a
//! database queue reads a row through its driver, and a handler wants that row, not the bytes it
//! came from. The in-memory broker carries bytes only, so the subscription here reads each
//! payload as a row the way such a client would, and a payload that is no row stands for a
//! claimed id whose row is gone: the delivery carries no value.
//!
//! A row is written `id:name` (`7:alice`). Each test binary that names this module uses part of
//! it, hence the `dead_code` allowance.
#![allow(dead_code)]

use std::future::{Future, ready};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::memory::{ConnectedMemoryBroker, MemoryError, MemoryMessage, MemorySubscriber};
use ruststream::runtime::{Input, IntoSource, SoloCarried};
use ruststream::{
    AckError, AddressedCopies, Carries, HeaderMap, IncomingMessage, RedeliveryAddress,
    RedeliveryAddressed, Subscribe, Subscriber, SubscriptionSource,
};

/// The value a delivery carries: the service's own struct, with no serde model.
#[derive(Debug, Clone, PartialEq, Eq, schemars::JsonSchema)]
pub(crate) struct Row {
    pub(crate) id: u64,
    pub(crate) name: String,
}

impl Row {
    pub(crate) fn new(id: u64, name: &str) -> Self {
        Self {
            id,
            name: name.to_owned(),
        }
    }

    /// The payload a test publishes for this row.
    pub(crate) fn payload(&self) -> String {
        format!("{}:{}", self.id, self.name)
    }

    /// Reads a payload the way the client reads a row; anything else is a row that is gone.
    fn read(payload: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(payload).ok()?;
        let (id, name) = text.split_once(':')?;
        Some(Self::new(id.parse().ok()?, name))
    }
}

// What a broker crate's derive writes for its row type: the type rides the carried lane.
impl Input for Row {
    type Axis = SoloCarried<Self>;
}

/// The subscription descriptor: one subject of the in-memory bus, read as rows.
#[derive(Debug, Clone)]
pub(crate) struct Rows {
    name: String,
}

impl Rows {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

// What lets a body mount on the descriptor by value, `subscriber(Rows::new(..), body)`.
impl IntoSource for Rows {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for Rows {
    type Subscriber = RowSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<RowSubscriber, MemoryError> {
        Ok(RowSubscriber(
            Subscribe::subscribe(connected, &self.name).await?,
        ))
    }
}

impl RedeliveryAddressed<ConnectedMemoryBroker> for Rows {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name.clone())))
    }
}

/// The subscription itself: the bus's own, with each delivery read as a row.
pub(crate) struct RowSubscriber(MemorySubscriber);

impl Subscriber for RowSubscriber {
    type Message = RowDelivery;
    type Error = std::convert::Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<RowDelivery, Self::Error>> + Send + '_ {
        self.0
            .stream()
            .map(|delivery| delivery.map(RowDelivery::read))
    }
}

/// One delivery and the row read out of it.
pub(crate) struct RowDelivery {
    inner: MemoryMessage,
    row: Option<Row>,
}

impl RowDelivery {
    fn read(inner: MemoryMessage) -> Self {
        let row = Row::read(inner.payload());
        Self { inner, row }
    }
}

impl Carries<Row> for RowDelivery {
    fn carried(&self) -> Option<&Row> {
        self.row.as_ref()
    }
}

impl IncomingMessage for RowDelivery {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    fn redelivery_count(&self) -> Option<u64> {
        self.inner.redelivery_count()
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.ack()
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.nack(requeue)
    }

    fn supports_nack_after(&self) -> bool {
        self.inner.supports_nack_after()
    }

    fn nack_after(self, delay: Duration) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.nack_after(delay)
    }
}
