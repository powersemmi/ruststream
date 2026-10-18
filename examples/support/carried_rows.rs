//! A subscription whose deliveries carry a value, over the in-memory broker: the part a broker
//! crate ships when its client decodes a delivery itself (a database queue reads a row through
//! its driver). The handler takes the row, not the bytes it came from. The in-memory broker
//! carries bytes only, so this subscription reads each payload `id:name` as a row the way such a
//! client would; a payload that is no row carries no value.

use std::convert::Infallible;
use std::future::ready;
use std::str;

use futures::{Stream, StreamExt};
use ruststream::memory::{
    ConnectedMemoryBroker, LogMode, MemoryError, MemoryMessage, MemorySubscriber,
};
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
    #[must_use]
    pub(crate) fn new(id: u64, name: &str) -> Self {
        Self {
            id,
            name: name.to_owned(),
        }
    }

    fn read(payload: &[u8]) -> Option<Self> {
        let (id, name) = str::from_utf8(payload).ok()?.split_once(':')?;
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
    #[must_use]
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

impl<Log: LogMode> SubscriptionSource<ConnectedMemoryBroker<Log>> for Rows {
    type Subscriber = RowSubscriber<Log>;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> Result<RowSubscriber<Log>, MemoryError> {
        Ok(RowSubscriber(
            Subscribe::subscribe(connected, &self.name).await?,
        ))
    }
}

// One in-memory subject is both ends of the bus, so the name is where a copy goes.
impl<Log: LogMode> RedeliveryAddressed<ConnectedMemoryBroker<Log>> for Rows {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name.clone())))
    }
}

/// The bus's own subscription, with each delivery read as a row.
pub(crate) struct RowSubscriber<Log>(MemorySubscriber<Log>);

impl<Log: LogMode> Subscriber for RowSubscriber<Log> {
    type Message = RowDelivery<Log>;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<RowDelivery<Log>, Infallible>> + Send + '_ {
        self.0.stream().map(|delivery| {
            delivery.map(|inner| {
                let row = Row::read(inner.payload());
                RowDelivery { inner, row }
            })
        })
    }
}

/// One delivery and the row read out of it; `None` where the payload is no row.
pub(crate) struct RowDelivery<Log> {
    inner: MemoryMessage<Log>,
    row: Option<Row>,
}

impl<Log> Carries<Row> for RowDelivery<Log> {
    fn carried(&self) -> Option<&Row> {
        self.row.as_ref()
    }
}

impl<Log: LogMode> IncomingMessage for RowDelivery<Log> {
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
}
