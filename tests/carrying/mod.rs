//! A subscription whose deliveries carry a typed value, over the in-memory broker.
//!
//! This is what a broker whose client decodes a delivery itself looks like to the runtime: a
//! database queue reads a row through its driver, and a handler wants that row, not the bytes it
//! came from. The in-memory broker carries bytes only, so the subscription here reads each
//! payload as a row the way such a client would, and a payload that is no row stands for a
//! claimed id whose row is gone: the delivery carries no value.
//!
//! A batch holds its rows in one vector and its deliveries in another, the way a client that
//! fetches a page of rows does, and lends the rows as one slice. Deliveries whose row is gone go
//! last, past the end of that slice.
//!
//! A row is written `id:name` (`7:alice`). A [`Fault`] breaks the subscription in one way, for the
//! conformance checks that must fail against it. Each test binary that names this module uses
//! part of it, hence the `dead_code` allowance.
#![allow(dead_code)]

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::memory::{
    ConnectedMemoryBroker, Discarding, LogMode, MemoryError, MemoryMessage, MemorySeeker,
    MemorySubscriber, Retaining,
};
use ruststream::runtime::{Input, IntoSource, SoloCarried};
use ruststream::{
    AckError, AddressedCopies, BatchSubscriber, BuildBatchContext, Carries, CarriesBatch, Field,
    HeaderMap, IncomingMessage, RedeliveryAddress, RedeliveryAddressed, Seekable, Subscribe,
    Subscriber, SubscriptionSource,
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

/// One way a broker can break the carried lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Fault {
    /// Behaves.
    #[default]
    None,
    /// Every delivery lends the first row the subscription read: a value cached past its
    /// delivery.
    StaleValue,
    /// A delivery whose row is gone lends a placeholder row instead of nothing.
    PlaceholderForGone,
    /// A page lends its rows in the reverse order of its deliveries.
    ReversedPage,
    /// A page lends one row more than it delivers.
    ExtraRow,
}

/// The subscription descriptor: one subject of the in-memory bus, read as rows.
#[derive(Debug, Clone)]
pub(crate) struct Rows {
    name: String,
    fault: Fault,
}

impl Rows {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fault: Fault::None,
        }
    }

    /// The same subscription, broken in one way.
    pub(crate) fn faulty(self, fault: Fault) -> Self {
        Self { fault, ..self }
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
        Ok(RowSubscriber {
            inner: Subscribe::subscribe(connected, &self.name).await?,
            fault: self.fault,
            first: None,
        })
    }
}

impl<Log: LogMode> RedeliveryAddressed<ConnectedMemoryBroker<Log>> for Rows {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name.clone())))
    }
}

/// The subscription itself: the bus's own, with each delivery read as a row.
pub(crate) struct RowSubscriber<Log = Discarding> {
    inner: MemorySubscriber<Log>,
    fault: Fault,
    /// The first row read, which a stale subscription keeps lending.
    first: Option<Row>,
}

impl<Log: LogMode> RowSubscriber<Log> {
    /// Reads one delivery as a row, the way the subscription's fault has it.
    fn read(fault: Fault, first: &mut Option<Row>, inner: MemoryMessage<Log>) -> RowDelivery<Log> {
        let row = match (fault, Row::read(inner.payload())) {
            (Fault::StaleValue, Some(row)) => Some(first.get_or_insert(row).clone()),
            (Fault::PlaceholderForGone, None) => Some(Row::new(0, "placeholder")),
            (_, row) => row,
        };
        RowDelivery { inner, row }
    }
}

impl<Log: LogMode> Subscriber for RowSubscriber<Log> {
    type Message = RowDelivery<Log>;
    type Error = std::convert::Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<RowDelivery<Log>, Self::Error>> + Send + '_ {
        let fault = self.fault;
        let first = &mut self.first;
        self.inner
            .stream()
            .map(move |delivery| delivery.map(|inner| Self::read(fault, first, inner)))
    }
}

/// A page of rows: the bus's own batch, each delivery read, the rows kept apart from the
/// deliveries that settle them.
impl<Log: LogMode> BatchSubscriber for RowSubscriber<Log> {
    type Batch = RowBatch<Log>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<RowBatch<Log>, Self::Error>> + Send + '_ {
        let fault = self.fault;
        self.inner
            .batches(size)
            .map(move |batch| batch.map(|page| RowBatch::read(page, fault)))
    }
}

// The bus replays its log on a seek, so a test can hand a subscription a whole page at once.
impl Seekable for RowSubscriber<Retaining> {
    type Seeker = MemorySeeker;

    fn seeker(&self) -> MemorySeeker {
        self.inner.seeker()
    }
}

/// One delivery and the row read out of it; `None` where the row is gone, and on a delivery a
/// batch handed its row to the batch's own vector.
pub(crate) struct RowDelivery<Log = Discarding> {
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

    fn supports_nack_after(&self) -> bool {
        self.inner.supports_nack_after()
    }

    fn nack_after(self, delay: Duration) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.nack_after(delay)
    }
}

/// One page: the rows in one vector, the deliveries that settle them in another, in the same
/// order, and the deliveries whose row is gone after them.
pub(crate) struct RowBatch<Log = Discarding> {
    rows: Vec<Row>,
    deliveries: Vec<RowDelivery<Log>>,
}

impl<Log: LogMode> RowBatch<Log> {
    fn read(page: Vec<MemoryMessage<Log>>, fault: Fault) -> Self {
        let mut rows = Vec::with_capacity(page.len());
        let mut deliveries = Vec::with_capacity(page.len());
        let mut gone = Vec::new();
        for inner in page {
            match Row::read(inner.payload()) {
                Some(row) => {
                    rows.push(row);
                    deliveries.push(RowDelivery { inner, row: None });
                }
                None => gone.push(RowDelivery { inner, row: None }),
            }
        }
        deliveries.extend(gone);
        match fault {
            Fault::ReversedPage => rows.reverse(),
            Fault::ExtraRow => rows.push(Row::new(0, "extra")),
            _ => {}
        }
        Self { rows, deliveries }
    }
}

impl<Log> CarriesBatch<Row> for RowBatch<Log> {
    fn carried(&self) -> &[Row] {
        &self.rows
    }
}

impl<Log> IntoIterator for RowBatch<Log> {
    type Item = RowDelivery<Log>;
    type IntoIter = std::vec::IntoIter<RowDelivery<Log>>;

    fn into_iter(self) -> Self::IntoIter {
        self.deliveries.into_iter()
    }
}

/// What a page's handler reads of the page itself: how many rows it fetched. On the carried lane
/// the batch context is built from the batch, because the batch stays whole while the body reads
/// its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PageContext {
    pub(crate) rows: usize,
}

impl<Log> BuildBatchContext<RowBatch<Log>> for PageContext {
    fn build(page: &RowBatch<Log>) -> Self {
        Self {
            rows: page.rows.len(),
        }
    }
}

// The copies a retry publishes build their context from one delivery, which knows no page.
impl<Log> BuildBatchContext<RowDelivery<Log>> for PageContext {
    fn build(_one: &RowDelivery<Log>) -> Self {
        Self { rows: 1 }
    }
}

/// The key a handler reads the page's row count with.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PageRows;

impl Field<PageContext> for PageRows {
    type Value<'a> = usize;

    fn get(self, src: &PageContext) -> usize {
        src.rows
    }
}
