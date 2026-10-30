//! A broker stand-in a device could run: frames in a `static` embassy-sync channel, nothing on the
//! heap between the publisher and the settlement.
//!
//! The channel locks with `NoopRawMutex`, so it is not `Sync`, and the subscriber that holds its
//! receiver is not `Send`. The delivery counts its settlement in the subscription's cells, so it
//! is neither `Send` nor `Sync`. The subscriber and its delivery therefore implement only the
//! `Local*` contract twins, and the body reaches them through those alone. [`FrameBroker`] is the
//! same channel as a broker an app registers, with [`FrameSource`] its subscription.

// The broker's futures hold its `!Send` forms on purpose: that is what the body is held to here.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::convert::Infallible;
use core::future::ready;
use core::pin::Pin;
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::{Channel, Receiver};
use futures_core::Stream;
use ruststream_core::{
    AckError, BrokerMoves, HeaderMap, LocalBroker, LocalConnectedBroker, LocalIncomingMessage,
    LocalSubscriber, LocalSubscriptionSource,
};
use ruststream_runtime::IntoSource;

/// The largest payload a frame holds.
pub const PAYLOAD: usize = 64;

/// How many frames the channel buffers.
pub const DEPTH: usize = 4;

/// The channel the stand-in delivers through: a device keeps it in a `static`.
pub type Frames = Channel<NoopRawMutex, Frame, DEPTH>;

/// One message on the wire, held inline.
#[derive(Clone, Debug)]
pub struct Frame(heapless::Vec<u8, PAYLOAD>);

impl Frame {
    /// A frame carrying `payload`, or `None` when it exceeds [`PAYLOAD`] bytes.
    #[must_use]
    pub fn new(payload: &[u8]) -> Option<Self> {
        heapless::Vec::from_slice(payload).ok().map(Self)
    }
}

/// How the deliveries of a subscription were settled.
#[derive(Debug, Default)]
pub struct Settlements {
    acked: Cell<u32>,
    nacked: Cell<u32>,
}

impl Settlements {
    /// No delivery settled yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            acked: Cell::new(0),
            nacked: Cell::new(0),
        }
    }

    /// How many deliveries were acknowledged.
    #[must_use]
    pub fn acked(&self) -> u32 {
        self.acked.get()
    }

    /// How many deliveries were negatively acknowledged.
    #[must_use]
    pub fn nacked(&self) -> u32 {
        self.nacked.get()
    }
}

/// A subscription over the channel.
#[derive(Debug)]
pub struct FrameSubscriber {
    receiver: Receiver<'static, NoopRawMutex, Frame, DEPTH>,
    settled: &'static Settlements,
}

impl FrameSubscriber {
    /// A subscription that receives from `frames` and records its settlements in `settled`.
    #[must_use]
    pub fn new(frames: &'static Frames, settled: &'static Settlements) -> Self {
        Self {
            receiver: frames.receiver(),
            settled,
        }
    }
}

impl LocalSubscriber for FrameSubscriber {
    type Message = FrameDelivery;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<FrameDelivery, Infallible>> + '_ {
        Deliveries { subscriber: self }
    }
}

/// The stream of a [`FrameSubscriber`]: never ends, parks on the channel while it is empty.
struct Deliveries<'a> {
    subscriber: &'a FrameSubscriber,
}

impl Stream for Deliveries<'_> {
    type Item = Result<FrameDelivery, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let settled = self.subscriber.settled;
        self.subscriber.receiver.poll_receive(cx).map(|frame| {
            Some(Ok(FrameDelivery {
                frame,
                headers: HeaderMap::new(),
                settled,
            }))
        })
    }
}

/// A frame as the subscription hands it out, recording how it was settled.
#[derive(Debug)]
pub struct FrameDelivery {
    frame: Frame,
    headers: HeaderMap,
    settled: &'static Settlements,
}

impl LocalIncomingMessage for FrameDelivery {
    fn payload(&self) -> &[u8] {
        &self.frame.0
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        count(&self.settled.acked);
        ready(Ok(()))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        count(&self.settled.nacked);
        ready(Ok(()))
    }
}

/// Adds one: the executor's one thread settles a subscription's deliveries.
fn count(counter: &Cell<u32>) {
    counter.set(counter.get() + 1);
}

/// The stand-in as a broker an app registers: the channel its one subscription receives from.
///
/// Its connected form opens [`FrameSource`]; neither form is `Send`, as the deliveries are not.
#[derive(Debug)]
pub struct FrameBroker {
    frames: &'static Frames,
    settled: &'static Settlements,
}

impl FrameBroker {
    /// A broker over `frames`, recording its settlements in `settled`. Performs no I/O.
    #[must_use]
    pub const fn new(frames: &'static Frames, settled: &'static Settlements) -> Self {
        Self { frames, settled }
    }
}

/// The connected form of a [`FrameBroker`].
#[derive(Debug)]
pub struct FrameConnected {
    frames: &'static Frames,
    settled: &'static Settlements,
}

impl LocalBroker for FrameBroker {
    type Error = Infallible;
    type Connected = FrameConnected;

    fn connect(self) -> impl Future<Output = Result<FrameConnected, Infallible>> {
        ready(Ok(FrameConnected {
            frames: self.frames,
            settled: self.settled,
        }))
    }
}

impl LocalConnectedBroker for FrameConnected {
    type Error = Infallible;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }
}

/// The descriptor of the broker's one subscription, named `name`.
#[derive(Debug, Clone, Copy)]
pub struct FrameSource {
    name: &'static str,
}

impl FrameSource {
    /// The subscription, named `name` in the app's diagnostics.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self { name }
    }
}

impl LocalSubscriptionSource<FrameConnected> for FrameSource {
    type Subscriber = FrameSubscriber;
    // A frame the handler retries goes back on the channel by the device's own logic, not by a
    // copy this process publishes.
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.name
    }

    fn subscribe(
        self,
        connected: &FrameConnected,
    ) -> impl Future<Output = Result<FrameSubscriber, Infallible>> {
        ready(Ok(FrameSubscriber::new(
            connected.frames,
            connected.settled,
        )))
    }
}

impl IntoSource for FrameSource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}
