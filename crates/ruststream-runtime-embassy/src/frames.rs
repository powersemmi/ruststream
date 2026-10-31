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
use core::error::Error;
use core::fmt;
use core::future::ready;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::{Channel, Receiver};
use futures_core::Stream;
use ruststream_core::{
    AckError, Broker, BrokerMoves, ConnectedBroker, DefaultPublish, HeaderMap, IncomingMessage,
    Lend, LocalBatchSubscriber, LocalPublisher, LocalSettle, LocalSubscriber, OutgoingMessage,
    PairError, PublishPolicy, SubscriptionSource,
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

    /// The bytes the frame carries.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.0
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

    /// One received frame as the subscription hands it out.
    fn delivery(&self, frame: Frame) -> FrameDelivery {
        FrameDelivery {
            frame,
            headers: HeaderMap::new(),
            settled: self.settled,
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
        let subscriber = self.subscriber;
        subscriber
            .receiver
            .poll_receive(cx)
            .map(|frame| Some(Ok(subscriber.delivery(frame))))
    }
}

// A batch is the frames waiting when the first one arrives, up to the size the registration
// asked for, held inline: taking one allocates nothing.
impl LocalBatchSubscriber for FrameSubscriber {
    type Batch = heapless::Vec<FrameDelivery, DEPTH>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Infallible>> + '_ {
        Bursts {
            subscriber: self,
            size,
        }
    }
}

/// The batch stream of a [`FrameSubscriber`]: parks until a frame arrives, then takes it and
/// every frame already waiting behind it, up to the batch size.
struct Bursts<'a> {
    subscriber: &'a FrameSubscriber,
    size: NonZeroUsize,
}

impl Stream for Bursts<'_> {
    type Item = Result<heapless::Vec<FrameDelivery, DEPTH>, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let subscriber = self.subscriber;
        let limit = self.size.get().min(DEPTH);
        subscriber.receiver.poll_receive(cx).map(|first| {
            let mut batch = heapless::Vec::new();
            let mut next = Some(first);
            while let Some(frame) = next {
                // The limit is at most the channel's depth, which is the batch's capacity.
                let _ = batch.push(subscriber.delivery(frame));
                next = (batch.len() < limit)
                    .then(|| subscriber.receiver.try_receive().ok())
                    .flatten();
            }
            Some(Ok(batch))
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

impl IncomingMessage for FrameDelivery {
    fn payload(&self) -> &[u8] {
        &self.frame.0
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl IncomingMessage for FrameDelivery {}

impl LocalSettle for FrameDelivery {
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
    bursts: &'static Frames,
    requests: &'static Frames,
    replies: &'static Frames,
    events: &'static Frames,
    settled: &'static Settlements,
}

impl FrameBroker {
    /// A broker over `frames`, recording its settlements in `settled`. Performs no I/O.
    ///
    /// Its burst subscription ([`FrameSource::bursts`]) reads `frames` too, until
    /// [`with_bursts`](Self::with_bursts) gives it a channel of its own.
    #[must_use]
    pub const fn new(frames: &'static Frames, settled: &'static Settlements) -> Self {
        Self {
            frames,
            bursts: frames,
            requests: frames,
            replies: frames,
            events: frames,
            settled,
        }
    }

    /// The same broker with `events` as the channel its event subscription reads.
    #[must_use]
    pub const fn with_events(mut self, events: &'static Frames) -> Self {
        self.events = events;
        self
    }

    /// The same broker with `requests` as the channel its request subscription reads.
    #[must_use]
    pub const fn with_requests(mut self, requests: &'static Frames) -> Self {
        self.requests = requests;
        self
    }

    /// The same broker with `replies` as the channel its publisher sends to.
    ///
    /// Until it is given one, its publisher sends to `frames`.
    #[must_use]
    pub const fn with_replies(mut self, replies: &'static Frames) -> Self {
        self.replies = replies;
        self
    }

    /// The same broker with `bursts` as the channel its burst subscription reads.
    #[must_use]
    pub const fn with_bursts(mut self, bursts: &'static Frames) -> Self {
        self.bursts = bursts;
        self
    }
}

/// The connected form of a [`FrameBroker`].
#[derive(Debug)]
pub struct FrameConnected {
    frames: &'static Frames,
    bursts: &'static Frames,
    requests: &'static Frames,
    replies: &'static Frames,
    events: &'static Frames,
    settled: &'static Settlements,
}

impl Broker for FrameBroker {
    type Error = Infallible;
    type Connected = FrameConnected;

    fn connect(self) -> impl Future<Output = Result<FrameConnected, Infallible>> {
        ready(Ok(FrameConnected {
            frames: self.frames,
            bursts: self.bursts,
            requests: self.requests,
            replies: self.replies,
            events: self.events,
            settled: self.settled,
        }))
    }
}

impl ConnectedBroker for FrameConnected {
    type Error = Infallible;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }
}

/// The descriptor of a subscription to one of the broker's channels, named `name`.
#[derive(Debug, Clone, Copy)]
pub struct FrameSource {
    name: &'static str,
    lane: Lane,
}

/// Which of the broker's channels a [`FrameSource`] reads.
#[derive(Debug, Clone, Copy)]
enum Lane {
    Frames,
    Bursts,
    Requests,
    Events,
}

impl FrameSource {
    /// The subscription to the broker's main channel, named `name` in the app's diagnostics.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            lane: Lane::Frames,
        }
    }

    /// The subscription to the broker's burst channel (see [`FrameBroker::with_bursts`]).
    #[must_use]
    pub const fn bursts(name: &'static str) -> Self {
        Self {
            name,
            lane: Lane::Bursts,
        }
    }

    /// The subscription to the broker's request channel (see [`FrameBroker::with_requests`]).
    #[must_use]
    pub const fn requests(name: &'static str) -> Self {
        Self {
            name,
            lane: Lane::Requests,
        }
    }
}

impl FrameSource {
    /// The subscription to the broker's event channel (see [`FrameBroker::with_events`]).
    #[must_use]
    pub const fn events(name: &'static str) -> Self {
        Self {
            name,
            lane: Lane::Events,
        }
    }
}

impl SubscriptionSource<FrameConnected> for FrameSource {
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
        let frames = match self.lane {
            Lane::Frames => connected.frames,
            Lane::Bursts => connected.bursts,
            Lane::Requests => connected.requests,
            Lane::Events => connected.events,
        };
        ready(Ok(FrameSubscriber::new(frames, connected.settled)))
    }
}

impl IntoSource for FrameSource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// The broker's one publish policy: frames sent to its reply channel.
#[derive(Debug, Clone, Copy, Default)]
pub struct FramePublish;

impl PublishPolicy<FrameConnected> for FramePublish {
    type Live = FramePublisher;

    fn pair(
        self,
        connected: &FrameConnected,
    ) -> impl Future<Output = Result<FramePublisher, PairError>> {
        ready(Ok(FramePublisher {
            replies: connected.replies,
        }))
    }
}

impl DefaultPublish for FrameConnected {
    type Policy = FramePublish;
}

/// What a frame publish fails with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FramePublishError {
    /// The payload is longer than a frame holds.
    TooLong,
    /// The reply channel holds [`DEPTH`] frames already.
    Full,
}

impl fmt::Display for FramePublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong => write!(f, "the payload is longer than {PAYLOAD} bytes"),
            Self::Full => write!(f, "the reply channel holds {DEPTH} frames"),
        }
    }
}

impl Error for FramePublishError {}

/// The live publisher: copies the payload it is lent into a frame on the reply channel, so a
/// publish allocates nothing.
#[derive(Debug, Clone, Copy)]
pub struct FramePublisher {
    replies: &'static Frames,
}

impl LocalPublisher for FramePublisher {
    // The frame copies the bytes inline, so the client keeps nothing of the buffer.
    type Payload = Lend;
    type Error = FramePublishError;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), FramePublishError>> {
        let sent = Frame::new(msg.payload())
            .ok_or(FramePublishError::TooLong)
            .and_then(|frame| {
                self.replies
                    .try_send(frame)
                    .map_err(|_| FramePublishError::Full)
            });
        ready(sent)
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;
    use alloc::string::ToString;

    use embassy_futures::block_on;
    use ruststream_core::{Broker, LocalPublisher, OutgoingMessage, PublishPolicy};

    // The channels lock through embassy-sync, which the executor's critical section serves.
    use embassy_executor as _;

    use super::{
        DEPTH, FrameBroker, FramePublish, FramePublishError, Frames, PAYLOAD, Settlements,
    };

    #[test]
    fn a_reply_the_frames_cannot_hold_is_refused_with_the_reason() {
        block_on(async {
            // A device keeps the channels in statics; the test leaks them to the same lifetime.
            let frames: &'static Frames = Box::leak(Box::new(Frames::new()));
            let replies_channel: &'static Frames = Box::leak(Box::new(Frames::new()));
            let settled: &'static Settlements = Box::leak(Box::new(Settlements::new()));
            let broker = FrameBroker::new(frames, settled).with_replies(replies_channel);
            let connected = broker.connect().await.expect("the stand-in connects");
            let replies = FramePublish
                .pair(&connected)
                .await
                .expect("the policy pairs");
            let too_long = [0_u8; PAYLOAD + 1];

            let refused = replies
                .publish(OutgoingMessage::new("replies", too_long.as_slice()), None)
                .await;
            assert_eq!(refused, Err(FramePublishError::TooLong));
            assert_eq!(
                FramePublishError::TooLong.to_string(),
                "the payload is longer than 64 bytes"
            );
            for _ in 0..DEPTH {
                replies
                    .publish(OutgoingMessage::new("replies", b"ok".as_slice()), None)
                    .await
                    .expect("the channel has room");
            }
            let full = replies
                .publish(OutgoingMessage::new("replies", b"ok".as_slice()), None)
                .await;
            assert_eq!(full, Err(FramePublishError::Full));
            assert_eq!(
                FramePublishError::Full.to_string(),
                "the reply channel holds 4 frames"
            );
        });
    }
}
