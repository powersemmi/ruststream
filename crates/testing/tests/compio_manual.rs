//! The compio services of `compio_handler`, `compio_reply` and `compio_retry` declared without the
//! macros: the test bodies are theirs, since the harness reads the same records whichever way the
//! service was declared.

#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::ready;
use std::time::Duration;

use compio::runtime::Runtime;
use ruststream::memory::local::prelude::{
    BytesMut, LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{
    Context, Deserialized, Handle, HandlerOutcome, Input, MessageWire, OutEntry, OutSlot, Outs,
    PublishedThrough, RETRY_COUNT_HEADER, Reads, ReplyShape, Serialized, SerializedReply,
    SerializedWire, SoloDeserialized, WireBytes, subscriber,
};
use ruststream::testing::Outcome;
use ruststream::testing::compio::TestApp;
use ruststream::{
    CallerName, FixedName, MessageHeaders, NoHeaders, OutgoingDestination, Publisher,
};

type Bus = LocalMemoryBroker<4>;

/// A frame, as its bytes.
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = fmt::Error;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, fmt::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

/// A frame the test publishes, to the destination it names.
struct Sent(&'static [u8]);

// `#[derive(Outgoing, Serialized)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Sent {
    type Form = CallerName;
}

impl MessageHeaders for Sent {
    type Contract = NoHeaders;
}

impl Serialized for Sent {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(self.0))
    }
}

impl MessageWire for Sent {
    type Wire = SerializedWire;
}

mod handler {
    use super::*;

    // --8<-- [start:handler-service]
    /// Acknowledges every reading.
    struct OnReading;

    impl Handle<Frame<'_>> for OnReading {
        fn handle(
            &self,
            reading: &Frame<'_>,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            let _ = reading.0;
            ready(Ok(()))
        }
    }

    fn app() -> RustStream {
        RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker(Bus::new(), |b| {
            b.include(subscriber(LocalMemorySource::new("readings"), OnReading).build())
        })
    }
    // --8<-- [end:handler-service]

    // --8<-- [start:handler-test]
    #[test]
    fn a_published_reading_is_handled_and_acknowledged() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            tb.broker::<Bus>()
                .message(&Sent(b"21"))
                .to("readings")
                .publish()
                .await?;

            tb.broker::<Bus>()
                .subscriber("readings")
                .assert_called_once()
                .with_raw(b"21")
                .settled(HandlerOutcome::ack());
            tb.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:handler-test]
}

mod reply {
    use super::*;

    /// What the display shows.
    struct Shown(Vec<u8>);

    // `#[derive(Outgoing, Serialized)]` with `#[outgoing(name = "display")]`, by hand.
    impl OutgoingDestination for Shown {
        type Form = FixedName;

        const DESTINATION: &'static str = "display";
    }

    impl MessageHeaders for Shown {
        type Contract = NoHeaders;
    }

    impl Serialized for Shown {
        type Error = Infallible;

        fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
            Ok(WireBytes::Own(&self.0))
        }
    }

    impl MessageWire for Shown {
        type Wire = SerializedWire;
    }

    impl ReplyShape for Shown {
        type Body = Self;
        type Headers = ();
        type Wire = SerializedReply;
    }

    /// The alarm a full tank raises.
    struct Alarm(Vec<u8>);

    // `#[derive(Outgoing, Serialized)]` with `#[outgoing(name = "alarms")]`, by hand.
    impl OutgoingDestination for Alarm {
        type Form = FixedName;

        const DESTINATION: &'static str = "alarms";
    }

    impl MessageHeaders for Alarm {
        type Contract = NoHeaders;
    }

    impl Serialized for Alarm {
        type Error = Infallible;

        fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
            Ok(WireBytes::Own(&self.0))
        }
    }

    impl MessageWire for Alarm {
        type Wire = SerializedWire;
    }

    // --8<-- [start:reply-service]
    /// The slot alarms leave through.
    struct Alarms;

    impl OutSlot for Alarms {
        const NAME: &'static str = "Alarms";
        type Destination = Reads;
    }

    impl PublishedThrough<Alarms> for Alarm {}

    /// Shows every level, in percent.
    struct Show;

    impl Handle<Frame<'_>, Shown> for Show {
        fn handle(
            &self,
            level: &Frame<'_>,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Shown, HandlerOutcome>> {
            ready(Ok(Shown([level.0, b" %"].concat())))
        }
    }

    /// Raises an alarm for every level.
    struct Watch;

    impl<Entry> Handle<Frame<'_>, (), Outs<(Entry,)>> for Watch
    where
        Entry: OutEntry<Alarms, Wire: Publisher>,
    {
        async fn handle(
            &self,
            level: &Frame<'_>,
            outs: &Outs<(Entry,)>,
            _ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            outs.get(Alarms)
                .message(&Alarm(level.0.to_vec()))
                .publish()
                .await
                .map_err(|_| HandlerOutcome::retry())
        }
    }

    fn app() -> RustStream {
        RustStream::new(AppInfo::new("tank", "0.1.0")).with_broker(Bus::new(), |b| {
            b.include(
                subscriber(LocalMemorySource::new("levels"), Show)
                    .reply()
                    .build(),
            )
            .out_reply(LocalMemoryPublish)
            .include(subscriber(LocalMemorySource::new("tank"), Watch).build())
            .out(Alarms, LocalMemoryPublish)
        })
    }
    // --8<-- [end:reply-service]

    // --8<-- [start:published]
    #[test]
    fn a_reply_reaches_its_destination() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            tb.broker::<Bus>()
                .message(&Sent(b"80"))
                .to("levels")
                .publish()
                .await?;

            tb.broker::<Bus>()
                .published::<()>("display")
                .assert_called_once()
                .with_raw(b"80 %");
            tb.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:published]

    // --8<-- [start:out]
    #[test]
    fn a_slot_publish_is_read_back_through_the_slot() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            tb.broker::<Bus>()
                .message(&Sent(b"full"))
                .to("tank")
                .publish()
                .await?;

            tb.out::<Alarms>().assert_called_once().with_raw(b"full");
            tb.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:out]
}

mod retry {
    use super::*;

    /// The delay the handler asks its retry for.
    const LATER: Duration = Duration::from_secs(5);

    // --8<-- [start:retry-service]
    /// Asks for a retry later on a first delivery, and acknowledges the copy that carries its
    /// retry count.
    struct OnJob;

    impl Handle<Frame<'_>> for OnJob {
        fn handle(
            &self,
            job: &Frame<'_>,
            _outs: &(),
            ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            let _ = job.0;
            ready(match ctx.headers().get_str(RETRY_COUNT_HEADER) {
                Some("1") => Ok(()),
                _ => Err(HandlerOutcome::retry_after(LATER)),
            })
        }
    }

    fn app() -> RustStream {
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(Bus::new(), |b| {
            b.include(subscriber(LocalMemorySource::new("jobs"), OnJob).build())
                .out_retry(LocalMemoryPublish)
        })
    }
    // --8<-- [end:retry-service]

    // --8<-- [start:advance]
    #[test]
    fn a_retry_is_redelivered_only_once_its_delay_has_passed() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            tb.broker::<Bus>()
                .message(&Sent(b"job"))
                .to("jobs")
                .publish()
                .await?;

            tb.advance(Duration::from_secs(4)).await?;
            tb.broker::<Bus>()
                .subscriber("jobs")
                .assert_called_once()
                .settled(HandlerOutcome::retry_after(LATER));

            tb.advance(Duration::from_secs(1)).await?;
            let jobs = tb.broker::<Bus>();
            let calls = jobs.subscriber("jobs").assert_called(2);
            assert_eq!(
                calls.outcomes().last(),
                Some(&Outcome::Ack),
                "the copy carries its retry count to the handler"
            );
            tb.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:advance]
}
