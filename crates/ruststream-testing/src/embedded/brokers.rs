//! [`TestBrokers`], the app's connected brokers as a state the test builds reads them.

use std::any::{Any, type_name};
use std::fmt;
use std::rc::Rc;

use ruststream_runtime::testing::{InProcess, RunBroker};

#[cfg(doc)]
use super::TestApp;

/// The app's brokers, connected in process, handed to the state a test builds with
/// [`TestApp::with_state`], so the state can hold what it needs of them.
///
/// # Examples
///
/// See [`broker`](Self::broker).
pub struct TestBrokers<'a> {
    brokers: &'a [RunBroker],
    /// Each broker's connected form, held for as long as the state is being built.
    connected: Vec<Option<Rc<dyn Any>>>,
}

impl fmt::Debug for TestBrokers<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestBrokers")
            .field("brokers", &self.brokers.len())
            .finish_non_exhaustive()
    }
}

impl<'a> TestBrokers<'a> {
    /// The brokers of a run, while its app holds them.
    pub(super) fn of(brokers: &'a [RunBroker]) -> Self {
        let connected = brokers.iter().map(RunBroker::connected).collect();
        Self { brokers, connected }
    }

    /// The connected form of the app's broker of type `B`, the production type the app was
    /// built with.
    ///
    /// # Panics
    ///
    /// Panics if the app has no broker of type `B`, or more than one.
    ///
    /// # Examples
    ///
    /// A state that reports through the bus holds a publisher of the connected broker:
    ///
    /// ```
    /// use ruststream::memory::local::prelude::{
    ///     BytesMut, LocalMemoryBroker, LocalMemoryPublisher, LocalMemorySource, LocalPublisher,
    ///     OutgoingMessage,
    /// };
    /// use ruststream::runtime::{Context, EmbeddedApp, HandlerOutcome, Mounts};
    /// use ruststream::testing::embedded::TestApp;
    /// use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
    ///
    /// type Bus = LocalMemoryBroker<4>;
    ///
    /// /// What the board's handlers read: where they report a reading, when something listens.
    /// struct Board {
    ///     report: Option<LocalMemoryPublisher<4>>,
    /// }
    ///
    /// /// A reading, as the sensor sends its bytes.
    /// #[derive(Deserialized)]
    /// struct Reading<'a>(&'a [u8]);
    ///
    /// #[subscriber(LocalMemorySource::new("sensors/temp"))]
    /// async fn on_reading(reading: &Reading<'_>, ctx: &mut Context<'_, (), Board>) -> HandlerOutcome {
    ///     let Some(report) = &ctx.state().report else {
    ///         return HandlerOutcome::ack();
    ///     };
    ///     let line = OutgoingMessage::produced("reports", BytesMut::from(reading.0));
    ///     match report.publish(line, None).await {
    ///         Ok(()) => HandlerOutcome::ack(),
    ///         Err(_) => HandlerOutcome::retry(),
    ///     }
    /// }
    ///
    /// fn app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Board>> {
    ///     EmbeddedApp::new()
    ///         .with_state(Board { report: None })
    ///         .with_broker(bus, |b| b.include(on_reading))
    /// }
    ///
    /// /// A reading as the test's sensor sends it.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "sensors/temp")]
    /// struct Sent(&'static [u8]);
    ///
    /// async fn reports_each_reading() -> Result<(), Box<dyn std::error::Error>> {
    ///     let tb = TestApp::with_state(app(Bus::new()), |brokers| Board {
    ///         report: Some(brokers.broker::<Bus>().publisher()),
    ///     })
    ///     .await?;
    ///     tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;
    ///
    ///     tb.broker::<Bus>()
    ///         .published::<()>("reports")
    ///         .assert_called_once()
    ///         .with_raw(b"21");
    ///     tb.shutdown().await?;
    ///     Ok(())
    /// }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     embassy_futures::block_on(reports_each_reading())
    /// # }
    /// ```
    #[must_use]
    pub fn broker<B: InProcess + 'static>(&self) -> &B::Connected {
        let mut found = self
            .brokers
            .iter()
            .zip(&self.connected)
            .filter(|(broker, _)| broker.is::<B>())
            .filter_map(|(_, connected)| connected.as_ref()?.downcast_ref::<B::Connected>());
        let first = found
            .next()
            .unwrap_or_else(|| panic!("the app has no broker of type {}", type_name::<B>()));
        assert!(
            found.next().is_none(),
            "the app has more than one broker of type {}",
            type_name::<B>(),
        );
        first
    }
}
