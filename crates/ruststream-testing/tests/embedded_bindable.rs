//! A device service that relays to a second bus: a token minted before the scopes rides the
//! mount chain as a plain publish policy, and the harness reads what reached the other bus.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;

use embassy_futures::block_on;
use ruststream::memory::local::prelude::{
    LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
use ruststream::testing::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
use ruststream_runtime_embassy::Bindable;

/// The sensors' bus.
type Sensors = LocalMemoryBroker<4>;

/// The actuators' bus.
type Actuators = LocalMemoryBroker<2>;

/// A frame, as its bytes.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A frame the test publishes.
#[derive(Outgoing, Serialized)]
struct Sent(&'static [u8]);

/// A command for the actuators' bus.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "commands")]
struct Command(Vec<u8>);

/// The slot commands leave through.
#[derive(OutSlot)]
#[publishes(Command)]
struct Commands;

/// Turns every level into a command for the other bus.
#[subscriber(LocalMemorySource::new("sensors/level"))]
async fn on_level(
    level: &Frame<'_>,
    Out(commands): Out<impl Publisher, Commands>,
) -> HandlerOutcome {
    match commands.message(&Command(level.0.to_vec())).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[ruststream::app]
fn app(sensors: Sensors, actuators: Actuators) -> EmbeddedApp<impl Mounts> {
    let actuators = Bindable::new(actuators);
    let to_actuators = actuators.bind(LocalMemoryPublish);
    EmbeddedApp::new()
        .with_broker(actuators, |b| b)
        .with_broker(sensors, |b| b.include(on_level).out(Commands, to_actuators))
}

#[test]
fn a_command_reaches_the_bus_its_token_is_bound_to() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(app(Sensors::new(), Actuators::new())).await?;
        tb.broker::<Sensors>()
            .message(&Sent(b"high"))
            .to("sensors/level")
            .publish()
            .await?;

        tb.broker::<Actuators>()
            .published::<()>("commands")
            .assert_called_once()
            .with_raw(b"high");
        tb.shutdown().await?;
        Ok(())
    })
}
