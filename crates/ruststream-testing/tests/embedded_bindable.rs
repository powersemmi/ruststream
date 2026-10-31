//! A device service that relays to a second bus: a token minted before the scopes rides the
//! mount chain as a plain publish policy, and the harness reads what reached the other bus.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;

use embassy_futures::block_on;
use ruststream::memory::local::prelude::{
    LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::prelude::*;
use ruststream::runtime::EmbeddedBindable;
use ruststream::testing::embedded::TestApp;

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

#[ruststream::app(runtime = embassy)]
fn app(sensors: Sensors, actuators: Actuators) -> EmbeddedApp<impl Mounts> {
    let actuators = EmbeddedBindable::new(actuators);
    let to_actuators = actuators.bind(LocalMemoryPublish);
    EmbeddedApp::new()
        .with_broker(actuators, |b| b)
        .with_broker(sensors, |b| b.include(on_level).out(Commands, to_actuators))
}

/// An acknowledgement for the sensors' bus.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "acks")]
struct Ack(Vec<u8>);

/// The slot acknowledgements leave through.
#[derive(OutSlot)]
#[publishes(Ack)]
struct Acks;

/// Answers every command on the sensors' bus.
#[subscriber(LocalMemorySource::new("commands"))]
async fn on_command(command: &Frame<'_>, Out(acks): Out<impl Publisher, Acks>) -> HandlerOutcome {
    match acks.message(&Ack(command.0.to_vec())).publish().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

/// The token's broker is registered after the broker that publishes through it.
#[ruststream::app(runtime = embassy)]
fn target_last(sensors: Sensors, actuators: Actuators) -> EmbeddedApp<impl Mounts> {
    let actuators = EmbeddedBindable::new(actuators);
    let to_actuators = actuators.bind(LocalMemoryPublish);
    EmbeddedApp::new()
        .with_broker(sensors, |b| b.include(on_level).out(Commands, to_actuators))
        .with_broker(actuators, |b| b)
}

/// Each broker publishes through a token bound to the other.
#[ruststream::app(runtime = embassy)]
fn bridge(sensors: Sensors, actuators: Actuators) -> EmbeddedApp<impl Mounts> {
    let sensors = EmbeddedBindable::new(sensors);
    let actuators = EmbeddedBindable::new(actuators);
    let to_actuators = actuators.bind(LocalMemoryPublish);
    let to_sensors = sensors.bind(LocalMemoryPublish);
    EmbeddedApp::new()
        .with_broker(sensors, |b| b.include(on_level).out(Commands, to_actuators))
        .with_broker(actuators, |b| b.include(on_command).out(Acks, to_sensors))
}

#[test]
fn a_command_reaches_a_bus_registered_after_its_publisher() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(target_last(Sensors::new(), Actuators::new())).await?;
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

#[test]
fn a_two_way_bridge_carries_a_frame_both_ways() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let tb = TestApp::start(bridge(Sensors::new(), Actuators::new())).await?;
        tb.broker::<Sensors>()
            .message(&Sent(b"high"))
            .to("sensors/level")
            .publish()
            .await?;

        tb.broker::<Sensors>()
            .published::<()>("acks")
            .assert_called_once()
            .with_raw(b"high");
        tb.shutdown().await?;
        Ok(())
    })
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
