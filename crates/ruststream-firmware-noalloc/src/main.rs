//! A device service as a board links it: the facade's embassy runtime over the local bus, one
//! subscriber, and no global allocator. The binary links only while nothing in the dependency
//! graph of the no-alloc build needs a heap.

#![no_std]
#![no_main]

use cortex_m_rt::exception;
use embassy_executor::Spawner;
use panic_halt as _;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryBus, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::subscriber;
use systick_timer::SystickDriver;

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

// The core clock of the board and the SysTick reload for a 1 ms wrap.
embassy_time_driver::time_driver_impl!(static DRIVER: SystickDriver<4>
    = SystickDriver::new(8_000_000, 7999));

#[exception]
fn SysTick() {
    DRIVER.systick_interrupt();
}

#[cfg(not(any(feature = "postcard", feature = "json-core")))]
mod service {
    use ruststream::Deserialized;

    use super::{HandlerOutcome, LocalMemorySource, subscriber};

    /// A reading, as its bytes.
    #[derive(Deserialized)]
    pub struct Reading<'a>(&'a [u8]);

    /// Acknowledges every reading.
    #[subscriber(LocalMemorySource::new("sensors/temp"))]
    pub async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
        let _ = reading.0;
        HandlerOutcome::ack()
    }
}

#[cfg(any(feature = "postcard", feature = "json-core"))]
mod service {
    use ruststream::Outgoing;
    use serde::{Deserialize, Serialize};

    use super::{HandlerOutcome, LocalMemorySource, subscriber};

    /// A sensor's reading, as the other end of the bus encodes it.
    #[derive(Serialize, Deserialize, Outgoing)]
    #[outgoing(name = "sensors/temp")]
    pub struct Reading {
        sensor: u8,
        millivolts: u16,
    }

    /// Acknowledges every reading.
    #[subscriber(LocalMemorySource::new("sensors/temp"))]
    pub async fn on_reading(reading: &Reading) -> HandlerOutcome {
        let _ = (reading.sensor, reading.millivolts);
        HandlerOutcome::ack()
    }
}

#[cfg(all(feature = "json-core", not(feature = "postcard")))]
use ruststream::codec::JsonCoreCodec as Chosen;
#[cfg(feature = "postcard")]
use ruststream::codec::PostcardCodec as Chosen;

#[ruststream::app(runtime = embassy)]
fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        #[cfg(any(feature = "postcard", feature = "json-core"))]
        let b = b.with_codec(Chosen);
        b.include(service::on_reading)
    })
}

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    let mut core = cortex_m::Peripherals::take().expect("the core peripherals are taken once");
    DRIVER.start(&mut core.SYST);
    spawner.spawn(app::task(Bus::new(&BUS)).expect("the task pool holds the one app"));
}
