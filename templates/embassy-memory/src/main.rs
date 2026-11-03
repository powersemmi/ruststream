//! {{project-name}} - a RustStream device service on the embassy runtime.
//!
//! The service lives in `service`; this file is the board: the bus, the clock and `main`, which
//! spawns the app's task. The framework is built without `alloc`, so the firmware has no heap and
//! the service allocates nothing at startup or per message.
//!
//! `cargo build --release` links the firmware for the target `.cargo/config.toml` names, and
//! `probe-rs run --chip <chip> target/thumbv7em-none-eabihf/release/{{project-name}}` flashes it
//! and prints its log lines. `memory.x` and the clock below describe the board.

#![no_std]
#![no_main]

mod service;

use cortex_m_rt::exception;
use defmt_rtt as _;
use embassy_executor::Spawner;
use panic_halt as _;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryBus};
use systick_timer::SystickDriver;

/// The board's bus: each subscription holds four messages, waiting or in its handler's hands.
pub type Bus = LocalMemoryBroker<4>;

/// The bus is a `static` the board declares; the app's task receives a broker built over it.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

// The clock of `embassy-time`: SysTick at the core clock of the board (8 MHz here), reloaded
// every 1 ms.
embassy_time_driver::time_driver_impl!(static DRIVER: SystickDriver<4>
    = SystickDriver::new(8_000_000, 7999));

#[exception]
fn SysTick() {
    DRIVER.systick_interrupt();
}

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    let mut core = cortex_m::Peripherals::take().expect("the core peripherals are taken once");
    DRIVER.start(&mut core.SYST);
    spawner.spawn(service::app::task(Bus::new(&BUS)).expect("the task pool holds the one app"));
}
