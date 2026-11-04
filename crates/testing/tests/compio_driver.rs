//! The compio `TestApp` refuses a test runtime on the polling driver where the environment asks for
//! `io_uring`, and starts on it otherwise. `io_uring` is a Linux driver, so the test is Linux's.

#![cfg(target_os = "linux")]
#![allow(clippy::future_not_send)]

use std::env;
use std::error::Error;
use std::panic::{AssertUnwindSafe, catch_unwind};

use compio::driver::{DriverType, ProactorBuilder};
use compio::runtime::RuntimeBuilder;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::testing::compio::TestApp;
use ruststream::{Deserialized, subscriber};

type Bus = LocalMemoryBroker<4>;

/// A command, as its bytes.
#[derive(Deserialized)]
struct Command<'a>(&'a [u8]);

/// Accepts every command.
#[subscriber(LocalMemorySource::new("commands"))]
async fn on_command(command: &Command<'_>) -> HandlerOutcome {
    let _ = command.0;
    HandlerOutcome::ack()
}

#[test]
fn a_polling_runtime_is_refused_where_io_uring_is_required() -> Result<(), Box<dyn Error>> {
    let required = env::var_os("RUSTSTREAM_COMPIO_REQUIRE_IO_URING").is_some_and(|v| v == "1");
    let mut proactor = ProactorBuilder::new();
    proactor.driver_type(DriverType::Poll);
    let runtime = RuntimeBuilder::new().with_proactor(proactor).build()?;
    let started = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            let app = RustStream::new(AppInfo::new("commands", "0.1.0"))
                .with_broker(Bus::new(), |b| b.include(on_command));
            TestApp::start(app).await.map(drop)
        })
    }));
    assert_eq!(started.is_err(), required);
    Ok(())
}
