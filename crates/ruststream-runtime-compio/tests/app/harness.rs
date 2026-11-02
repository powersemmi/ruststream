//! How the compio `TestApp` starts an app in process: with the state the test builds, and the
//! refusals it makes before and while the brokers connect.

use std::error::Error;
use std::future::ready;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use compio::runtime::Runtime;
use ruststream::Broker;
use ruststream::memory::local::LocalMemoryBroker;
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::testing::compio::TestApp;

/// A broker no crate gave an in-process mode.
struct Remote;

impl Broker for Remote {
    type Error = io::Error;
    type Connected = <LocalMemoryBroker<8> as Broker>::Connected;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Err(io::Error::other("no server in a test")))
    }
}

#[test]
fn the_state_a_test_builds_replaces_the_one_the_app_produces() -> Result<(), Box<dyn Error>> {
    let seen = Arc::new(AtomicU32::new(0));
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async |()| Ok::<_, io::Error>(7_u32))
        .after_startup({
            let seen = Arc::clone(&seen);
            async move |state: Arc<u32>| {
                seen.store(*state, Ordering::SeqCst);
                Ok::<_, io::Error>(())
            }
        })
        .register_broker(LocalMemoryBroker::<8>::new());

    Runtime::new()?.block_on(async {
        let tb = TestApp::with_state(app, |_brokers| 42).await?;
        tb.shutdown().await?;
        Ok::<_, Box<dyn Error>>(())
    })?;

    assert_eq!(seen.load(Ordering::SeqCst), 42);
    Ok(())
}

#[test]
fn a_broker_with_no_in_process_mode_is_refused_by_name() -> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(Remote);

    let error = Runtime::new()?
        .block_on(TestApp::start(app))
        .expect_err("a test cannot reach a server");

    assert!(error.to_string().contains("Remote"), "{error}");
    Ok(())
}

#[test]
fn a_state_producer_that_fails_stops_the_harness_before_a_broker_connects()
-> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async |()| Err::<(), _>(io::Error::other("no database")))
        .register_broker(LocalMemoryBroker::<8>::new());

    let error = Runtime::new()?
        .block_on(TestApp::start(app))
        .expect_err("the producer failed");

    assert!(error.to_string().contains("no database"), "{error}");
    Ok(())
}
