//! The brokers connect in registration order: one that refuses leaves the ones already connected
//! shut down, and a connected service pairs a bound publisher for a task beside it.

use std::future::ready;
use std::sync::{Arc, Mutex};

use ruststream_core::{Broker, BytesMut, DescribeServer, OutgoingMessage, Publisher, ServerSpec};
use ruststream_memory::local::{
    ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError, LocalMemoryPublish,
    LocalMemoryPublisher,
};
use ruststream_runtime::AppInfo;
use ruststream_runtime_compio::{RustStream, RustStreamError};

use super::support::{Clinging, on_runtime};

/// A bus that tells the test how to reach it once it is connected.
struct Watched(
    LocalMemoryBroker<8>,
    Arc<Mutex<Option<LocalMemoryPublisher<8>>>>,
);

impl Broker for Watched {
    type Error = LocalMemoryError;
    type Connected = ConnectedLocalMemoryBroker<8>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let connected = self.0.connect().await?;
        *self.1.lock().expect("the slot") = Some(connected.publisher());
        Ok(connected)
    }
}

impl DescribeServer for Watched {
    fn describe_server(&self) -> ServerSpec {
        self.0.describe_server()
    }
}

/// A server that is not there.
struct Unreachable;

impl Broker for Unreachable {
    type Error = LocalMemoryError;
    type Connected = ConnectedLocalMemoryBroker<8>;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Err(LocalMemoryError::ShutDown))
    }
}

impl DescribeServer for Unreachable {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("unreachable")
    }
}

#[test]
fn a_broker_that_cannot_connect_leaves_the_connected_ones_shut_down() {
    let slot = Arc::default();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .register_broker(Watched(LocalMemoryBroker::new(), Arc::clone(&slot)))
        .register_broker(Unreachable);

    on_runtime(async {
        let error = app.start().await.expect_err("the second broker refuses");
        assert!(
            matches!(error, RustStreamError::Connect(_)),
            "got {error:?}"
        );
        let publisher = slot.lock().expect("the slot").clone();
        let publisher = publisher.expect("the first broker connected");
        let refused = publisher
            .publish(OutgoingMessage::produced("probe", BytesMut::new()), None)
            .await;
        assert!(refused.is_err(), "the first broker was shut down");
    });
}

#[test]
fn a_running_service_pairs_a_bound_publisher_for_a_task_beside_it() {
    let bus = LocalMemoryBroker::<8>::new().bindable();
    let to_bus = bus.bind(LocalMemoryPublish);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(bus);

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        let publisher = running.publisher(to_bus).await.expect("the token pairs");
        publisher
            .publish(OutgoingMessage::produced("probe", BytesMut::new()), None)
            .await
            .expect("the bus is up");
        assert!(format!("{running:?}").contains("RunningApp"));
        running.shutdown().await.expect("a clean shutdown");
    });
}

#[test]
fn a_broker_that_fails_to_shut_down_is_the_error_of_the_shutdown_and_the_others_still_close() {
    let slot = Arc::default();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .register_broker(Watched(LocalMemoryBroker::new(), Arc::clone(&slot)))
        .register_broker(Clinging);

    on_runtime(async {
        let running = app.start().await.expect("the service starts");
        let error = running
            .shutdown()
            .await
            .expect_err("the second broker clings");
        assert!(
            matches!(error, RustStreamError::Shutdown(_)),
            "got {error:?}"
        );
        let publisher = slot.lock().expect("the slot").clone();
        let refused = publisher
            .expect("the first broker connected")
            .publish(OutgoingMessage::produced("probe", BytesMut::new()), None)
            .await;
        assert!(
            refused.is_err(),
            "the first broker was shut down regardless"
        );
    });
}
