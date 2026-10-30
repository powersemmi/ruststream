The local form of [`InProcess`]: the transition that runs a broker whose types are not `Send`
inside the test process.

Every [`InProcess`] broker is a `LocalInProcess` through a blanket impl.

# Examples

```
use std::convert::Infallible;

use std::rc::Rc;

use ruststream::LocalBroker;
use ruststream::testing::LocalInProcess;
# use std::cell::{OnceCell, RefCell};
#
# use ruststream::testing::{Coordinator, LocalTestableBroker};
# use ruststream::{LocalConnectedBroker, OutgoingMessage, RawMessage};
#
# /// The in-process transport: what was published and the harness's coordinator, behind `Rc`.
# #[derive(Default)]
# struct Transport {
#     published: RefCell<Vec<RawMessage>>,
#     coordinator: OnceCell<Coordinator>,
# }
#
# pub struct QueueConnected {
#     transport: Rc<Transport>,
# }
#
# impl LocalTestableBroker for QueueConnected {
#     fn install_coordinator(&self, coordinator: Coordinator) {
#         // A second install is ignored: the run keeps counting on the first coordinator.
#         self.transport.coordinator.get_or_init(|| coordinator);
#     }
#
#     fn inject(&self, message: OutgoingMessage<'_>) {
#         let raw = RawMessage::new(message.name().to_owned(), message.payload().to_vec());
#         self.transport.published.borrow_mut().push(raw);
#     }
#
#     fn published(&self, name: &str) -> Vec<RawMessage> {
#         self.transport
#             .published
#             .borrow()
#             .iter()
#             .filter(|message| message.name() == name)
#             .cloned()
#             .collect()
#     }
# }
#
# impl LocalConnectedBroker for QueueConnected {
#     type Error = Infallible;
#     type Closed = ();
#
#     async fn shutdown(self) -> Result<(), Infallible> {
#         Ok(())
#     }
# }
pub struct QueueBroker {
    transport: Rc<Transport>,
}

impl LocalBroker for QueueBroker {
    type Error = Infallible;
    type Connected = QueueConnected;

    async fn connect(self) -> Result<QueueConnected, Infallible> {
        // The production transition opens the client's socket here.
        Ok(QueueConnected {
            transport: self.transport,
        })
    }
}

impl LocalInProcess for QueueBroker {
    async fn connect_in_process(self) -> Result<QueueConnected, Infallible> {
        Ok(QueueConnected {
            transport: self.transport,
        })
    }
}
```
