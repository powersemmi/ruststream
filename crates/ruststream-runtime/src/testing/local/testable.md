The local form of [`TestableBroker`]: the view of an in-process connected form the test harness
drives, for a broker whose types are not `Send`.

The items are those of [`TestableBroker`] without the `Send` and `Sync` bounds. Every
[`TestableBroker`] is a `LocalTestableBroker` through a blanket impl.

# Examples

```
use std::cell::{OnceCell, RefCell};
use std::rc::Rc;

use ruststream::testing::{Coordinator, LocalTestableBroker};
use ruststream::{OutgoingMessage, RawMessage};

/// The in-process transport: what was published and the harness's coordinator, behind `Rc`.
#[derive(Default)]
struct Transport {
    published: RefCell<Vec<RawMessage>>,
    coordinator: OnceCell<Coordinator>,
}

pub struct QueueConnected {
    transport: Rc<Transport>,
}

impl LocalTestableBroker for QueueConnected {
    fn install_coordinator(&self, coordinator: Coordinator) {
        // A second install is ignored: the run keeps counting on the first coordinator.
        self.transport.coordinator.get_or_init(|| coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let raw = RawMessage::new(message.name().to_owned(), message.payload().to_vec());
        self.transport.published.borrow_mut().push(raw);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.transport
            .published
            .borrow()
            .iter()
            .filter(|message| message.name() == name)
            .cloned()
            .collect()
    }
}
# fn main() {
#     let connected = QueueConnected { transport: Rc::default() };
#     let first = Coordinator::new(1);
#     first.enqueued();
#     connected.install_coordinator(first);
#     connected.install_coordinator(Coordinator::new(1));
#     let kept = format!("{:?}", connected.transport.coordinator);
#     assert!(kept.contains("in_flight: 1"), "{kept}");
# }
```
