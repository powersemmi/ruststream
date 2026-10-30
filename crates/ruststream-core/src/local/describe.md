The local form of [`DescribeServer`]: the server coordinate a broker whose types are not `Send`
reports for the `AsyncAPI` document.

Every [`DescribeServer`] is a `LocalDescribeServer` through a blanket impl.

# Examples

```
# use std::convert::Infallible;
# use std::rc::Rc;
#
use ruststream::{LocalDescribeServer, ServerSpec};
# use ruststream::{LocalBroker, LocalConnectedBroker};
#
# #[derive(Default)]
# struct Client;
# struct QueueConnected {
#     client: Rc<Client>,
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

struct QueueBroker {
    url: String,
    client: Rc<Client>,
}
#
# impl LocalBroker for QueueBroker {
#     type Error = Infallible;
#     type Connected = QueueConnected;
#
#     async fn connect(self) -> Result<QueueConnected, Infallible> {
#         Ok(QueueConnected { client: self.client })
#     }
# }

impl LocalDescribeServer for QueueBroker {
    fn describe_server(&self) -> ServerSpec {
        // The coordinate, never the credentials the URL may carry.
        ServerSpec::from_url(&self.url, "queue")
    }
}
```
