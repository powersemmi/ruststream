use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, Outgoing, PublishContext, PublishTransform, RustStream};
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize)]
struct Receipt {
    id: u32,
}

struct Stamp;

impl<C> PublishTransform<C> for Stamp {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-stamp", b"1".to_vec());
    }
}

#[subscriber("orders", publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

// A step rides the position the `.out(marker, policy)` before it named, and there is none here.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm).transform(Stamp);
    });
}
