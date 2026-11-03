//! What the app-wide layers do to the replies a handler returns, and how a labeled broker shows up
//! in the app's description.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use compio::runtime::Runtime;
use ruststream::codec::JsonCodec;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::compio::{AppInfo, RustStream};
use ruststream::runtime::{Outgoing as WireOut, PublishLayer, PublishNext, PublishPipeline};
use ruststream::testing::compio::TestApp;
use ruststream::{Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u32,
}

#[subscriber(LocalMemorySource::new("orders"), reply)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

/// Counts the messages the app publishes.
#[derive(Clone)]
struct CountSent(Arc<AtomicUsize>);

impl PublishLayer for CountSent {
    fn on_publish<'a, Next: PublishPipeline, Sender: Publisher>(
        &'a self,
        out: &'a mut WireOut<'_>,
        next: PublishNext<'a, Next, Sender>,
    ) -> impl Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'a {
        self.0.fetch_add(1, Ordering::SeqCst);
        next.run(out)
    }
}

#[test]
fn a_publish_layer_sees_every_reply_the_app_publishes() -> Result<(), Box<dyn Error>> {
    let sent = Arc::new(AtomicUsize::new(0));
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .publish_layer(CountSent(Arc::clone(&sent)))
        .with_broker(LocalMemoryBroker::<8>::new(), |b| {
            b.include(confirm).out_reply(LocalMemoryPublish)
        });

    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app).await?;
        let orders = tb.broker::<LocalMemoryBroker<8>>();
        orders.message(&Order { id: 4 }).publish().await?;
        orders
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 4 });
        tb.shutdown().await?;
        Ok::<_, Box<dyn Error>>(())
    })?;

    assert_eq!(sent.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn a_labeled_broker_names_its_server_in_the_description() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker_labeled("ingress", LocalMemoryBroker::<8>::new(), |b| b)
        .with_broker_labeled_codec("egress", LocalMemoryBroker::<8>::new(), JsonCodec, |b| b);

    assert_eq!(
        app.servers().keys().collect::<Vec<_>>(),
        ["egress", "ingress"]
    );
    assert!(
        app.servers()
            .values()
            .all(|spec| format!("{spec:?}").contains("memory")),
        "each label describes the broker it names"
    );
}

#[test]
fn a_scope_codec_decodes_what_the_scope_mounts() -> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_codec(
        LocalMemoryBroker::<8>::new(),
        JsonCodec,
        |b| b.include(confirm).out_reply(LocalMemoryPublish),
    );

    Runtime::new()?.block_on(async {
        let tb = TestApp::start(app).await?;
        let orders = tb.broker::<LocalMemoryBroker<8>>();
        orders.message(&Order { id: 8 }).publish().await?;
        orders
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 8 });
        tb.shutdown().await?;
        Ok(())
    })
}
