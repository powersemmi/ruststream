//! Subscription descriptors over the in-memory broker: a placeholder takes its name, a start
//! position decorates and hands back the source it wraps, and a source reports the address its
//! broker gives the name.
#![cfg(feature = "memory")]

use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryPosition, MemorySource, Retaining, Retention,
};
use ruststream::{
    Broker, Buffered, Name, RedeliveryAddress, RedeliveryAddressed, StartAt, SubscriptionSource,
    Unnamed, nonzero,
};

/// The generic clone keeps the assertion honest: a `Copy` placeholder would otherwise make
/// the call read as redundant at the call site.
fn clone_of<T: Clone>(value: &T) -> T {
    value.clone()
}

#[test]
fn an_unnamed_placeholder_builds_its_kind_once_the_name_arrives() {
    let placeholder = Unnamed::<Name>::default();
    assert!(format!("{placeholder:?}").contains("Unnamed"));

    let named: Name = clone_of(&placeholder).into_named("orders");
    assert_eq!(
        SubscriptionSource::<ConnectedMemoryBroker>::name(&named),
        "orders"
    );
}

#[test]
fn a_start_position_decorates_the_source_it_wraps() {
    let source = StartAt::new(MemorySource::new("orders"), MemoryPosition::start());
    assert_eq!(
        SubscriptionSource::<ConnectedMemoryBroker<Retaining>>::name(&source),
        "orders"
    );
    assert!(format!("{source:?}").contains("StartAt"));
}

/// The name source has no address of its own: the broker's answer for that name is what it
/// reports, and a decorator reports whatever it wraps.
#[tokio::test]
async fn a_source_reports_the_address_its_broker_gives_the_name() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("the in-memory broker connects");
    let address = RedeliveryAddress::new("orders");
    assert_eq!(address.as_str(), "orders");
    assert_eq!(address.to_string(), "orders");

    assert_eq!(
        Name::new("orders")
            .redelivery_address(&connected)
            .await
            .expect("the in-memory broker answers without a lookup"),
        address.clone(),
    );
    assert_eq!(
        Buffered::new(MemorySource::new("orders"))
            .redelivery_address(&connected)
            .await
            .expect("client-side batching changes no address"),
        address.clone(),
    );

    // The start-position decorator only wraps a source whose subscriptions replay, so it is
    // asked on a retaining broker.
    let retaining = MemoryBroker::retaining(Retention::Messages(nonzero!(8)))
        .connect()
        .await
        .expect("the in-memory broker connects");
    assert_eq!(
        StartAt::new(MemorySource::new("orders"), MemoryPosition::start())
            .redelivery_address(&retaining)
            .await
            .expect("a start position changes no address"),
        address,
    );
}

/// The wrapper hides the descriptor a broker's settings trait is bound to, so it hands it
/// back: the mapped source is what the subscription opens on, and the position is untouched.
#[test]
fn a_start_position_hands_back_the_source_it_wraps() {
    let renamed =
        StartAt::new(MemorySource::new("orders"), MemoryPosition::start()).map_inner(|inner| {
            assert_eq!(
                SubscriptionSource::<ConnectedMemoryBroker<Retaining>>::name(&inner),
                "orders",
            );
            MemorySource::new("orders-7")
        });
    assert_eq!(
        SubscriptionSource::<ConnectedMemoryBroker<Retaining>>::name(&renamed),
        "orders-7",
    );
}
