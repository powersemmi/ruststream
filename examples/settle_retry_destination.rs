//! Where a copy of a delivery goes: the `Retry` slot every registration has, replaced with
//! `.out_retry(policy)` and its slot steps, and `.to(name)` on a subscription that reads many
//! destinations and addresses none. Each scenario is a test.
//!
//! ```text
//! cargo test --example settle_retry_destination --features testing,macros,memory,json,asyncapi
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream::memory::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
}

mod mount {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    // The derive in the prelude and the value a transform edits share the name `Outgoing`.
    use ruststream::runtime::{Outgoing, PublishContext};
    use ruststream::testing::TestApp;

    use super::Payment;

    // --8<-- [start:mount]
    /// Stamps every copy with the subscription its delivery came from.
    struct RetriedFrom;

    impl<C, Options> PublishTransform<ForReply<C>, Options> for RetriedFrom {
        type Destination = Reads;

        fn apply(
            &self,
            out: &mut Outgoing<'_>,
            _options: &mut Option<Options>,
            cx: &PublishContext<'_, C>,
        ) {
            out.headers_mut()
                .insert("x-retried-from", cx.name().to_owned());
        }
    }

    #[subscriber("payments")]
    async fn charge(payment: &Payment) -> HandlerOutcome {
        tracing::warn!(payment.id, "gateway refused the charge");
        HandlerOutcome::retry()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(charge)
                .max_attempts(nonzero!(2u32))
                .dead_letter("payments.dead")
                // Replaces the broker's default publisher for the copies of this registration.
                .out_retry(Publish)
                .transform(RetriedFrom);
        })
    }
    // --8<-- [end:mount]

    #[tokio::test]
    async fn the_dead_letter_copy_leaves_through_the_retry_slot() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&Payment { id: 7 }).publish().await?;

        tb.broker::<MemoryBroker>()
            .published::<Payment>("payments.dead")
            .assert_called_once()
            .with(&Payment { id: 7 })
            .with_header("x-retried-from", "payments");
        Ok(())
    }
}

mod named {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::asyncapi::build_spec;
    use ruststream::memory::prelude::*;
    use ruststream::memory::{ConnectedMemoryBroker, MemorySubscriber};
    use ruststream::testing::TestApp;
    use ruststream::{NamedCopies, SubscriptionSource};

    use super::Payment;

    // --8<-- [start:filter]
    /// A subscription that reads every payment subject and addresses none of them, as a wildcard
    /// or a topic filter does. `Copies = NamedCopies` says so.
    #[derive(Debug, Clone)]
    struct PaymentFilter {
        pattern: &'static str,
    }

    impl PaymentFilter {
        const fn new(pattern: &'static str) -> Self {
            Self { pattern }
        }
    }

    impl SubscriptionSource<ConnectedMemoryBroker> for PaymentFilter {
        type Subscriber = MemorySubscriber<Discarding>;
        type Copies = NamedCopies;

        fn name(&self) -> &str {
            self.pattern
        }

        async fn subscribe(
            self,
            connected: &ConnectedMemoryBroker,
        ) -> Result<Self::Subscriber, MemoryError> {
            MemoryPattern::new(self.pattern).subscribe(connected).await
        }
    }

    #[subscriber(PaymentFilter::new("payments.*"))]
    async fn reconcile(payment: &Payment) -> HandlerOutcome {
        tracing::info!(payment.id, "not cleared yet");
        HandlerOutcome::retry_after(Duration::from_secs(5))
    }
    // --8<-- [end:filter]

    // --8<-- [start:named]
    fn app() -> impl App {
        RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            // The descriptor addresses nothing, so the mount site names where a copy goes.
            b.include(reconcile).out_retry(Publish).to("payments.retry");
        })
    }

    /// The same registration with no destination named.
    fn unnamed() -> impl App {
        RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(reconcile);
        })
    }
    // --8<-- [end:named]

    #[tokio::test]
    async fn a_registration_without_a_destination_does_not_start() {
        assert!(TestApp::start(unnamed()).await.is_err());
        assert!(TestApp::start(app()).await.is_ok());
    }

    #[test]
    fn the_document_reports_where_copies_go() -> Result<(), Box<dyn Error>> {
        let spec = build_spec(&app());

        let retry = spec
            .channels
            .get("payments.retry")
            .ok_or("no retry channel")?;
        assert_eq!(retry.address.as_deref(), Some("payments.retry"));
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
