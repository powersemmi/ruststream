//! The payments side of the shop without the `macros` feature: the messages declare their
//! destination through the trait impls the derive would write, and the subscriber is a named type
//! with an `impl Handle`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ruststream::codec::CborCodec;
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::orders::Line;
use crate::state::Shop;

// --8<-- [start:codec]
/// The codec of the topics between the shop's own steps: CBOR is smaller than JSON, and no
/// other team reads them.
pub(crate) const INTERNAL: CborCodec = CborCodec;
// --8<-- [end:codec]

// --8<-- [start:request]
/// What the orders side asks payments to charge.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub(crate) struct PaymentRequest {
    pub(crate) order: u64,
    pub(crate) customer: String,
    pub(crate) lines: Vec<Line>,
    pub(crate) amount_cents: u64,
    pub(crate) request_id: String,
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "payments.requested")]` writes.
impl OutgoingDestination for PaymentRequest {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments.requested";
}

impl MessageHeaders for PaymentRequest {
    type Contract = NoHeaders;
}

impl MessageInfo for PaymentRequest {
    const NAME: &'static str = "PaymentRequest";
    const DESCRIPTION: Option<&'static str> = Some("What the orders side asks payments to charge.");
}

/// A charged order, on its way to shipping.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub(crate) struct Paid {
    pub(crate) order: u64,
    pub(crate) customer: String,
    pub(crate) lines: Vec<Line>,
}

impl OutgoingDestination for Paid {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments.settled";
}

impl MessageHeaders for Paid {
    type Contract = NoHeaders;
}

impl MessageInfo for Paid {
    const NAME: &'static str = "Paid";
    const DESCRIPTION: Option<&'static str> = Some("A charged order, on its way to shipping.");
}
// --8<-- [end:request]

/// Why the payment provider refused a charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The card does not cover the amount; another try gets the same answer.
    Declined,
    /// The provider's rate limit is used up for the current second.
    Busy,
    /// The provider did not answer.
    Unavailable,
}

/// Stands in for the payment provider's client: it declines amounts over the card limit, answers
/// `Busy` past its rate limit, and fails while it is offline.
#[derive(Debug)]
pub(crate) struct Gateway {
    limit_cents: u64,
    per_second: u32,
    window: Mutex<(Instant, u32)>,
    offline: AtomicBool,
}

impl Gateway {
    pub(crate) fn new(limit_cents: u64, per_second: u32) -> Self {
        Self {
            limit_cents,
            per_second,
            window: Mutex::new((Instant::now(), 0)),
            offline: AtomicBool::new(false),
        }
    }

    /// Charges `request`, or says why not.
    pub(crate) async fn charge(&self, request: &PaymentRequest) -> Result<(), Refusal> {
        if self.offline.load(Ordering::Relaxed) {
            return Err(Refusal::Unavailable);
        }
        if request.amount_cents > self.limit_cents {
            return Err(Refusal::Declined);
        }
        let admitted = {
            let mut window = self.window.lock().await;
            if window.0.elapsed() >= Duration::from_secs(1) {
                *window = (Instant::now(), 0);
            }
            let admitted = window.1 < self.per_second;
            window.1 += u32::from(admitted);
            admitted
        };
        if admitted { Ok(()) } else { Err(Refusal::Busy) }
    }

    /// Takes the provider offline: every charge fails until it comes back.
    #[cfg(test)]
    pub(crate) fn go_offline(&self) {
        self.offline.store(true, Ordering::Relaxed);
    }
}

// --8<-- [start:charge]
/// How long a busy provider asks its callers to wait.
pub(crate) const BACKOFF: Duration = Duration::from_secs(1);

/// Charges an order through the payment provider.
struct Charge;

// The state type is the last parameter of `Handle`; the mount checks it against the app's.
impl Handle<PaymentRequest, Paid, (), (), Shop> for Charge {
    async fn handle(
        &self,
        request: &PaymentRequest,
        _outs: &(),
        ctx: &mut Context<'_, (), Shop>,
    ) -> Result<Paid, HandlerOutcome> {
        let State(gateway) = State::<Arc<Gateway>>::from_context(ctx).await?;
        match gateway.charge(request).await {
            Ok(()) => Ok(Paid {
                order: request.order,
                customer: request.customer.clone(),
                lines: request.lines.clone(),
            }),
            // Past the rate limit: a redelivery right now would be refused again.
            Err(Refusal::Busy) => Err(HandlerOutcome::retry_after(BACKOFF)),
            // No answer: try again at once, up to the attempt cap.
            Err(Refusal::Unavailable) => Err(HandlerOutcome::retry()),
            // A refused card stays refused.
            Err(Refusal::Declined) => {
                tracing::warn!(order = request.order, "card declined, payment dropped");
                Err(HandlerOutcome::drop())
            }
        }
    }
}
// --8<-- [end:charge]

// --8<-- [start:routes]
/// The payments side's subscriber: five deliveries at most, then the dead-letter topic.
pub(crate) fn routes() -> impl RouterDef<MemoryBroker, Shop> {
    Router::new()
        .with_codec(INTERNAL)
        .include(
            subscriber(PaymentRequest::DESTINATION, Charge)
                .describe("Charges an order through the payment provider.")
                .reply()
                .build(),
        )
        .max_attempts(nonzero!(5u32))
        .dead_letter("payments.dead")
        .out_reply(Publish)
        .codec(INTERNAL)
        .build()
}
// --8<-- [end:routes]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use ruststream::codec::CborCodec;
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{BACKOFF, Gateway, Paid, PaymentRequest};
    use crate::orders::Line;
    use crate::service::{shop, test_broker};
    use crate::state::Shop;

    fn request(order: u64, amount_cents: u64) -> PaymentRequest {
        PaymentRequest {
            order,
            customer: "ada@example.com".to_owned(),
            lines: vec![Line {
                sku: "mug".to_owned(),
                quantity: 1,
                price_cents: amount_cents,
            }],
            amount_cents,
            request_id: format!("req-{order}"),
        }
    }

    // --8<-- [start:busy]
    #[tokio::test(start_paused = true)]
    async fn a_busy_provider_is_asked_again_after_the_backoff() -> Result<(), Box<dyn Error>> {
        // A provider that takes one charge a second.
        let state = Shop {
            gateway: Arc::new(Gateway::new(100_000, 1)),
            ..Shop::open().await?
        };
        let tb = TestApp::with_state(shop(test_broker()), |_| state).await?;
        let broker = tb.broker::<MemoryBroker>();
        for order in [1, 2] {
            broker
                .message(&request(order, 500))
                .with_codec(CborCodec)
                .publish()
                .await?;
        }
        broker
            .subscriber("payments.requested")
            .assert_called(2)
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;
        broker
            .subscriber("payments.requested")
            .assert_called(3)
            .settled(HandlerOutcome::ack());
        broker
            .published::<Paid>("payments.settled")
            .assert_called(2);
        Ok(())
    }
    // --8<-- [end:busy]

    // --8<-- [start:dead_letter]
    #[tokio::test]
    async fn a_silent_provider_sends_the_request_to_the_dead_letter_topic()
    -> Result<(), Box<dyn Error>> {
        let state = Shop::open().await?;
        state.gateway.go_offline();
        let tb = TestApp::with_state(shop(test_broker()), |_| state).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&request(3, 500))
            .with_codec(CborCodec)
            .publish()
            .await?;

        broker.subscriber("payments.requested").assert_called(5);
        broker
            .published::<PaymentRequest>("payments.dead")
            .assert_called_once()
            .with_codec(&CborCodec, &request(3, 500));
        Ok(())
    }
    // --8<-- [end:dead_letter]

    #[tokio::test]
    async fn a_declined_card_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(shop(test_broker())).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&request(4, 1_000_000))
            .with_codec(CborCodec)
            .publish()
            .await?;

        broker
            .subscriber("payments.requested")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        broker
            .published::<Paid>("payments.settled")
            .assert_not_called();
        Ok(())
    }
}
