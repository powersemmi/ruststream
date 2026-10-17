//! The notifications side of the shop without the `macros` feature: the slot marker declares
//! what may leave through it with the trait impls `#[derive(OutSlot)]` would write.

use std::any::type_name;
use std::fmt;
use std::sync::Arc;

use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::state::Shop;

// --8<-- [start:notice]
/// What the shop tells a customer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub(crate) struct Notice {
    pub(crate) customer: String,
    pub(crate) text: String,
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "notices")]` writes.
impl OutgoingDestination for Notice {
    type Form = FixedName;

    const DESTINATION: &'static str = "notices";
}

impl MessageHeaders for Notice {
    type Contract = NoHeaders;
}

impl MessageInfo for Notice {
    const NAME: &'static str = "Notice";
    const DESCRIPTION: Option<&'static str> = Some("What the shop tells a customer.");
}

/// The slot a subscriber tells customers through, and what may leave through it.
pub(crate) struct Notices;

// What `#[derive(OutSlot)]` with `#[publishes(Notice)]` writes.
impl OutSlot for Notices {
    const NAME: &'static str = "Notices";
    // `Notice` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;

    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        <Notice as OutMessages<Self>>::outgoing()
    }
}

impl PublishedThrough<Notices> for Notice {}

impl<Slot: OutSlot> OutMessages<Slot> for Notice {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            Self::DESTINATION,
            type_name::<Self>(),
        )]
    }
}
// --8<-- [end:notice]

/// The mail server refused a message.
#[derive(Debug)]
pub(crate) struct MailError(String);

impl fmt::Display for MailError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the mail server refused the message: {}", self.0)
    }
}

impl std::error::Error for MailError {}

/// Stands in for an SMTP client: it refuses an address with no `@` and logs what it sends.
#[derive(Debug)]
pub(crate) struct Mailer {
    server: String,
}

impl Mailer {
    /// Opens a connection to the mail server.
    pub(crate) async fn connect(server: &str) -> Result<Self, MailError> {
        tokio::task::yield_now().await;
        Ok(Self {
            server: server.to_owned(),
        })
    }

    /// Sends one message.
    pub(crate) async fn send(&self, to: &str, text: &str) -> Result<(), MailError> {
        tokio::task::yield_now().await;
        if !to.contains('@') {
            return Err(MailError(format!("no mailbox at {to:?}")));
        }
        tracing::info!(server = %self.server, to, text, "mail sent");
        Ok(())
    }
}

// --8<-- [start:send]
/// E-mails a notice to the customer.
struct Mail;

// The mailer comes from the app's state; the context names the subscription in the warning.
impl Handle<Notice, (), (), (), Shop> for Mail {
    async fn handle(
        &self,
        notice: &Notice,
        _outs: &(),
        ctx: &mut Context<'_, (), Shop>,
    ) -> Result<(), HandlerOutcome> {
        let State(mailer) = State::<Arc<Mailer>>::from_context(ctx).await?;
        if let Err(error) = mailer.send(&notice.customer, &notice.text).await {
            // A refused mailbox stays refused; another try would only repeat the warning.
            tracing::warn!(subscription = ctx.name(), %error, "notice not sent");
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}
// --8<-- [end:send]

/// The notifications side's subscriber.
pub(crate) fn routes() -> impl RouterDef<MemoryBroker, Shop> {
    Router::new().include(
        subscriber(Notice::DESTINATION, Mail)
            .describe("E-mails a notice to the customer.")
            .build(),
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Notice;
    use crate::service::shop;

    #[tokio::test]
    async fn a_notice_to_a_mailbox_that_does_not_exist_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(shop(MemoryBroker::new())).await?;
        let notice = Notice {
            customer: "nobody".to_owned(),
            text: "We have your order 9.".to_owned(),
        };
        tb.message(&notice).publish().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("notices")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
}
