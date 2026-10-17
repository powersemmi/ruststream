//! The notifications side of the shop: what the shop tells a customer, and the subscriber that
//! e-mails it.

use std::fmt;
use std::sync::Arc;

use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::state::Shop;

// --8<-- [start:notice]
/// What the shop tells a customer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, Outgoing)]
#[outgoing(name = "notices")]
pub(crate) struct Notice {
    pub(crate) customer: String,
    pub(crate) text: String,
}

/// The slot a subscriber tells customers through, and what may leave through it.
#[derive(OutSlot)]
#[publishes(Notice)]
pub(crate) struct Notices;
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
// The mailer comes from the app's state; the context names the subscription in the warning.
#[subscriber("notices")]
async fn send(
    notice: &Notice,
    ctx: &mut Context<'_, (), Shop>,
    State(mailer): State<Arc<Mailer>>,
) -> HandlerOutcome {
    match mailer.send(&notice.customer, &notice.text).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) => {
            // A refused mailbox stays refused; another try would only repeat the warning.
            tracing::warn!(subscription = ctx.name(), %error, "notice not sent");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:send]

/// The notifications side's subscriber.
pub(crate) fn routes() -> impl RouterDef<MemoryBroker, Shop> {
    Router::new().include(send)
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
