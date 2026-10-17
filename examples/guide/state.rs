//! What the shop's subscribers share: the clients opened once, at startup.

use std::sync::Arc;

use ruststream::memory::prelude::*;

use crate::notifications::{MailError, Mailer};
use crate::payments::Gateway;

/// The card limit the payment provider enforces, in cents.
const CARD_LIMIT_CENTS: u64 = 100_000;
/// How many charges a second the payment provider accepts.
const CHARGES_PER_SECOND: u32 = 50;

// --8<-- [start:state]
/// The app's state: each field reaches a subscriber as `State<FieldType>`.
#[derive(Clone, FromRef)]
pub(crate) struct Shop {
    pub(crate) gateway: Arc<Gateway>,
    pub(crate) mailer: Arc<Mailer>,
}

impl Shop {
    /// Opens the clients. The app runs it once, before the first subscription opens; an error
    /// stops the startup.
    pub(crate) async fn open() -> Result<Self, MailError> {
        Ok(Self {
            gateway: Arc::new(Gateway::new(CARD_LIMIT_CENTS, CHARGES_PER_SECOND)),
            mailer: Arc::new(Mailer::connect("smtp://mail.internal:25").await?),
        })
    }
}
// --8<-- [end:state]
