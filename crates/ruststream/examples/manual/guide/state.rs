//! What the shop's subscribers share, without the `macros` feature: the field projections
//! `#[derive(FromRef)]` would write are spelled out.

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
#[derive(Clone)]
pub(crate) struct Shop {
    pub(crate) gateway: Arc<Gateway>,
    pub(crate) mailer: Arc<Mailer>,
}

// What `#[derive(FromRef)]` writes for each field: `State<Arc<Gateway>>` resolves through it.
impl FromRef<Shop> for Arc<Gateway> {
    fn from_ref(shop: &Shop) -> Self {
        shop.gateway.clone()
    }
}

impl FromRef<Shop> for Arc<Mailer> {
    fn from_ref(shop: &Shop) -> Self {
        shop.mailer.clone()
    }
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
