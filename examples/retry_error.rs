//! A dead-letter copy or a reply the broker refuses: the handler's `.error` continuation learns of
//! it and records what was lost. Each scenario is a test.
//!
//! ```text
//! cargo test --example retry_error --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::ready;
use std::io;

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, LogMode};
use ruststream::{Lend, OutgoingMessage, PairError, PublishMessageError, PublishOutcome};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
}

/// What the alert subject receives.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    destination: String,
    cause: String,
}

/// A publisher whose broker is down.
#[derive(Clone)]
struct Down;

impl Publisher for Down {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
        // The broker is down: the message did not go out, so it comes back with the error.
        ready(Err(PublishMessageError::with_message(
            io::Error::other("broker down"),
            msg,
        )))
    }
}

/// The policy that pairs into [`Down`].
struct DownPublish;

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for DownPublish {
    type Live = Down;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<Down, PairError>> {
        ready(Ok(Down))
    }
}

mod error_continuation {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Alert, DownPublish, Payment};

    /// The alerts the handler raised, kept where the test reads them.
    type Alerts = Arc<Mutex<Vec<Alert>>>;

    // --8<-- [start:error]
    const RECHECK: Duration = Duration::from_secs(5);

    #[subscriber("payments")]
    async fn reconcile(payment: &Payment, ctx: &mut Context<'_, (), Alerts>) -> HandlerOutcome {
        let id = payment.id;
        let alerts = Arc::clone(ctx.state());
        HandlerOutcome::retry_after(RECHECK).error(async move |failures: Failures| {
            for failed in failures {
                let (err, lost) = failed.into_parts();
                // The dead-letter copy never left: record where it was going and why.
                tracing::error!(id, %err, destination = lost.destination(), "dead letter lost");
                alerts.lock().expect("alerts").push(Alert {
                    destination: lost.destination().to_owned(),
                    cause: err.to_string(),
                });
            }
        })
    }
    // --8<-- [end:error]

    fn app(alerts: &Alerts) -> impl App<State = Alerts> {
        let alerts = Arc::clone(alerts);
        RustStream::new(AppInfo::new("payments", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(alerts))
            .with_broker(MemoryBroker::new(), |b| {
                // The first attempt is the cap, so this retry becomes a dead letter.
                b.include(reconcile)
                    .max_attempts(nonzero!(1u32))
                    .dead_letter("payments.dead")
                    .out_retry(DownPublish)
            })
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_dead_letter_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let alerts = Alerts::default();
        let tb = TestApp::start(app(&alerts)).await?;

        tb.message(&Payment { id: 7 }).publish().await?;
        tb.settle().await?;

        assert_eq!(
            *alerts.lock().expect("alerts"),
            [Alert {
                destination: "payments.dead".to_owned(),
                cause: "broker down".to_owned(),
            }]
        );
        tb.broker::<MemoryBroker>()
            .subscriber("payments")
            .assert_called_once();
        Ok(())
    }
}

mod reply_error {
    use std::convert::Infallible;
    use std::error::Error;
    use std::sync::{Arc, Mutex};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::DownPublish;

    /// What the handler recorded about the quotes it could not send.
    type Notes = Arc<Mutex<Vec<String>>>;

    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "prices")]
    struct PriceRequest {
        id: u64,
    }

    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "quotes")]
    struct Quote {
        id: u64,
    }

    // --8<-- [start:reply]
    #[subscriber("prices", reply)]
    async fn quote(req: &PriceRequest, ctx: &mut Context<'_, (), Notes>) -> Quote {
        let id = req.id;
        let notes = Arc::clone(ctx.state());
        ctx.after(HandlerOutcome::ack())
            .error(async move |failures: Failures| {
                for failed in failures {
                    tracing::warn!(id, error = %failed.error(), "the quote was not sent");
                    notes
                        .lock()
                        .expect("notes")
                        .push(format!("{id}: {}", failed.error()));
                }
            });
        Quote { id }
    }
    // --8<-- [end:reply]

    fn app(notes: &Notes) -> impl App<State = Notes> {
        let notes = Arc::clone(notes);
        RustStream::new(AppInfo::new("prices", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(notes))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(quote).out_reply(DownPublish)
            })
    }

    #[tokio::test]
    async fn a_lost_quote_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let notes = Notes::default();
        let tb = TestApp::start(app(&notes)).await?;

        tb.message(&PriceRequest { id: 7 }).publish().await?;
        tb.settle().await?;

        assert_eq!(*notes.lock().expect("notes"), ["7: broker down"]);
        tb.broker::<MemoryBroker>()
            .subscriber("prices")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}

fn main() {}
