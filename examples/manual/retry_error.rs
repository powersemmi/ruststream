//! The services of `examples/retry_error.rs` written without the `macros` feature: the `.error`
//! continuation on an outcome and on the context, when a publish the runtime makes fails. The
//! tests are the ones of `examples/retry_error.rs`.
//!
//! ```text
//! cargo test --example manual_retry_error --no-default-features --features testing,memory,json,tokio
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::ready;
use std::io;

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, LogMode};
use ruststream::{Lend, OutgoingMessage, PairError, PublishMessageError, PublishOutcome};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Payment {
    id: u64,
}

// `#[outgoing(name = "payments")]` by hand: the type fixes its destination.
impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

/// What the alert subject receives.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Alert {
    destination: String,
    cause: String,
}

impl OutgoingDestination for Alert {
    type Form = FixedName;

    const DESTINATION: &'static str = "alerts";
}

impl MessageHeaders for Alert {
    type Contract = NoHeaders;
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
    use std::future::ready;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Alert, DownPublish, Payment};

    /// The alerts the handler raised, kept where the test reads them.
    type Alerts = Arc<Mutex<Vec<Alert>>>;

    // --8<-- [start:error]
    const RECHECK: Duration = Duration::from_secs(5);

    /// The outcome carries the `.error`; a plain `Ok(())` acks with none.
    struct Reconcile;

    impl Handle<Payment, (), (), (), Alerts> for Reconcile {
        fn handle(
            &self,
            payment: &Payment,
            _outs: &(),
            ctx: &mut Context<'_, (), Alerts>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            let id = payment.id;
            let alerts = Arc::clone(ctx.state());
            ready(Err(HandlerOutcome::retry_after(RECHECK).error(
                async move |failures: Failures| {
                    for failed in failures {
                        let (err, lost) = failed.into_parts();
                        // The dead-letter copy never left: record where it was going and why.
                        tracing::error!(id, %err, destination = lost.destination(), "dead letter lost");
                        alerts.lock().expect("alerts").push(Alert {
                            destination: lost.destination().to_owned(),
                            cause: err.to_string(),
                        });
                    }
                },
            )))
        }
    }
    // --8<-- [end:error]

    fn app(alerts: &Alerts) -> impl App<State = Alerts> {
        let alerts = Arc::clone(alerts);
        RustStream::new(AppInfo::new("payments", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(alerts))
            .with_broker(MemoryBroker::new(), |b| {
                // The first attempt is the cap, so this retry becomes a dead letter.
                b.include(subscriber("payments", Reconcile).build())
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
    use std::future::ready;
    use std::sync::{Arc, Mutex};

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    use super::DownPublish;

    /// What the handler recorded about the quotes it could not send.
    type Notes = Arc<Mutex<Vec<String>>>;

    #[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
    struct PriceRequest {
        id: u64,
    }

    impl OutgoingDestination for PriceRequest {
        type Form = FixedName;

        const DESTINATION: &'static str = "prices";
    }

    impl MessageHeaders for PriceRequest {
        type Contract = NoHeaders;
    }

    #[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
    struct Quote {
        id: u64,
    }

    // `#[outgoing(name = "quotes")]` by hand.
    impl OutgoingDestination for Quote {
        type Form = FixedName;

        const DESTINATION: &'static str = "quotes";
    }

    impl MessageHeaders for Quote {
        type Contract = NoHeaders;
    }

    // --8<-- [start:reply]
    /// The `.error` is registered on the context, for the settlement the reply goes with.
    struct Answer;

    impl Handle<PriceRequest, Quote, (), (), Notes> for Answer {
        fn handle(
            &self,
            req: &PriceRequest,
            _outs: &(),
            ctx: &mut Context<'_, (), Notes>,
        ) -> impl Future<Output = Result<Quote, HandlerOutcome>> {
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
            ready(Ok(Quote { id }))
        }
    }
    // --8<-- [end:reply]

    fn app(notes: &Notes) -> impl App<State = Notes> {
        let notes = Arc::clone(notes);
        RustStream::new(AppInfo::new("prices", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(notes))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(subscriber("prices", Answer).reply().build())
                    .out_reply(DownPublish)
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

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
