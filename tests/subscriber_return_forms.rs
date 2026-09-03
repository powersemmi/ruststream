//! The settling return forms of `#[subscriber]`: a handler body is checked against the type its
//! own signature declares, exactly as a plain function is, so a `Result` whose error type
//! nothing inside the body pins still compiles - and each form lowers to the settlement the
//! dispatcher acts on.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use std::num::ParseIntError;

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Outgoing, Serialize, Deserialize)]
struct Ticket {
    label: String,
}

/// The settlement named outright.
#[subscriber("returns.plain")]
async fn settle_plain(ticket: &Ticket) -> HandlerOutcome {
    let _ = &ticket.label;
    HandlerOutcome::ack()
}

/// No return type at all: the body settles by finishing.
#[subscriber("returns.unit")]
async fn settle_unit(ticket: &Ticket) {
    let _ = &ticket.label;
}

/// A `Result` the body never propagates through: with no `?` anywhere, the declared error type
/// is the only thing that names it, so a bare `Ok(())` compiles only while the expansion keeps
/// the signature's return type on the body.
#[subscriber("returns.ok")]
async fn settle_ok(ticket: &Ticket) -> Result<(), HandlerOutcome> {
    let _ = &ticket.label;
    Ok(())
}

/// The `?` form: a propagated error refuses the delivery.
#[subscriber("returns.parsed")]
async fn settle_parsed(ticket: &Ticket) -> Result<(), ParseIntError> {
    let _: u32 = ticket.label.parse()?;
    Ok(())
}

/// An outcome behind a `Result`: `Ok` carries the settlement the body chose, `Err` refuses.
#[subscriber("returns.outcome")]
async fn settle_outcome(ticket: &Ticket) -> Result<HandlerOutcome, ParseIntError> {
    let count: u32 = ticket.label.parse()?;
    if count == 0 {
        return Ok(HandlerOutcome::drop());
    }
    Ok(HandlerOutcome::ack())
}

fn ticket(label: &str) -> Ticket {
    Ticket {
        label: label.to_owned(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_settling_return_form_acks_a_finished_body() {
    let app =
        RustStream::new(AppInfo::new("returns", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(settle_plain);
            b.include(settle_unit);
            b.include(settle_ok);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    for name in ["returns.plain", "returns.unit", "returns.ok"] {
        tb.message(&ticket("7"))
            .to(name)
            .publish()
            .await
            .expect("inject");
        tb.broker::<MemoryBroker>()
            .subscriber(name)
            .assert_called_once()
            .settled(HandlerOutcome::ack());
    }

    tb.shutdown().await.expect("graceful shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_returning_body_settles_by_the_arm_it_takes() {
    let app =
        RustStream::new(AppInfo::new("returns", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(settle_parsed);
            b.include(settle_outcome);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // The error a body propagates with `?` is a refusal, whatever its type.
    tb.message(&ticket("not a number"))
        .to("returns.parsed")
        .publish()
        .await
        .expect("inject");
    tb.broker::<MemoryBroker>()
        .subscriber("returns.parsed")
        .assert_called_once()
        .settled(HandlerOutcome::drop());

    // The `Ok` arm hands the wrapper the settlement the body chose, both ways round.
    tb.message(&ticket("3"))
        .to("returns.outcome")
        .publish()
        .await
        .expect("inject");
    tb.broker::<MemoryBroker>()
        .subscriber("returns.outcome")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.message(&ticket("0"))
        .to("returns.outcome")
        .publish()
        .await
        .expect("inject");
    tb.broker::<MemoryBroker>()
        .subscriber("returns.outcome")
        .assert_called(2)
        .settled(HandlerOutcome::drop());

    tb.shutdown().await.expect("graceful shutdown");
}
