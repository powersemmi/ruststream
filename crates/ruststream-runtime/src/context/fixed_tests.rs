//! The behaviours of the build without `alloc`: a delivery's registrations and failure buffer are
//! fixed.
use core::pin::pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context as TaskContext, Waker};

use ruststream_core::Cause;

use super::Context;
use crate::HeaderSet;
use crate::dispatch::Delivery;
use crate::handler::HandlerOutcome;
use crate::lifecycle::BoxError;
use crate::publish_failure::{ErrorHook, FailedMessage, Failures, Reaction, publish_lost};
use crate::registrations::{REGISTRATIONS, RegistrationsFull};

#[test]
fn registering_past_the_fixed_table_is_refused_and_keeps_what_was_registered() {
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderSet::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);

    for _ in 0..REGISTRATIONS {
        let after = ctx
            .after(HandlerOutcome::ack())
            .expect("a free registration");
        after.then(async {});
    }

    assert!(matches!(
        ctx.after(HandlerOutcome::ack()),
        Err(RegistrationsFull)
    ));
    assert_eq!(
        ctx.take_hooks_for(super::HandlerResult::Ack).len(),
        REGISTRATIONS
    );
}

#[test]
fn the_failure_of_a_delivery_outlives_its_context_for_the_lane_to_react_to() {
    static SEEN: AtomicUsize = AtomicUsize::new(0);
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderSet::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    assert!(ctx.take_failures().is_none());
    let hook = ErrorHook::new(async |failures: &Failures| {
        SEEN.fetch_add(failures.len(), Ordering::SeqCst);
    });
    publish_lost(
        Some(Reaction::new(hook, None)),
        "orders",
        "replies",
        "Reply",
        BoxError::from(Cause::new("down", 7)),
        ctx.failure_slot(),
        || FailedMessage::new("replies", None, HeaderSet::new()),
    );

    let taken = ctx.take_failures();
    drop(ctx);
    let mut taken = taken.expect("a failure waits");
    let mut react = pin!(taken.react());

    assert!(
        react
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_ready()
    );
    assert_eq!(SEEN.load(Ordering::SeqCst), 1);
}
