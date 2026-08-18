
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use futures::future::join_all;

use super::Context;
use crate::Headers;
use crate::runtime::dispatch::Delivery;
use crate::runtime::handler::HandlerResult;

fn run_all(continuations: Vec<super::Continuation>) {
    futures::executor::block_on(async {
        join_all(continuations).await;
    });
}

#[test]
fn outcome_kind_distinguishes_drop_retry_and_retry_after() {
    use super::OutcomeKind;
    assert_eq!(OutcomeKind::of(HandlerResult::Ack), OutcomeKind::Ack);
    // drop (nack, no requeue) and retry (nack, requeue) are distinct kinds.
    assert_eq!(OutcomeKind::of(HandlerResult::drop()), OutcomeKind::Drop);
    assert_eq!(OutcomeKind::of(HandlerResult::retry()), OutcomeKind::Retry);
    assert_ne!(
        OutcomeKind::of(HandlerResult::drop()),
        OutcomeKind::of(HandlerResult::retry()),
    );
    // retry_after is its own kind, distinct from retry, and matches regardless of the delay.
    assert_eq!(
        OutcomeKind::of(HandlerResult::retry_after(Duration::from_secs(1))),
        OutcomeKind::RetryAfter,
    );
    assert_ne!(
        OutcomeKind::of(HandlerResult::retry_after(Duration::ZERO)),
        OutcomeKind::of(HandlerResult::retry()),
    );
    assert_eq!(
        OutcomeKind::of(HandlerResult::retry_after(Duration::from_secs(1))),
        OutcomeKind::of(HandlerResult::retry_after(Duration::from_secs(9))),
    );
}

#[test]
fn the_debug_forms_report_the_subscription_and_pending_work() {
    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);

    let rendered = format!("{ctx:?}");
    assert!(rendered.contains("orders"), "{rendered}");
    assert!(rendered.contains("after_hooks: 0"), "{rendered}");

    let after = ctx.after(HandlerResult::Ack);
    // The gate is what decides whether the continuation runs, so Debug must name it.
    assert!(format!("{after:?}").contains("Ack"));

    ctx.after_settle(async {});
    assert!(format!("{ctx:?}").contains("after_hooks: 1"));
    run_all(ctx.take_hooks_for(HandlerResult::Ack));
}

#[test]
fn take_hooks_runs_only_the_matching_gate() {
    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("t", &headers, &state, (), &delivery);

    let acked = Arc::new(AtomicU32::new(0));
    let dropped = Arc::new(AtomicU32::new(0));
    let retried = Arc::new(AtomicU32::new(0));
    let settled = Arc::new(AtomicU32::new(0));

    let bump = |c: &Arc<AtomicU32>| {
        let c = Arc::clone(c);
        async move {
            c.fetch_add(1, Ordering::SeqCst);
        }
    };

    ctx.after(HandlerResult::Ack).then(bump(&acked));
    ctx.after(HandlerResult::drop()).then(bump(&dropped));
    ctx.after(HandlerResult::retry()).then(bump(&retried));
    ctx.after_ack(bump(&acked));
    ctx.after_settle(bump(&settled));

    // Settling with Ack runs both ack-gated hooks and the ungated one, not drop or retry.
    run_all(ctx.take_hooks_for(HandlerResult::Ack));
    assert_eq!(acked.load(Ordering::SeqCst), 2);
    assert_eq!(settled.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(retried.load(Ordering::SeqCst), 0);

    // A retry settle runs only the retry-gated hook: drop and retry are distinct mechanics.
    run_all(ctx.take_hooks_for(HandlerResult::retry()));
    assert_eq!(retried.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);

    // The drop-gated hook is still registered: a later drop settle runs it.
    run_all(ctx.take_hooks_for(HandlerResult::drop()));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(retried.load(Ordering::SeqCst), 1);
}

#[test]
fn take_settle_hooks_drops_outcome_gated_ones() {
    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("t", &headers, &state, (), &delivery);

    let gated = Arc::new(AtomicU32::new(0));
    let ungated = Arc::new(AtomicU32::new(0));

    let gated_clone = Arc::clone(&gated);
    ctx.after(HandlerResult::Ack).then(async move {
        gated_clone.fetch_add(1, Ordering::SeqCst);
    });
    let ungated_clone = Arc::clone(&ungated);
    ctx.after_settle(async move {
        ungated_clone.fetch_add(1, Ordering::SeqCst);
    });

    // The batch path drops outcome-gated hooks (per-element outcomes), keeps ungated ones.
    run_all(ctx.take_settle_hooks());
    assert_eq!(ungated.load(Ordering::SeqCst), 1);
    assert_eq!(gated.load(Ordering::SeqCst), 0);
}

#[test]
fn context_reads_typed_field_by_key() {
    use crate::Field;

    struct Meta {
        offset: u64,
    }
    #[derive(Clone, Copy)]
    struct Offset;
    impl Field<Meta> for Offset {
        type Value<'a> = u64;
        fn get(self, m: &Meta) -> u64 {
            m.offset
        }
    }

    let state = String::from("app");
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let ctx = Context::new("test", &headers, &state, Meta { offset: 42 }, &delivery);

    // The typed broker field is read by key, straight off the context.
    assert_eq!(ctx.context(Offset), 42);
    // App state is reached only through state(), independent of the per-delivery context.
    assert_eq!(ctx.state().as_str(), "app");
}

#[test]
fn set_writes_scratch_and_reads_it_back() {
    use crate::{Field, FieldMut};

    #[derive(Default)]
    struct Scratch {
        user: Option<u64>,
    }
    #[derive(Clone, Copy)]
    struct User;
    impl Field<Scratch> for User {
        type Value<'a> = Option<&'a u64>;
        fn get(self, s: &Scratch) -> Option<&u64> {
            s.user.as_ref()
        }
    }
    impl FieldMut<Scratch> for User {
        type Owned = u64;
        fn set(self, s: &mut Scratch, value: u64) {
            s.user = Some(value);
        }
    }

    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("test", &headers, &state, Scratch::default(), &delivery);

    assert_eq!(ctx.context(User), None);
    ctx.set(User, 9);
    assert_eq!(ctx.context(User), Some(&9));
}

#[test]
fn headers_clone_only_on_first_mutation() {
    let mut original = Headers::new();
    original.insert("k", "v");
    let state = ();
    let delivery = Delivery::empty();
    let mut ctx = Context::new("test", &original, &state, (), &delivery);

    // Untouched: the context borrows the message headers, no copy exists.
    assert!(std::ptr::eq(ctx.headers(), &raw const original));

    ctx.headers_mut().insert("added", "1");
    ctx.headers_mut().insert("added2", "2");

    // Mutations land in one lazily-made copy; the original is untouched.
    assert!(!std::ptr::eq(ctx.headers(), &raw const original));
    assert_eq!(ctx.headers().get("added"), Some(&b"1"[..]));
    assert_eq!(ctx.headers().get("k"), Some(&b"v"[..]));
    assert_eq!(original.get("added"), None);
}
