//! The cooperative yield after each delivery.

// The tasks are local to their core: they hold an `Rc`, and compio never moves them.
#![allow(clippy::future_not_send)]

use std::cell::RefCell;
use std::rc::Rc;

use compio::runtime::{self, Runtime};
use ruststream_runtime_compio::yield_now;

/// A task that never waits on anything but its own yields.
async fn busy(name: char, turns: usize, log: Rc<RefCell<String>>) {
    for _ in 0..turns {
        log.borrow_mut().push(name);
        yield_now().await;
    }
}

#[test]
fn a_busy_task_gives_the_others_a_turn_after_each_yield() {
    let log = Rc::new(RefCell::new(String::new()));
    Runtime::new().expect("a compio runtime").block_on(async {
        let a = runtime::spawn(busy('a', 3, Rc::clone(&log)));
        let b = runtime::spawn(busy('b', 3, Rc::clone(&log)));
        a.await.expect("a ends");
        b.await.expect("b ends");
    });
    assert_eq!(*log.borrow(), "ababab");
}

#[test]
fn a_yield_ends_after_one_turn() {
    Runtime::new().expect("a compio runtime").block_on(async {
        // Alone on the core, the task is polled again at once.
        yield_now().await;
    });
}
