//! Waits of the harness that a task keeping itself runnable would never let end.
//!
//! A paused clock moves only when the runtime has nothing runnable, so a task parked on a timer
//! lets it land exactly on the deadline, while a task that keeps itself runnable stops it for good.
//! The harness tells the two apart by the runner's own count of runnable tasks. While none is
//! runnable it waits as it always did. Once a task has stayed runnable for a bounded number of
//! turns it moves the clock itself, as real time would pass, so a shutdown timeout fires.

use std::pin::pin;
use std::time::Duration;

use tokio::select;
use tokio::task;
use tokio::time;

use ruststream_runtime::testing::TestError;
use ruststream_runtime_tokio::Quiescence;

use super::clock_is_paused;

/// How many consecutive turns a task may stay runnable before the harness looks at the clock.
const SPIN_TURNS: usize = 1_000;

/// How many turns the clock advances by one tick, before the tick doubles.
const ROUND_TURNS: usize = 1_000;

/// How many times the tick doubles before the wait gives up: from a millisecond, the clock has by
/// then passed about twelve days.
const ROUNDS: usize = 20;

/// The first tick, the timer wheel's own resolution.
const FIRST_TICK: Duration = Duration::from_millis(1);

/// Awaits `wait`, and fails with [`TestError::KeptRunning`] when it cannot end because a task of the
/// app keeps itself runnable.
///
/// # Errors
///
/// Returns [`TestError::KeptRunning`] when `wait` is still pending after every round.
pub(crate) async fn until_idle<Wait: Future>(
    quiescence: &Quiescence,
    wait: Wait,
) -> Result<Wait::Output, TestError> {
    let mut wait = pin!(wait);
    // Turns in a row that a task of the app was runnable.
    let mut busy = 0_usize;
    let mut tick = FIRST_TICK;
    let mut paused = None;
    loop {
        // Interest is registered before the count is read, so a task that becomes runnable in
        // between still ends the park below.
        let woken = quiescence.woken();
        let mut woken = pin!(woken);
        woken.as_mut().enable();
        if quiescence.runnable() == 0 {
            busy = 0;
            tick = FIRST_TICK;
            select! {
                biased;
                output = &mut wait => return Ok(output),
                () = &mut woken => {}
            }
            continue;
        }
        busy += 1;
        if busy <= SPIN_TURNS {
            select! {
                biased;
                output = &mut wait => return Ok(output),
                () = task::yield_now() => {}
            }
            continue;
        }
        // Why the clock is looked at only now: on a running clock time passes by itself and a
        // timeout the wait is bounded by fires; only a paused clock is the harness's to move.
        if !*paused.get_or_insert_with(clock_is_paused) {
            return Ok(wait.await);
        }
        let round = (busy - SPIN_TURNS) / ROUND_TURNS;
        if round >= ROUNDS {
            return Err(TestError::KeptRunning { turns: busy });
        }
        if (busy - SPIN_TURNS).is_multiple_of(ROUND_TURNS) && round > 0 {
            tick *= 2;
        }
        select! {
            biased;
            output = &mut wait => return Ok(output),
            () = time::advance(tick) => {}
        }
    }
}
