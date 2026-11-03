//! Time for service code: the sleep that runs on the app's clock.
//!
//! A handler that waits sleeps through [`sleep`], not through its runtime's timer directly: the
//! sleep is a timer of the clock the app runs on, so the test harness moves it with
//! [`advance`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.advance)
//! like every timer the framework arms.

use core::time::Duration;

use ruststream_runtime::Clock;

use crate::Tokio;

/// Waits until `duration` has passed on the app's clock.
///
/// The timer is the runtime's own future, held in the caller's state: a sleep costs what the
/// runtime's timer costs and nothing more.
///
/// # Examples
///
/// A handler that backs off before it answers; under the harness, `advance` moves the sleep:
///
/// ```
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use ruststream::time;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn throttled(order: &Order) -> HandlerOutcome {
///     time::sleep(Duration::from_millis(250)).await;
///     if order.id == 0 {
///         return HandlerOutcome::drop();
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// # fn main() {}
/// ```
pub fn sleep(duration: Duration) -> impl Future<Output = ()> + Send + 'static {
    Tokio.sleep(duration)
}
