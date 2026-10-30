//! The tokio runner of [RustStream](https://docs.rs/ruststream): it runs the shared delivery body
//! of `ruststream-runtime` on tokio, and owns what is tokio's: the app's lifecycle, spawning, the
//! worker pools and dedicated threads, the signals, the main runtime handle and the clock.
//!
//! Services use these items through the `ruststream` facade, which re-exports them under the
//! paths its documentation teaches:
//!
//! - the crate root beside the shared body, as
//!   [`ruststream::runtime`](https://docs.rs/ruststream/latest/ruststream/runtime/);
//! - [`time`] as [`ruststream::time`](https://docs.rs/ruststream/latest/ruststream/time/).

mod clock;
pub mod time;

pub use clock::Tokio;
