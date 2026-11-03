//! Cross-broker publisher tokens on the embedded runner: a publish policy bound to another
//! broker of the same app.
//!
//! A bare policy attached at an include site pairs with that scope's own broker. To publish to a
//! different broker, wrap it in [`Bindable`] and mint [`Bound`] tokens from it before the scopes
//! are built. A token is a plain [`PublishPolicy`](ruststream_core::PublishPolicy): it rides the
//! mount chain like any other, and pairs against its own broker once the app has connected it. The
//! slot it reads is filled once at start, so a token adds nothing per message.
//!
//! With `alloc` the wrapper shares the connected form through a counted pointer. Without it the
//! user declares the slot, a [`BindCell`] in a `static`, and hands the wrapper a reference to it.

#[cfg(ruststream_alloc)]
mod heap;
#[cfg(ruststream_alloc)]
pub use heap::{Bindable, Bound};

#[cfg(not(ruststream_alloc))]
mod cell;
#[cfg(not(ruststream_alloc))]
pub use cell::{BindCell, Bindable, Bound};
