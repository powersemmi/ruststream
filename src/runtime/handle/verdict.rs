//! The computed verdict of a [`Handle`](super::Handle) body: one canonical return shape per
//! input family, fixed by the trait itself.
//!
//! The shape is computed, not chosen: the input axis picks the family
//! ([`OneByOne`] / [`Batched`]) and the reply parameter `R` is the `Ok` side, so an illegal
//! combination (a batch body with a single-message verdict, a reply from a body that declared
//! none) has no type to be written in, and `async fn handle` bodies match the trait signature
//! exactly. The canonical spellings:
//!
//! - one-by-one: `Result<(), HandlerOutcome>`, and `Result<Reply, HandlerOutcome>` for a reply
//!   body;
//! - batched: `Result<(), Vec<HandlerOutcome>>` (the `Err` vector settles element-wise and is
//!   batch-length by contract), and `Result<Vec<Reply>, Vec<HandlerOutcome>>` for a batch reply
//!   body (one reply per element).
//!
//! An [`and_after`](crate::runtime::HandlerOutcome::and_after) continuation rides the outcome
//! itself, so it stays a return capability on every shape. Per-element vectors are batch-length
//! by contract; the adapters check once per batch and panic on a mismatch (see the module docs
//! of [`handle`](super)).

use crate::runtime::handler::HandlerOutcome;

use super::axis::{Axis, Input};

/// The single-delivery verdict family: one input, one settlement (and at most one reply).
#[derive(Debug, Clone, Copy)]
pub struct OneByOne;

/// The batch verdict family: one input slice, per-element settlement.
#[derive(Debug, Clone, Copy)]
pub struct Batched;

/// The error side of one family's verdict. Machinery behind [`Handle`](super::Handle); never
/// named in user code.
#[doc(hidden)]
pub trait VerdictFamily {
    /// One [`HandlerOutcome`] one-by-one; a batch-length vector of them per batch.
    type Bad;
}

impl VerdictFamily for OneByOne {
    type Bad = HandlerOutcome;
}

impl VerdictFamily for Batched {
    type Bad = Vec<HandlerOutcome>;
}

/// The canonical verdict of the input `In` carrying the reply `R`: exactly what a
/// [`Handle`](super::Handle) body returns.
///
/// `R` is the whole `Ok` side - `()` for a body with no reply, the reply type one-by-one, and
/// `Vec<Reply>` on a batch - and the `Err` side follows the input family: one
/// [`HandlerOutcome`], or a batch-length `Vec` of them.
pub type Verdict<In, R> = Result<R, <<<In as Input>::Axis as Axis>::Family as VerdictFamily>::Bad>;
