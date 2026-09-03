//! The computed verdict of a [`Handle`](super::Handle) body: one canonical return shape per
//! input family, fixed by the trait itself.
//!
//! The shape is computed, not chosen: the input axis picks the family
//! ([`OneByOne`] / [`Paged`]) and the reply parameter `R` is the `Ok` side, so an illegal
//! combination (a page body with a single-message verdict, a reply from a body that declared
//! none) has no type to be written in, and `async fn handle` bodies match the trait signature
//! exactly. The canonical spellings:
//!
//! - one-by-one: `Result<(), HandlerOutcome>`, and `Result<Reply, HandlerOutcome>` for a reply
//!   body;
//! - paged: `Result<(), Vec<HandlerOutcome>>` (the `Err` vector settles element-wise and is
//!   page-length by contract), and `Result<Vec<Reply>, Vec<HandlerOutcome>>` for a page reply
//!   body (one reply per element).
//!
//! An [`and_after`](crate::runtime::HandlerOutcome::and_after) continuation rides the outcome
//! itself, so it stays a return capability on every shape. Per-element vectors are page-length
//! by contract; the adapters check once per page and panic on a mismatch (see the module docs
//! of [`handle`](super)).

use crate::runtime::handler::HandlerOutcome;

use super::axis::{Axis, Input};

/// The single-delivery verdict family: one input, one settlement (and at most one reply).
#[derive(Debug, Clone, Copy)]
pub struct OneByOne;

/// The page verdict family: one input slice, per-element settlement.
#[derive(Debug, Clone, Copy)]
pub struct Paged;

/// The error side of one family's verdict. Machinery behind [`Handle`](super::Handle); never
/// named in user code.
#[doc(hidden)]
pub trait VerdictFamily {
    /// One [`HandlerOutcome`] one-by-one; a page-length vector of them per page.
    type Bad;
}

impl VerdictFamily for OneByOne {
    type Bad = HandlerOutcome;
}

impl VerdictFamily for Paged {
    type Bad = Vec<HandlerOutcome>;
}

/// The canonical verdict of the input `In` carrying the reply `R`: exactly what a
/// [`Handle`](super::Handle) body returns.
///
/// `R` is the whole `Ok` side - `()` for a body with no reply, the reply type one-by-one, and
/// `Vec<Reply>` on a page - and the `Err` side follows the input family: one
/// [`HandlerOutcome`], or a page-length `Vec` of them.
pub type Verdict<In, R> = Result<R, <<<In as Input>::Axis as Axis>::Family as VerdictFamily>::Bad>;
