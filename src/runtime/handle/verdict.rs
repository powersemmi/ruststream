//! Verdict normalization: what a [`Handle`](super::Handle) body may return, per input family.
//!
//! The return shape is computed, not chosen: the input axis picks the family
//! ([`OneByOne`] / [`Paged`]) and the reply parameter `R` fixes the `Ok` side, so an illegal
//! combination (a page body with a single-message verdict, a reply from a body that declared
//! none) has no type to be written in. The accepted spellings normalize into one internal shape
//! per family, which is what the dispatch adapters consume:
//!
//! - one-by-one: `Result<R, HandlerResult>` (the canonical form), and for `R = ()` also
//!   `Result<(), Settle>`, a bare [`Settle`] or [`HandlerResult`] - so
//!   [`and_after`](HandlerResult::and_after) continuations stay a return capability;
//! - paged: `Result<Vec<R>, Vec<HandlerResult>>` (canonical; the `Err` vector settles
//!   element-wise), `Result<Vec<R>, HandlerResult>` (one outcome for the page), and for
//!   `R = ()` the `Ok(())` spellings of both plus `Result<(), Vec<Settle>>`.
//!
//! Per-element vectors are page-length by contract; the adapters check once per page and panic
//! on a mismatch (see the module docs of [`handle`](super)).

use crate::runtime::batch::BatchResult;
use crate::runtime::handler::{HandlerResult, Settle};

use super::axis::{Axis, Input};

/// The single-delivery verdict family: one input, one settlement (and at most one reply).
#[derive(Debug, Clone, Copy)]
pub struct OneByOne;

/// The page verdict family: one input slice, per-element (or uniform) settlement.
#[derive(Debug, Clone, Copy)]
pub struct Paged;

/// The normalized verdict shape of one family. Machinery behind [`IntoVerdict`]; never named in
/// user code.
#[doc(hidden)]
pub trait VerdictFamily {
    /// The normalized verdict carrying a reply of type `R`.
    type Norm<R>;
}

impl VerdictFamily for OneByOne {
    /// `Ok(reply)` publishes and acks; `Err(settle)` settles without a reply.
    type Norm<R> = Result<R, Settle>;
}

impl VerdictFamily for Paged {
    /// `Ok(replies)` publishes element-wise and acks the page; `Err(result)` settles it.
    type Norm<R> = Result<Vec<R>, BatchResult>;
}

/// A value a [`Handle`](super::Handle) body may return for the input `In` and the reply `R`.
///
/// The legal set is closed per input family (see the module docs); the bound appears on the
/// body's returned future, so a body returning anything else fails to compile at its own
/// definition.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a verdict a handler over `{In}` can return",
    note = "a single-message body returns `Result<Reply, HandlerResult>` (with `Reply = ()` when \
            it declares no reply; `Result<(), Settle>` keeps an `and_after` continuation), and a \
            page body returns `Result<Vec<Reply>, Vec<HandlerResult>>` or \
            `Result<(), Vec<HandlerResult>>`, with a uniform `Result<_, HandlerResult>` accepted \
            for the whole page"
)]
pub trait IntoVerdict<In: ?Sized + Input, R>: Send {
    /// Normalizes the returned value into the family's internal shape.
    #[doc(hidden)]
    fn into_verdict(self) -> <<In::Axis as Axis>::Family as VerdictFamily>::Norm<R>;
}

impl<In, R, V> IntoVerdict<In, R> for V
where
    In: ?Sized + Input,
    V: VerdictFor<<In::Axis as Axis>::Family, R> + Send,
{
    fn into_verdict(self) -> <<In::Axis as Axis>::Family as VerdictFamily>::Norm<R> {
        self.normalize()
    }
}

/// One accepted return spelling of one family. Machinery behind [`IntoVerdict`]; the family
/// token keeps the one-by-one and the paged sets disjoint.
#[doc(hidden)]
pub trait VerdictFor<F: VerdictFamily, R> {
    fn normalize(self) -> F::Norm<R>;
}

// --------------------------------------------------------------------------------- one-by-one

impl<R> VerdictFor<OneByOne, R> for Result<R, HandlerResult> {
    fn normalize(self) -> Result<R, Settle> {
        self.map_err(Settle::from)
    }
}

impl VerdictFor<OneByOne, ()> for Result<(), Settle> {
    fn normalize(self) -> Result<(), Settle> {
        self
    }
}

impl VerdictFor<OneByOne, ()> for Settle {
    fn normalize(self) -> Result<(), Settle> {
        Err(self)
    }
}

impl VerdictFor<OneByOne, ()> for HandlerResult {
    fn normalize(self) -> Result<(), Settle> {
        Err(self.into())
    }
}

// -------------------------------------------------------------------------------------- paged

impl<R> VerdictFor<Paged, R> for Result<Vec<R>, Vec<HandlerResult>> {
    fn normalize(self) -> Result<Vec<R>, BatchResult> {
        self.map_err(|outcomes| {
            BatchResult::PerElement(outcomes.into_iter().map(Settle::from).collect())
        })
    }
}

impl<R> VerdictFor<Paged, R> for Result<Vec<R>, HandlerResult> {
    fn normalize(self) -> Result<Vec<R>, BatchResult> {
        self.map_err(BatchResult::Uniform)
    }
}

impl VerdictFor<Paged, ()> for Result<(), Vec<HandlerResult>> {
    fn normalize(self) -> Result<Vec<()>, BatchResult> {
        match self {
            Ok(()) => Ok(Vec::new()),
            Err(outcomes) => Err(BatchResult::PerElement(
                outcomes.into_iter().map(Settle::from).collect(),
            )),
        }
    }
}

impl VerdictFor<Paged, ()> for Result<(), Vec<Settle>> {
    fn normalize(self) -> Result<Vec<()>, BatchResult> {
        match self {
            Ok(()) => Ok(Vec::new()),
            Err(settles) => Err(BatchResult::PerElement(settles)),
        }
    }
}

impl VerdictFor<Paged, ()> for Result<(), HandlerResult> {
    fn normalize(self) -> Result<Vec<()>, BatchResult> {
        match self {
            Ok(()) => Ok(Vec::new()),
            Err(outcome) => Err(BatchResult::Uniform(outcome)),
        }
    }
}
