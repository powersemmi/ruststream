//! What a mount keeps of a registration's description in the build without `alloc`.
//!
//! The `AsyncAPI` document needs the heap, so this build keeps no description of a handler. The
//! one fact it holds is the input type's name, which the retry path puts on its diagnostics.

use crate::input::InputKind;

/// The part of a registration's description that the build without `alloc` keeps.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Meta {
    /// Type name of the decoded input value, as captured at registration time.
    pub input_type: &'static str,
}

impl Meta {
    /// The description of a registration whose input is `Input`.
    pub(crate) fn of<Input: InputKind>() -> Self {
        Self {
            input_type: Input::input_label(),
        }
    }
}
