//! Router, middleware and lifecycle.

mod handler;
mod metadata;
mod middleware;
mod router;
mod typed;

pub use handler::{Handler, HandlerResult};
pub use metadata::HandlerMetadata;
pub use middleware::{HandlerExt, Layer, layers};
pub use router::{Router, RouterError};
pub use typed::{DecodeFailure, Typed, typed};
