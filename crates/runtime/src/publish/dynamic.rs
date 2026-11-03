//! The opt-in dynamic publish middleware: a list decided at runtime, which boxes one future per layer
//! and so exists where there is a heap.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::fmt;

use ruststream_core::{IfSend, MaybeSend, MaybeSendSync};

use super::{Outgoing, PublishFut, PublishLayer, PublishNext, PublishPipeline};
use crate::Publisher;
use crate::lifecycle::BoxError;

/// An object-safe publish middleware, for a [`PublishDynStack`].
///
/// The dynamic counterpart of [`PublishLayer`]: it cannot name the rest of the pipeline as a
/// type parameter (that is what keeps it object-safe and lets a heterogeneous, runtime-built list
/// live in one [`PublishDynStack`]), so it continues through the type-erased [`PublishDynNext`]
/// instead. Use it only when the middleware set is decided at runtime; otherwise a static
/// [`PublishLayer`] is zero-cost.
pub trait PublishDynLayer: MaybeSendSync {
    /// Handle the outgoing message, calling `next` to continue.
    fn on_publish<'a>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        next: PublishDynNext<'a>,
    ) -> PublishFut<'a>;
}

type PublishTail<'a> = IfSend<
    Box<dyn for<'o> FnOnce(&'a mut Outgoing<'o>) -> PublishFut<'a> + Send + 'a>,
    Box<dyn for<'o> FnOnce(&'a mut Outgoing<'o>) -> PublishFut<'a> + 'a>,
>;

/// A cursor over the rest of a [`PublishDynStack`], ending in the surrounding static pipeline.
///
/// Mirrors [`PublishNext`] for the dynamic list: [`run`](Self::run) advances to the next
/// [`PublishDynLayer`], or hands control back to the static chain once the list is exhausted.
pub struct PublishDynNext<'a> {
    rest: &'a [Arc<dyn PublishDynLayer>],
    // The surrounding static `PublishNext::run`, erased so this cursor need not carry its type. A
    // one-shot continuation: `run` is called exactly once per published message.
    tail: PublishTail<'a>,
}

impl<'a> PublishDynNext<'a> {
    /// Runs the next dynamic middleware, or the surrounding static pipeline if the list is done.
    pub fn run(self, out: &'a mut Outgoing<'_>) -> PublishFut<'a> {
        match self.rest.split_first() {
            Some((middleware, rest)) => middleware.on_publish(
                out,
                PublishDynNext {
                    rest,
                    tail: self.tail,
                },
            ),
            None => (self.tail)(out),
        }
    }
}

impl fmt::Debug for PublishDynNext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishDynNext")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// A single static [`PublishLayer`] wrapping a runtime-built, frozen list of
/// [`PublishDynLayer`].
///
/// The publish-side counterpart of the consume-side [`DynStack`](crate::DynStack): the opt-in
/// escape hatch for a middleware set decided at runtime (from config, a loop, feature flags) that
/// therefore cannot be a compile-time [`publish_layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.publish_layer) chain.
/// Add it like any other middleware; only the middleware inside it pay one boxed future per layer.
///
/// ```
/// # #[cfg(feature = "json")]
/// # {
/// use std::sync::Arc;
/// use ruststream::runtime::{PublishDynLayer, PublishDynStack};
///
/// fn stack(
///     middleware: Vec<Arc<dyn PublishDynLayer>>,
/// ) -> PublishDynStack {
///     PublishDynStack::new(middleware)
/// }
/// # }
/// ```
pub struct PublishDynStack(Arc<[Arc<dyn PublishDynLayer>]>);

// Manual `Clone`: the field is an `Arc`, so a clone is a refcount bump regardless of the contents.
impl Clone for PublishDynStack {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl PublishDynStack {
    /// Builds a stack from a list of middleware, applied in iteration order (first runs outermost).
    #[must_use]
    pub fn new(middleware: impl IntoIterator<Item = Arc<dyn PublishDynLayer>>) -> Self {
        Self(middleware.into_iter().collect())
    }
}

impl fmt::Debug for PublishDynStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishDynStack")
            .field("middleware", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl PublishLayer for PublishDynStack {
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), BoxError>> + MaybeSend + 'a {
        // Erase the static continuation into a one-shot closure so the object-safe walker can end
        // by handing control back to the surrounding static pipeline. Boxing starts here: only
        // the dynamic list pays it.
        PublishDynNext {
            rest: &self.0,
            tail: Box::new(move |out| -> PublishFut<'a> { Box::pin(next.run(out)) }),
        }
        .run(out)
    }
}
