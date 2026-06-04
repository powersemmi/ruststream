//! Dynamic middleware container.
//!
//! [`DynStack`] is a single static [`Layer`] holding a runtime-built, frozen list of object-safe
//! middleware. Use it when the middleware set is decided at runtime (from config, a loop, feature
//! flags) and so cannot be a compile-time [`with`](super::HandlerExt::with) chain. Everything else
//! stays a zero-allocation static chain; only middleware placed inside a `DynStack` pay one boxed
//! future per layer per message.
//!
//! It is generic over the handler input `I`, so the same container works at either pipeline level:
//! raw deliveries (`DynStack<M>`, middleware bound `M: IncomingMessage`) or decoded values
//! (`DynStack<T>`).

use std::{future::Future, pin::Pin, sync::Arc};

use super::context::Context;
use super::handler::{Handler, HandlerResult};
use super::middleware::Layer;

type BoxFut<'a> = Pin<Box<dyn Future<Output = HandlerResult> + Send + 'a>>;

/// The wrapped handler at the end of the chain, erased so [`Next`] need not carry its type.
trait ErasedHandler<I>: Send + Sync {
    fn handle_boxed<'a>(&'a self, input: &'a I, ctx: &'a mut Context<'_>) -> BoxFut<'a>;
}

impl<I, H> ErasedHandler<I> for H
where
    I: Sync,
    H: Handler<I>,
{
    fn handle_boxed<'a>(&'a self, input: &'a I, ctx: &'a mut Context<'_>) -> BoxFut<'a> {
        Box::pin(self.handle(input, ctx))
    }
}

/// A middleware in the around / next style, operating on a borrowed input `I` and its [`Context`].
///
/// Each middleware inspects `input` / `ctx` (and may modify the context), optionally
/// short-circuits, and otherwise calls [`Next::run`] to continue the chain. Object-safe, so a
/// heterogeneous list can be stored in a [`DynStack`].
pub trait DynMiddleware<I>: Send + Sync {
    /// Handle `input`, calling `next` to continue to the rest of the chain.
    fn handle<'a>(
        &'a self,
        input: &'a I,
        ctx: &'a mut Context<'_>,
        next: Next<'a, I>,
    ) -> BoxFut<'a>;
}

/// A cursor over the remaining middleware in a [`DynStack`], ending in the wrapped handler.
pub struct Next<'a, I> {
    rest: &'a [Arc<dyn DynMiddleware<I>>],
    tail: &'a dyn ErasedHandler<I>,
}

impl<'a, I> Next<'a, I> {
    /// Runs the next middleware in the chain, or the wrapped handler if the chain is exhausted.
    #[must_use]
    pub fn run(self, input: &'a I, ctx: &'a mut Context<'_>) -> BoxFut<'a> {
        match self.rest.split_first() {
            Some((middleware, rest)) => middleware.handle(
                input,
                ctx,
                Next {
                    rest,
                    tail: self.tail,
                },
            ),
            None => self.tail.handle_boxed(input, ctx),
        }
    }
}

impl<I> std::fmt::Debug for Next<'_, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Next")
            .field("remaining", &self.rest.len())
            .finish_non_exhaustive()
    }
}

/// A [`Layer`] that runs a frozen list of [`DynMiddleware`] before the handler it wraps.
#[derive(Clone)]
pub struct DynStack<I>(Arc<[Arc<dyn DynMiddleware<I>>]>);

impl<I> DynStack<I> {
    /// Builds a stack from a list of middleware, applied in iteration order (first runs outermost).
    #[must_use]
    pub fn new(middleware: impl IntoIterator<Item = Arc<dyn DynMiddleware<I>>>) -> Self {
        Self(middleware.into_iter().collect())
    }
}

impl<I> std::fmt::Debug for DynStack<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynStack")
            .field("middleware", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl<I, H> Layer<H> for DynStack<I>
where
    I: Sync,
    H: Handler<I>,
{
    type Handler = DynStackHandler<I, H>;

    fn layer(&self, inner: H) -> Self::Handler {
        DynStackHandler {
            chain: self.0.clone(),
            inner: Arc::new(inner),
        }
    }
}

/// Handler produced by [`DynStack::layer`]. Runs the frozen middleware chain, then the wrapped
/// handler.
pub struct DynStackHandler<I, H> {
    chain: Arc<[Arc<dyn DynMiddleware<I>>]>,
    inner: Arc<H>,
}

impl<I, H> std::fmt::Debug for DynStackHandler<I, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynStackHandler")
            .field("middleware", &self.chain.len())
            .finish_non_exhaustive()
    }
}

impl<I, H> Handler<I> for DynStackHandler<I, H>
where
    I: Sync,
    H: Handler<I>,
{
    fn handle(&self, input: &I, ctx: &mut Context) -> impl Future<Output = HandlerResult> + Send {
        let chain = self.chain.clone();
        let inner = self.inner.clone();
        async move {
            let tail: &dyn ErasedHandler<I> = &inner;
            Next { rest: &chain, tail }.run(input, ctx).await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::super::HandlerExt;
    use super::super::context::{Context, State};
    use super::super::handler::{Handler, HandlerResult};
    use super::{BoxFut, DynMiddleware, DynStack, Next};
    use crate::Headers;

    struct Input;

    struct Recorder(Arc<Mutex<Vec<&'static str>>>, &'static str);

    impl DynMiddleware<Input> for Recorder {
        fn handle<'a>(
            &'a self,
            input: &'a Input,
            ctx: &'a mut Context<'_>,
            next: Next<'a, Input>,
        ) -> BoxFut<'a> {
            Box::pin(async move {
                self.0.lock().expect("poisoned").push(self.1);
                next.run(input, ctx).await
            })
        }
    }

    #[tokio::test]
    async fn runs_middleware_in_order_then_inner() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let stack = DynStack::new([
            Arc::new(Recorder(Arc::clone(&log), "a")) as Arc<dyn DynMiddleware<Input>>,
            Arc::new(Recorder(Arc::clone(&log), "b")) as Arc<dyn DynMiddleware<Input>>,
        ]);
        let inner_log = Arc::clone(&log);
        let inner = move |_: &Input, _ctx: &mut Context| {
            let inner_log = Arc::clone(&inner_log);
            async move {
                inner_log.lock().expect("poisoned").push("inner");
                HandlerResult::Ack
            }
        };
        let handler = inner.with(stack);
        let state = State::default();
        let mut ctx = Context::new("test", Headers::new(), &state);
        assert_eq!(handler.handle(&Input, &mut ctx).await, HandlerResult::Ack);
        assert_eq!(*log.lock().expect("poisoned"), vec!["a", "b", "inner"]);
    }
}
