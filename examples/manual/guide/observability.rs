//! A middleware of the shop's own, the same with the macros and without them: a layer is a plain
//! type in both forms.

use std::time::Duration;

use ruststream::memory::prelude::*;
use ruststream::runtime::{BlanketLayer, Handler, Layer};
use tokio::time::Instant;

// --8<-- [start:layer]
/// Warns about every delivery whose handler ran longer than the threshold.
#[derive(Clone)]
pub(crate) struct SlowHandlers {
    threshold: Duration,
}

impl SlowHandlers {
    pub(crate) const fn over(threshold: Duration) -> Self {
        Self { threshold }
    }
}

/// The handler `SlowHandlers` makes of the one it wraps.
pub(crate) struct Timed<H> {
    inner: H,
    threshold: Duration,
}

impl<H> Layer<H> for SlowHandlers {
    type Handler = Timed<H>;

    fn layer(&self, inner: H) -> Timed<H> {
        Timed {
            inner,
            threshold: self.threshold,
        }
    }
}

impl<M, C, S, H> Handler<M, C, S> for Timed<H>
where
    M: Send + Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        let started = Instant::now();
        let outcome = self.inner.handle(msg, ctx).await;
        let took = started.elapsed();
        if took > self.threshold {
            tracing::warn!(subscription = ctx.name(), ?took, "slow handler");
        }
        outcome
    }
}

// The app wraps handlers whose types it hides, through this generic method.
impl BlanketLayer for SlowHandlers {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        self.layer(handler)
    }
}
// --8<-- [end:layer]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};
    use tokio::time;
    use tracing::subscriber;

    use super::SlowHandlers;
    use crate::service::test_broker;

    #[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
    struct Report {
        pages: u64,
    }

    impl OutgoingDestination for Report {
        type Form = FixedName;

        const DESTINATION: &'static str = "reports";
    }

    impl MessageHeaders for Report {
        type Contract = NoHeaders;
    }

    /// Takes a millisecond per page.
    struct Render;

    impl Handle<Report> for Render {
        fn handle(
            &self,
            report: &Report,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            let pages = report.pages;
            async move {
                time::sleep(Duration::from_millis(pages)).await;
                Ok(())
            }
        }
    }

    /// The formatted log lines, as an operator reads them.
    #[derive(Clone, Default)]
    struct Lines(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Lines {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Lines {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap_or_else(PoisonError::into_inner))
                .into_owned()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn only_the_slow_delivery_is_reported() -> Result<(), Box<dyn Error>> {
        let lines = Lines::default();
        let writer = lines.clone();
        let logger = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let _guard = subscriber::set_default(logger);

        let app = RustStream::new(AppInfo::new("reports", "0.1.0"))
            .layer(SlowHandlers::over(Duration::from_millis(250)))
            .with_broker(test_broker(), |b| {
                b.include(subscriber("reports", Render).build());
            });
        let tb = TestApp::start(app).await?;
        tb.message(&Report { pages: 100 }).publish().await?;
        assert!(!lines.text().contains("slow handler"), "{}", lines.text());

        tb.message(&Report { pages: 400 }).publish().await?;
        let text = lines.text();
        assert_eq!(text.matches("slow handler").count(), 1, "{text}");
        assert!(text.contains("subscription=\"reports\""), "{text}");
        Ok(())
    }
}
