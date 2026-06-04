//! Colorful, environment-driven console logging built on [`tracing-subscriber`].
//!
//! RustStream emits structured [`tracing`] events throughout dispatch, publishing and the service
//! lifecycle, but - like any well-behaved library - installs no subscriber on its own. That choice
//! belongs to the application. This module is the batteries-included answer: one call wires up a
//! human-friendly, colored formatter whose verbosity is read from the `RUST_LOG` environment
//! variable, so `RUST_LOG=debug cargo run -- run` just works.
//!
//! Output goes to **stderr** (keeping stdout clean for machine-readable command output such as
//! `asyncapi gen`), and ANSI colors are enabled automatically when stderr is a terminal.
//!
//! The generated [`#[ruststream::app]`](macro@crate::app) CLI calls [`init`] for you on the `run`
//! command when the `logging` feature is enabled; reach for [`Logging`] directly only when you want
//! to override the defaults.
//!
//! Available with the `logging` feature.
//!
//! # Examples
//!
//! ```no_run
//! ruststream::logging::init()?;
//! tracing::info!("service starting");
//! # Ok::<(), ruststream::logging::LoggingInitError>(())
//! ```
//!
//! Tuning the defaults through the builder:
//!
//! ```no_run
//! use ruststream::logging::Logging;
//!
//! Logging::new()
//!     .with_default_filter("ruststream=debug,info")
//!     .with_target(false)
//!     .try_init()?;
//! # Ok::<(), ruststream::logging::LoggingInitError>(())
//! ```

use std::fmt;
use std::io::IsTerminal as _;

use thiserror::Error;
use tracing::field::{Field, Visit};
use tracing::{Event, Level};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::{Directive, ParseError};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::{FormatTime as _, SystemTime};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt as _;

/// Errors returned while installing the logger.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LoggingInitError {
    /// The default filter directive could not be parsed.
    #[error("invalid log filter directive: {0}")]
    Filter(#[from] ParseError),
    /// A global [`tracing`] subscriber was already installed by this or another crate.
    #[error("a tracing subscriber is already installed")]
    AlreadyInitialized(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Builder for RustStream's colored console logger.
///
/// The defaults match [`init`]: filter `info` (overridable per-target via `RUST_LOG`), colors auto-
/// detected from the terminal, and event targets shown. Every setter returns `self`, so calls
/// chain into a final [`try_init`](Self::try_init).
///
/// # Examples
///
/// ```no_run
/// use ruststream::logging::Logging;
///
/// Logging::new().with_ansi(true).try_init()?;
/// # Ok::<(), ruststream::logging::LoggingInitError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Logging {
    default_filter: String,
    ansi: Option<bool>,
    target: bool,
}

impl Default for Logging {
    fn default() -> Self {
        Self {
            default_filter: "info".to_owned(),
            ansi: None,
            target: true,
        }
    }
}

impl Logging {
    /// Creates a logger configuration with the default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the fallback filter used when `RUST_LOG` is unset or empty.
    ///
    /// Accepts the [`tracing_subscriber` directive syntax](EnvFilter), e.g. `"info"`,
    /// `"warn,ruststream=debug"`. Parsing is deferred to [`try_init`](Self::try_init), so an invalid
    /// directive surfaces there as [`LoggingInitError::Filter`].
    #[must_use]
    pub fn with_default_filter(mut self, filter: impl Into<String>) -> Self {
        self.default_filter = filter.into();
        self
    }

    /// Forces ANSI colors on or off.
    ///
    /// By default colors are enabled only when stderr is a terminal; set this to override that
    /// detection (e.g. force colors when piping into a pager that understands them).
    #[must_use]
    pub const fn with_ansi(mut self, ansi: bool) -> Self {
        self.ansi = Some(ansi);
        self
    }

    /// Controls whether each event's target (its module path) is printed. Defaults to `true`.
    #[must_use]
    pub const fn with_target(mut self, target: bool) -> Self {
        self.target = target;
        self
    }

    /// Installs the configured logger as the global [`tracing`] subscriber.
    ///
    /// Reads the filter from `RUST_LOG`, falling back to the configured default filter. Logs are
    /// written to stderr with colored levels.
    ///
    /// # Errors
    ///
    /// Returns [`LoggingInitError::Filter`] if the default filter directive is invalid, or
    /// [`LoggingInitError::AlreadyInitialized`] if a global subscriber is already in place (this
    /// function never replaces an existing one).
    pub fn try_init(self) -> Result<(), LoggingInitError> {
        let directive: Directive = self.default_filter.parse()?;
        let filter = EnvFilter::builder()
            .with_default_directive(directive)
            .with_env_var("RUST_LOG")
            .from_env_lossy();
        let ansi = self.ansi.unwrap_or_else(|| std::io::stderr().is_terminal());

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .event_format(ColorFormatter {
                ansi,
                target: self.target,
            })
            .finish()
            .try_init()
            .map_err(|err| LoggingInitError::AlreadyInitialized(err.into()))
    }
}

/// Installs the default colored console logger.
///
/// Convenience for [`Logging::new().try_init()`](Logging::try_init): filter `info` (overridable via
/// `RUST_LOG`), terminal-detected colors, targets shown. Call this once, early in `main`.
///
/// # Errors
///
/// See [`Logging::try_init`].
///
/// # Examples
///
/// ```no_run
/// ruststream::logging::init()?;
/// tracing::warn!(retries = 3, "broker reconnecting");
/// # Ok::<(), ruststream::logging::LoggingInitError>(())
/// ```
pub fn init() -> Result<(), LoggingInitError> {
    Logging::new().try_init()
}

/// A compact, colorful event formatter: dim timestamp, a bold colored level badge, a dim cyan
/// target, and the message (tinted for warnings and errors so they stand out). Hand-rolled ANSI
/// keeps the dependency surface to `tracing-subscriber` alone.
#[derive(Debug, Clone, Copy)]
struct ColorFormatter {
    ansi: bool,
    target: bool,
}

impl ColorFormatter {
    fn paint(self, w: &mut Writer<'_>, codes: &str, text: &str) -> fmt::Result {
        if self.ansi {
            write!(w, "\x1b[{codes}m{text}\x1b[0m")
        } else {
            write!(w, "{text}")
        }
    }
}

impl<S, N> FormatEvent<S, N> for ColorFormatter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let level = *meta.level();

        if self.ansi {
            write!(writer, "\x1b[2m")?;
            SystemTime.format_time(&mut writer)?;
            write!(writer, "\x1b[0m ")?;
        } else {
            SystemTime.format_time(&mut writer)?;
            write!(writer, " ")?;
        }

        let color = level_color(level);
        self.paint(
            &mut writer,
            &format!("1;{color}"),
            &format!("{:>5}", meta.level()),
        )?;
        write!(writer, " ")?;

        if self.target {
            self.paint(&mut writer, "2;36", meta.target())?;
            write!(writer, " ")?;
        }

        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);

        if matches!(level, Level::WARN | Level::ERROR) {
            self.paint(&mut writer, color, visitor.message.trim_end())?;
        } else {
            write!(writer, "{}", visitor.message.trim_end())?;
        }
        if !visitor.fields.is_empty() {
            self.paint(&mut writer, "2", &visitor.fields)?;
        }
        writeln!(writer)
    }
}

const fn level_color(level: Level) -> &'static str {
    match level {
        Level::ERROR => "31",
        Level::WARN => "33",
        Level::INFO => "32",
        Level::DEBUG => "34",
        Level::TRACE => "35",
    }
}

/// Splits an event's fields into the `message` (rendered first) and the remaining `key=value`
/// pairs (rendered dimmed after it).
#[derive(Debug, Default)]
struct EventVisitor {
    message: String,
    fields: String,
}

impl Visit for EventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        use std::fmt::Write as _;
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Logging;

    #[test]
    fn defaults_are_info_auto_color_with_targets() {
        let cfg = Logging::new();
        assert_eq!(cfg.default_filter, "info");
        assert!(cfg.ansi.is_none());
        assert!(cfg.target);
    }

    #[test]
    fn setters_override_defaults() {
        let cfg = Logging::new()
            .with_default_filter("ruststream=debug,warn")
            .with_ansi(false)
            .with_target(false);
        assert_eq!(cfg.default_filter, "ruststream=debug,warn");
        assert_eq!(cfg.ansi, Some(false));
        assert!(!cfg.target);
    }

    #[test]
    fn invalid_default_filter_is_rejected_at_init() {
        let err = Logging::new()
            .with_default_filter("=:not a directive:=")
            .try_init();
        assert!(err.is_err());
    }
}
