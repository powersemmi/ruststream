//! The framework's diagnostics: one set of macros the crates of the family report through, and the
//! logger an app selects to receive them.
//!
//! Nothing is emitted until the app selects a logger: `RustStream::new(info).logger(TracingLogger)`
//! on the tokio runner, the same step on `EmbeddedApp`. The features `tracing` (which needs
//! `alloc`), `log` and `defmt` only compile a logger in. The selection is process-wide, like the
//! `log` crate's: every diagnostic of the family reads it, so two apps in one process share one
//! logger, and the last app that started decides.
//!
//! * [`TracingLogger`] emits a `tracing` event at the place that reports, so the target, the
//!   structured fields and the subscriber's per-thread dispatch are `tracing`'s own.
//! * [`LogLogger`] writes a `log` record and [`DefmtLogger`] a defmt line. Both carry the fields
//!   after the message as `name=value`, so a warning keeps its context (the subscription and the
//!   message type).
//!
//! Each logger hands the event to its backend, whose filter decides which levels show.
//!
//! The macros take the syntax of `tracing`'s events: an optional `target:`, fields written
//! `name = %value` (shown with `Display`), `name = ?value` (shown with `Debug`), `name = value` or
//! just `name`, and a message with its format arguments last. Machinery for the crates of the
//! family; the loggers and [`Logger`] are the public part.

use core::fmt::{self, Debug, Display, Formatter};
#[cfg(any(feature = "log", feature = "defmt"))]
use core::cell::Cell;
#[cfg(any(diag_tracing, feature = "log", feature = "defmt"))]
use core::sync::atomic::{AtomicU8, Ordering};

#[cfg(any(feature = "log", feature = "defmt"))]
use critical_section::Mutex;

pub use crate::__diag_warn as warn;
pub use crate::{__diag_debug as debug, __diag_error as error, __diag_info as info};

/// The backends the macros name by path, so a crate that reports needs no dependency of its own.
#[doc(hidden)]
pub mod private {
    #[cfg(feature = "log")]
    pub use log;
    #[cfg(diag_tracing)]
    pub use tracing;
}

mod sealed {
    /// Keeps the set of loggers closed.
    pub trait Sealed {}
}

/// A place the framework's diagnostics go, which an app selects with `.logger(..)`.
///
/// The loggers are [`TracingLogger`], [`LogLogger`] and [`DefmtLogger`], each present when its
/// feature is on. The trait is sealed: another backend is a `tracing` subscriber or a `log`
/// logger, which these forward to.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "json", feature = "tokio"))]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::diagnostics::TracingLogger;
///
/// /// What `#[ruststream::app]` runs: the framework reports through `tracing`.
/// pub fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .logger(TracingLogger)
///         .with_broker(MemoryBroker::new(), |b| b)
/// }
/// # }
/// # fn main() {}
/// ```
pub trait Logger: sealed::Sealed + 'static {
    /// Makes this logger the process's: the app calls it when it starts.
    #[doc(hidden)]
    fn select();
}

/// Reports through `tracing` events, filtered by the subscriber the service installed.
///
/// An event is emitted at the place that reports, so its target, level and fields are what
/// `tracing` makes of them.
#[cfg(diag_tracing)]
#[derive(Debug, Clone, Copy, Default)]
pub struct TracingLogger;

#[cfg(diag_tracing)]
impl sealed::Sealed for TracingLogger {}

#[cfg(diag_tracing)]
impl Logger for TracingLogger {
    fn select() {
        SELECTED.store(TRACING, Ordering::Release);
    }
}

/// Reports through the `log` crate, filtered by `log::max_level()` and the installed logger.
#[cfg(feature = "log")]
#[derive(Debug, Clone, Copy, Default)]
pub struct LogLogger;

#[cfg(feature = "log")]
impl sealed::Sealed for LogLogger {}

#[cfg(feature = "log")]
impl Logger for LogLogger {
    fn select() {
        select_sink(&LogSink);
    }
}

/// Reports through defmt, filtered by defmt's level filter.
///
/// A build links defmt's symbols only when the app selects this logger.
#[cfg(feature = "defmt")]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefmtLogger;

#[cfg(feature = "defmt")]
impl sealed::Sealed for DefmtLogger {}

#[cfg(feature = "defmt")]
impl Logger for DefmtLogger {
    fn select() {
        select_sink(&DefmtSink);
    }
}

/// The level of an event.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// `error`.
    Error,
    /// `warn`.
    Warn,
    /// `info`.
    Info,
    /// `debug`.
    Debug,
}

/// An event handed to a sink: its level, target, message and the fields after it.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct Event<'event> {
    /// The level.
    pub level: Level,
    /// The target.
    pub target: &'event str,
    /// The message.
    pub message: fmt::Arguments<'event>,
    /// The fields, shown as ` name=value` pairs.
    pub fields: &'event dyn Display,
}

impl Debug for Event<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("level", &self.level)
            .field("target", &self.target)
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

/// A backend that receives events through a call: the `log` and defmt loggers.
#[doc(hidden)]
pub trait Sink: Sync {
    /// Writes the event.
    fn event(&self, event: &Event<'_>);
}

#[cfg(feature = "log")]
struct LogSink;

#[cfg(feature = "log")]
impl Sink for LogSink {
    fn event(&self, event: &Event<'_>) {
        let level = match event.level {
            Level::Error => log::Level::Error,
            Level::Warn => log::Level::Warn,
            Level::Info => log::Level::Info,
            Level::Debug => log::Level::Debug,
        };
        log::log!(target: event.target, level, "{}{}", event.message, event.fields);
    }
}

#[cfg(feature = "defmt")]
struct DefmtSink;

#[cfg(feature = "defmt")]
impl Sink for DefmtSink {
    fn event(&self, event: &Event<'_>) {
        use defmt::Display2Format;

        macro_rules! line {
            ($level:ident) => {
                defmt::$level!(
                    "{=str}: {}{}",
                    event.target,
                    Display2Format(&event.message),
                    Display2Format(&event.fields),
                )
            };
        }
        match event.level {
            Level::Error => line!(error),
            Level::Warn => line!(warn),
            Level::Info => line!(info),
            Level::Debug => line!(debug),
        }
    }
}

#[cfg(any(diag_tracing, feature = "log", feature = "defmt"))]
const NONE: u8 = 0;
#[cfg(diag_tracing)]
const TRACING: u8 = 1;
#[cfg(any(feature = "log", feature = "defmt"))]
const SINK: u8 = 2;

/// Which logger the app selected: a plain load and store, so a target with no compare-and-swap
/// has it, and the last selection wins.
#[cfg(any(diag_tracing, feature = "log", feature = "defmt"))]
static SELECTED: AtomicU8 = AtomicU8::new(NONE);

/// The selected sink; read only when [`SELECTED`] says a sink is. Held in a critical section so
/// that no `unsafe` is needed on a target without `std`.
#[cfg(any(feature = "log", feature = "defmt"))]
static SINK_CELL: Mutex<Cell<Option<&'static dyn Sink>>> = Mutex::new(Cell::new(None));

#[cfg(any(feature = "log", feature = "defmt"))]
fn select_sink(sink: &'static dyn Sink) {
    critical_section::with(|section| SINK_CELL.borrow(section).set(Some(sink)));
    SELECTED.store(SINK, Ordering::Release);
}

/// The logger the app selected.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum Selected {
    /// None: events are dropped.
    None,
    /// `tracing`: the event is expanded at the call site.
    #[cfg(diag_tracing)]
    Tracing,
    /// A sink the event is handed to.
    #[cfg(any(feature = "log", feature = "defmt"))]
    Sink(&'static dyn Sink),
}

impl Debug for Selected {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            #[cfg(diag_tracing)]
            Self::Tracing => f.write_str("Tracing"),
            #[cfg(any(feature = "log", feature = "defmt"))]
            Self::Sink(_) => f.write_str("Sink"),
        }
    }
}

/// The selected logger: one load per event.
#[doc(hidden)]
#[inline]
#[must_use]
pub fn selected() -> Selected {
    #[cfg(any(diag_tracing, feature = "log", feature = "defmt"))]
    {
        match SELECTED.load(Ordering::Acquire) {
            #[cfg(diag_tracing)]
            TRACING => Selected::Tracing,
            #[cfg(any(feature = "log", feature = "defmt"))]
            SINK => critical_section::with(|section| SINK_CELL.borrow(section).get())
                .map_or(Selected::None, Selected::Sink),
            _ => Selected::None,
        }
    }
    #[cfg(not(any(diag_tracing, feature = "log", feature = "defmt")))]
    {
        Selected::None
    }
}

/// An event's field.
#[doc(hidden)]
pub trait Entry {
    /// Writes the field as ` name=value`.
    fn write(&self, f: &mut Formatter<'_>) -> fmt::Result;
}

/// A field shown with `Display`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Shown<'value, Value: ?Sized>(&'static str, &'value Value);

/// A field shown with `Debug`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Debugged<'value, Value: ?Sized>(&'static str, &'value Value);

/// A field named `name` that shows `value` with `Display`.
#[doc(hidden)]
#[must_use]
pub const fn shown<'value, Value: Display + ?Sized>(
    name: &'static str,
    value: &'value Value,
) -> Shown<'value, Value> {
    Shown(name, value)
}

/// A field named `name` that shows `value` with `Debug`.
#[doc(hidden)]
#[must_use]
pub const fn debugged<'value, Value: Debug + ?Sized>(
    name: &'static str,
    value: &'value Value,
) -> Debugged<'value, Value> {
    Debugged(name, value)
}

impl<Value: Display + ?Sized> Entry for Shown<'_, Value> {
    fn write(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, " {}={}", self.0, self.1)
    }
}

impl<Value: Debug + ?Sized> Entry for Debugged<'_, Value> {
    fn write(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, " {}={:?}", self.0, self.1)
    }
}

/// The fields of an event as the text after its message. `Fields` is a tuple of [`Entry`]s.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Show<'fields, Fields>(pub &'fields Fields);

macro_rules! show_tuple {
    ($($entry:ident),*) => {
        impl<$($entry: Entry),*> Display for Show<'_, ($($entry,)*)> {
            #[allow(non_snake_case, unused_variables)]
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                let ($($entry,)*) = self.0;
                $($entry.write(f)?;)*
                Ok(())
            }
        }
    };
}

show_tuple!();
show_tuple!(A);
show_tuple!(A, B);
show_tuple!(A, B, C);
show_tuple!(A, B, C, D);
show_tuple!(A, B, C, D, E);
show_tuple!(A, B, C, D, E, F);
show_tuple!(A, B, C, D, E, F, G);
show_tuple!(A, B, C, D, E, F, G, H);

/// An event at `error` level, in the syntax of `tracing`'s.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_error {
    ($($event:tt)*) => { $crate::__diag!(error, $($event)*) };
}

/// An event at `warn` level, in the syntax of `tracing`'s.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_warn {
    ($($event:tt)*) => { $crate::__diag!(warn, $($event)*) };
}

/// An event at `info` level, in the syntax of `tracing`'s.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_info {
    ($($event:tt)*) => { $crate::__diag!(info, $($event)*) };
}

/// An event at `debug` level, in the syntax of `tracing`'s.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_debug {
    ($($event:tt)*) => { $crate::__diag!(debug, $($event)*) };
}

/// The event for the logger the app selected: a `tracing` event where `TracingLogger` is, a call
/// to the sink where `LogLogger` or `DefmtLogger` is, nothing otherwise. The arguments are
/// type-checked in every build and evaluated only for the logger that shows them.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag {
    ($level:ident, $($event:tt)*) => {{
        #[allow(unused_variables)]
        let selected = $crate::diag::selected();
        $crate::__diag_trace!(selected, $level, $($event)*);
        $crate::__diag_parse!(@ selected, $level; []; []; $($event)*);
    }};
}

/// The event as a `tracing` event, when the selected logger is `TracingLogger`.
#[cfg(diag_tracing)]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_trace {
    ($selected:ident, $level:ident, $($event:tt)*) => {
        if let $crate::diag::Selected::Tracing = $selected {
            $crate::diag::private::tracing::$level!($($event)*)
        }
    };
}

/// No `tracing` logger in this build.
#[cfg(not(diag_tracing))]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_trace {
    ($selected:ident, $level:ident, $($event:tt)*) => {};
}

/// Reads the `target:`, the fields and the message of an event, one at a time.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_parse {
    (@ $selected:ident, $level:ident; [$($target:tt)*]; [$($fields:tt)*]; target: $new:expr, $($rest:tt)*) => {
        $crate::__diag_parse!(@ $selected, $level; [$new]; [$($fields)*]; $($rest)*)
    };
    (@ $selected:ident, $level:ident; [$($target:tt)*]; [$($fields:tt)*]; $name:ident = % $value:expr, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $selected, $level; [$($target)*];
            [$($fields)* $crate::diag::shown(stringify!($name), &$value),];
            $($rest)*
        )
    };
    (@ $selected:ident, $level:ident; [$($target:tt)*]; [$($fields:tt)*]; $name:ident = ? $value:expr, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $selected, $level; [$($target)*];
            [$($fields)* $crate::diag::debugged(stringify!($name), &$value),];
            $($rest)*
        )
    };
    (@ $selected:ident, $level:ident; [$($target:tt)*]; [$($fields:tt)*]; $name:ident = $value:expr, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $selected, $level; [$($target)*];
            [$($fields)* $crate::diag::shown(stringify!($name), &$value),];
            $($rest)*
        )
    };
    (@ $selected:ident, $level:ident; [$($target:tt)*]; [$($fields:tt)*]; ? $name:ident, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $selected, $level; [$($target)*];
            [$($fields)* $crate::diag::debugged(stringify!($name), &$name),];
            $($rest)*
        )
    };
    (@ $selected:ident, $level:ident; [$($target:tt)*]; [$($fields:tt)*]; % $name:ident, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $selected, $level; [$($target)*];
            [$($fields)* $crate::diag::shown(stringify!($name), &$name),];
            $($rest)*
        )
    };
    (@ $selected:ident, $level:ident; [$($target:tt)*]; [$($fields:tt)*]; $name:ident, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $selected, $level; [$($target)*];
            [$($fields)* $crate::diag::shown(stringify!($name), &$name),];
            $($rest)*
        )
    };
    (
        @ $selected:ident, $level:ident; [$($target:tt)*]; [$($fields:tt)*];
        $format:literal $(, $argument:expr)* $(,)?
    ) => {
        $crate::__diag_sink!(
            $selected,
            $level,
            $crate::__diag_target!($($target)*),
            ::core::format_args!($format $(, $argument)*),
            ($($fields)*)
        )
    };
}

/// The event's target, or the module that reports it.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_target {
    () => {
        ::core::module_path!()
    };
    ($target:expr) => {
        $target
    };
}

/// The event's level.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_level {
    (error) => {
        $crate::diag::Level::Error
    };
    (warn) => {
        $crate::diag::Level::Warn
    };
    (info) => {
        $crate::diag::Level::Info
    };
    (debug) => {
        $crate::diag::Level::Debug
    };
}

/// The event handed to the sink, when the selected logger is one.
#[cfg(any(feature = "log", feature = "defmt"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_sink {
    ($selected:ident, $level:ident, $target:expr, $message:expr, $fields:expr) => {
        if let $crate::diag::Selected::Sink(sink) = $selected {
            // One `match`, so the temporaries the fields and the message borrow live through the
            // call.
            match ($target, $message, $fields) {
                (target, message, fields) => sink.event(&$crate::diag::Event {
                    level: $crate::__diag_level!($level),
                    target,
                    message,
                    fields: &$crate::diag::Show(&fields),
                }),
            }
        }
    };
}

/// No sink in this build: the arguments are type-checked and dropped.
#[cfg(not(any(feature = "log", feature = "defmt")))]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_sink {
    ($selected:ident, $level:ident, $target:expr, $message:expr, $fields:expr) => {
        let _ = || match ($target, $message, $fields) {
            (target, message, fields) => {
                let _ = (target, &message, &$crate::diag::Show(&fields));
            }
        };
    };
}

#[cfg(test)]
mod tests {
    use std::format;

    use super::{Show, debugged, shown};

    #[test]
    fn an_event_without_fields_adds_nothing_to_its_message() {
        assert_eq!(format!("{}", Show(&())), "");
    }

    #[test]
    fn a_field_shown_with_display_follows_the_message_as_a_name_value_pair() {
        assert_eq!(
            format!("{}", Show(&(shown("subscription", "orders"),))),
            " subscription=orders"
        );
    }

    #[test]
    fn a_field_shown_with_debug_keeps_its_debug_form() {
        assert_eq!(
            format!("{}", Show(&(debugged("error", &Some("lost")),))),
            " error=Some(\"lost\")"
        );
    }

    #[test]
    fn fields_follow_each_other_in_the_order_they_were_written_up_to_eight() {
        let fields = (
            shown("a", &1),
            shown("b", &2),
            shown("c", &3),
            shown("d", &4),
            shown("e", &5),
            shown("f", &6),
            shown("g", &7),
            shown("h", &8),
        );
        assert_eq!(
            format!("{}", Show(&fields)),
            " a=1 b=2 c=3 d=4 e=5 f=6 g=7 h=8"
        );
        let two = (shown("a", &1), shown("b", &2));
        assert_eq!(format!("{}", Show(&two)), " a=1 b=2");
        let three = (shown("a", &1), shown("b", &2), shown("c", &3));
        assert_eq!(format!("{}", Show(&three)), " a=1 b=2 c=3");
        let four = (
            shown("a", &1),
            shown("b", &2),
            shown("c", &3),
            shown("d", &4),
        );
        assert_eq!(format!("{}", Show(&four)), " a=1 b=2 c=3 d=4");
        let five = (
            shown("a", &1),
            shown("b", &2),
            shown("c", &3),
            shown("d", &4),
            shown("e", &5),
        );
        assert_eq!(format!("{}", Show(&five)), " a=1 b=2 c=3 d=4 e=5");
        let six = (
            shown("a", &1),
            shown("b", &2),
            shown("c", &3),
            shown("d", &4),
            shown("e", &5),
            shown("f", &6),
        );
        assert_eq!(format!("{}", Show(&six)), " a=1 b=2 c=3 d=4 e=5 f=6");
        let seven = (
            shown("a", &1),
            shown("b", &2),
            shown("c", &3),
            shown("d", &4),
            shown("e", &5),
            shown("f", &6),
            shown("g", &7),
        );
        assert_eq!(format!("{}", Show(&seven)), " a=1 b=2 c=3 d=4 e=5 f=6 g=7");
    }
}
