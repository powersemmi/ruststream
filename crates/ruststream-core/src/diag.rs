//! The framework's diagnostics: one set of macros the crates of the family report through.
//!
//! The macros take the syntax of `tracing`'s events: an optional `target:`, fields written
//! `name = %value` (shown with `Display`), `name = ?value` (shown with `Debug`), `name = value` or
//! just `name`, and a message with its format arguments last.
//!
//! * With the contract's `tracing` feature (the heap variant) an event is a `tracing` event and
//!   no other sink receives it.
//! * Without it an event goes to every sink that is switched on: the `log` feature writes a
//!   record, the `defmt` feature a defmt line. Both carry the fields after the message as
//!   `name=value`, so a warning keeps its context (the subscription and the message type) in each.
//! * With none of them the arguments are type-checked and dropped: the macro costs nothing.
//!
//! Machinery for the crates of the family; not part of the public API.

use core::fmt::{self, Debug, Display, Formatter};

pub use crate::__diag_warn as warn;
pub use crate::{__diag_debug as debug, __diag_error as error, __diag_info as info};

/// The sinks the macros name by path, so a crate that reports needs no dependency of its own.
#[doc(hidden)]
pub mod private {
    #[cfg(feature = "log")]
    pub use log;
    #[cfg(feature = "tracing")]
    pub use tracing;
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

#[cfg(feature = "defmt")]
impl<Value: Display + ?Sized> defmt::Format for Shown<'_, Value> {
    fn format(&self, f: defmt::Formatter<'_>) {
        defmt::write!(f, " {=str}={}", self.0, defmt::Display2Format(self.1));
    }
}

impl<Value: Debug + ?Sized> Entry for Debugged<'_, Value> {
    fn write(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, " {}={:?}", self.0, self.1)
    }
}

#[cfg(feature = "defmt")]
impl<Value: Debug + ?Sized> defmt::Format for Debugged<'_, Value> {
    fn format(&self, f: defmt::Formatter<'_>) {
        defmt::write!(f, " {=str}={}", self.0, defmt::Debug2Format(self.1));
    }
}

/// The fields of an event as the text after its message, for a sink that formats with `core::fmt`
/// and for a defmt line. `Fields` is a tuple of [`Entry`]s.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Show<'fields, Fields>(pub &'fields Fields);

macro_rules! show_tuple {
    ($format:literal; $($entry:ident),*) => {
        impl<$($entry: Entry),*> Display for Show<'_, ($($entry,)*)> {
            #[allow(non_snake_case, unused_variables)]
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                let ($($entry,)*) = self.0;
                $($entry.write(f)?;)*
                Ok(())
            }
        }

        // A defmt line is written by one `write!`, which takes the formatter, so each arity has
        // its own format string.
        #[cfg(feature = "defmt")]
        impl<$($entry: defmt::Format),*> defmt::Format for Show<'_, ($($entry,)*)> {
            #[allow(non_snake_case, unused_variables)]
            fn format(&self, f: defmt::Formatter<'_>) {
                let ($($entry,)*) = self.0;
                defmt::write!(f, $format $(, $entry)*);
            }
        }
    };
}

show_tuple!("";);
show_tuple!("{}"; A);
show_tuple!("{}{}"; A, B);
show_tuple!("{}{}{}"; A, B, C);
show_tuple!("{}{}{}{}"; A, B, C, D);
show_tuple!("{}{}{}{}{}"; A, B, C, D, E);
show_tuple!("{}{}{}{}{}{}"; A, B, C, D, E, F);
show_tuple!("{}{}{}{}{}{}{}"; A, B, C, D, E, F, G);
show_tuple!("{}{}{}{}{}{}{}{}"; A, B, C, D, E, F, G, H);

/// The defmt sink: one function per level, so the line carries the message and the fields as
/// arguments and the crate that reports needs no `defmt` dependency of its own.
#[cfg(feature = "defmt")]
#[doc(hidden)]
pub mod defmt_sink {
    use core::fmt::Arguments;

    use defmt::Format;

    use super::Show;

    macro_rules! level {
        ($($name:ident),*) => {$(
            /// Writes the event at this level.
            pub fn $name<Fields>(target: &str, message: Arguments<'_>, fields: &Fields)
            where
                for<'fields> Show<'fields, Fields>: Format,
            {
                defmt::$name!(
                    "{=str}: {}{}",
                    target,
                    defmt::Display2Format(&message),
                    Show(fields),
                );
            }
        )*};
    }

    level!(error, warn, info, debug);
}

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

/// The event as a `tracing` event.
#[cfg(feature = "tracing")]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag {
    ($level:ident, $($event:tt)*) => {
        $crate::diag::private::tracing::$level!($($event)*)
    };
}

/// The event handed to the sinks that are on, or type-checked and dropped when none is.
#[cfg(not(feature = "tracing"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag {
    ($level:ident, $($event:tt)*) => {
        $crate::__diag_parse!(@ $level; []; []; $($event)*)
    };
}

/// Reads the `target:`, the fields and the message of an event, one at a time.
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_parse {
    (@ $level:ident; [$($target:tt)*]; [$($fields:tt)*]; target: $new:expr, $($rest:tt)*) => {
        $crate::__diag_parse!(@ $level; [$new]; [$($fields)*]; $($rest)*)
    };
    (@ $level:ident; [$($target:tt)*]; [$($fields:tt)*]; $name:ident = % $value:expr, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $level; [$($target)*];
            [$($fields)* $crate::diag::shown(stringify!($name), &$value),];
            $($rest)*
        )
    };
    (@ $level:ident; [$($target:tt)*]; [$($fields:tt)*]; $name:ident = ? $value:expr, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $level; [$($target)*];
            [$($fields)* $crate::diag::debugged(stringify!($name), &$value),];
            $($rest)*
        )
    };
    (@ $level:ident; [$($target:tt)*]; [$($fields:tt)*]; $name:ident = $value:expr, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $level; [$($target)*];
            [$($fields)* $crate::diag::shown(stringify!($name), &$value),];
            $($rest)*
        )
    };
    (@ $level:ident; [$($target:tt)*]; [$($fields:tt)*]; ? $name:ident, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $level; [$($target)*];
            [$($fields)* $crate::diag::debugged(stringify!($name), &$name),];
            $($rest)*
        )
    };
    (@ $level:ident; [$($target:tt)*]; [$($fields:tt)*]; % $name:ident, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $level; [$($target)*];
            [$($fields)* $crate::diag::shown(stringify!($name), &$name),];
            $($rest)*
        )
    };
    (@ $level:ident; [$($target:tt)*]; [$($fields:tt)*]; $name:ident, $($rest:tt)*) => {
        $crate::__diag_parse!(
            @ $level; [$($target)*];
            [$($fields)* $crate::diag::shown(stringify!($name), &$name),];
            $($rest)*
        )
    };
    (
        @ $level:ident; [$($target:tt)*]; [$($fields:tt)*];
        $format:literal $(, $argument:expr)* $(,)?
    ) => {
        // One `match`, so the temporaries the fields and the message borrow live through every
        // sink.
        match (
            $crate::__diag_target!($($target)*),
            ::core::format_args!($format $(, $argument)*),
            ($($fields)*),
        ) {
            (target, message, fields) => {
                let _ = (target, &message, &fields);
                $crate::__diag_to_log!($level, target, message, &fields);
                $crate::__diag_to_defmt!($level, target, message, &fields);
            }
        }
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

/// The event as a `log` record.
#[cfg(feature = "log")]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_to_log {
    ($level:ident, $target:expr, $message:expr, $fields:expr) => {
        $crate::diag::private::log::$level!(
            target: $target,
            "{}{}",
            $message,
            $crate::diag::Show($fields),
        )
    };
}

/// No `log` sink.
#[cfg(not(feature = "log"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_to_log {
    ($level:ident, $target:expr, $message:expr, $fields:expr) => {};
}

/// The event as a defmt line.
#[cfg(feature = "defmt")]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_to_defmt {
    ($level:ident, $target:expr, $message:expr, $fields:expr) => {
        $crate::diag::defmt_sink::$level($target, $message, $fields)
    };
}

/// No `defmt` sink.
#[cfg(not(feature = "defmt"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __diag_to_defmt {
    ($level:ident, $target:expr, $message:expr, $fields:expr) => {};
}
