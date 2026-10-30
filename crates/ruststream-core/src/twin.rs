//! The macro that writes a contract trait and its `Local*` twin from one list of items.
//!
//! Every contract trait comes in two flavors. The Send trait carries the `Send` and `Sync` bounds a
//! work-stealing runtime needs to move a broker, a delivery and their futures across threads. Its
//! `Local*` twin carries the same items without them, for brokers whose types hold `Rc` or
//! `RefCell`. A blanket impl makes every implementation of the Send trait an implementation of the
//! twin, forwarding each item statically.
//!
//! The two traits differ in a few tokens: a `+ Send` on a returned future, a supertrait, a bound
//! naming another contract trait. `trait-variant` cannot rewrite the last kind, so the core writes
//! the pair with a macro of its own. The item list is written once, and two markers say where the
//! flavors differ:
//!
//! - `@send { .. }`: tokens that appear in the Send trait only (`@send { + Send }`);
//! - `@twin { .. } { .. }`: the Send trait's tokens, then the twin's
//!   (`@twin { IncomingMessage } { LocalIncomingMessage }`).
//!
//! A marker may stand in an associated type's bounds and in a method's return type, the two
//! places the flavors differ. A method's `where` clause, its parameters and its default body are
//! the same in both traits.
//!
//! # Input
//!
//! ```text
//! __contract_twin! {
//!     send { /* attributes and header of the Send trait */ }
//!     local { /* attributes and header of the twin */ }
//!     blanket { /* the impl header that makes every Send implementation a twin */ }
//!     forward { /* the qualified path of the Send trait, `<T as Subscriber>` */ }
//!     docs { Subscriber }
//!     types { /* `type Name: [bounds];`, each with `#[cfg]` and docs */ }
//!     fns { /* `fn name(params) -> [ret] where [..] ;` or `{ default body }` */ }
//!     send_only { /* items of the Send trait alone, written as they are */ }
//! }
//! ```
//!
//! An item takes `#[cfg(..)]` attributes first, then `#[must_use]`, then its docs. The twin's
//! items point at the Send trait's item for their documentation, so a doc example runs once.

/// Writes a Send contract trait, its `Local*` twin and the blanket impl between them.
///
/// # Where `Send` comes from
///
/// A body written once over the twin is spawned on a multi-thread runtime by generic code that
/// knows only the Send trait. Its future is `Send` because the twin's method resolves, through
/// the blanket impl, to the Send trait's method, whose future carries `+ Send`:
///
/// ```
/// use core::future::Future;
///
/// ruststream_core::__contract_twin! {
///     send { pub trait Settle: Send }
///     local { pub trait LocalSettle }
///     blanket { impl<T: Settle> LocalSettle for T }
///     forward { <T as Settle> }
///     docs { Settle }
///     fns {
///         fn settle(self) -> [impl Future<Output = ()> @send { + Send }];
///     }
/// }
///
/// async fn body<M: LocalSettle>(delivery: M) {
///     delivery.settle().await;
/// }
///
/// fn spawn<M: Settle + 'static>(delivery: M) {
///     tokio::spawn(body(delivery));
/// }
/// ```
///
/// The same pair without the `+ Send` leaves the spawn without a proof, and it does not compile:
///
/// ```compile_fail,E0277
/// use core::future::Future;
///
/// ruststream_core::__contract_twin! {
///     send { pub trait Settle: Send }
///     local { pub trait LocalSettle }
///     blanket { impl<T: Settle> LocalSettle for T }
///     forward { <T as Settle> }
///     docs { Settle }
///     fns {
///         fn settle(self) -> [impl Future<Output = ()>];
///     }
/// }
///
/// async fn body<M: LocalSettle>(delivery: M) {
///     delivery.settle().await;
/// }
///
/// fn spawn<M: Settle + 'static>(delivery: M) {
///     tokio::spawn(body(delivery));
/// }
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! __contract_twin {
    (
        send { $($send_head:tt)* }
        local { $($local_head:tt)* }
        blanket { $($blanket:tt)* }
        forward $forward:tt
        docs { $send_name:ident }
        $(types {
            $(
                $(#[cfg $tcfg:tt])*
                $(#[doc = $tdoc:literal])*
                type $tname:ident $(: [$($tbound:tt)*])?;
            )*
        })?
        $(fns {
            $(
                $(#[cfg $fcfg:tt])*
                $(#[must_use $($fmu:tt)*])?
                $(#[doc = $fdoc:literal])*
                fn $fname:ident $fparams:tt $(-> [$($fret:tt)*])? $(where [$($fwhere:tt)*])?
                    $(;)? $({ $($fbody:tt)* })?
            )*
        })?
        $(send_only { $($send_only:tt)* })?
    ) => {
        $($send_head)* {
            $($(
                $crate::__twin_select! {
                    send [] [$($($tbound)*)?] then __twin_type {
                        [$(#[cfg $tcfg])* $(#[doc = $tdoc])*] $tname
                    }
                }
            )*)?
            $($(
                $crate::__twin_select! {
                    send [] [$($($fret)*)?] then __twin_fn {
                        [$(#[cfg $fcfg])* $(#[must_use $($fmu)*])? $(#[doc = $fdoc])* fn $fname $fparams]
                        [$(where $($fwhere)*)?]
                        [$({ $($fbody)* })?]
                    }
                }
            )*)?
            $($($send_only)*)?
        }

        $($local_head)* {
            $($(
                $crate::__twin_select! {
                    local [] [$($($tbound)*)?] then __twin_type {
                        [
                            $(#[cfg $tcfg])*
                            #[doc = concat!(
                                "The local form of [`", stringify!($send_name), "::",
                                stringify!($tname), "`]."
                            )]
                        ]
                        $tname
                    }
                }
            )*)?
            $($(
                $crate::__twin_select! {
                    local [] [$($($fret)*)?] then __twin_fn {
                        [
                            $(#[cfg $fcfg])*
                            $(#[must_use $($fmu)*])?
                            #[doc = concat!(
                                "The local form of [`", stringify!($send_name), "::",
                                stringify!($fname), "`]."
                            )]
                            // The `# Errors` and `# Panics` sections live on the Send trait's
                            // method, which this documentation points to.
                            #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
                            fn $fname $fparams
                        ]
                        [$(where $($fwhere)*)?]
                        [$({ $($fbody)* })?]
                    }
                }
            )*)?
        }

        #[diagnostic::do_not_recommend]
        $($blanket)* {
            $($(
                $crate::__twin_forward_type! { [$(#[cfg $tcfg])*] $tname $forward }
            )*)?
            $($(
                $crate::__twin_forward! {
                    [$(#[cfg $fcfg])*] $fname $fparams [$(where $($fwhere)*)?] $forward
                    [$($($fret)*)?]
                }
            )*)?
        }
    };
}

/// Keeps the tokens of one flavor: drops the other flavor's side of every `@send` and `@twin`
/// marker, then hands the result to the continuation macro as its last argument.
#[doc(hidden)]
#[macro_export]
macro_rules! __twin_select {
    ($flavor:ident [$($kept:tt)*] [] then $next:ident { $($args:tt)* }) => {
        $crate::$next! { $($args)* [$($kept)*] }
    };
    (send [$($kept:tt)*] [@send { $($only:tt)* } $($rest:tt)*] then $($next:tt)*) => {
        $crate::__twin_select! { send [$($kept)* $($only)*] [$($rest)*] then $($next)* }
    };
    (local [$($kept:tt)*] [@send { $($only:tt)* } $($rest:tt)*] then $($next:tt)*) => {
        $crate::__twin_select! { local [$($kept)*] [$($rest)*] then $($next)* }
    };
    (send [$($kept:tt)*] [@twin { $($send:tt)* } { $($local:tt)* } $($rest:tt)*] then $($next:tt)*) => {
        $crate::__twin_select! { send [$($kept)* $($send)*] [$($rest)*] then $($next)* }
    };
    (local [$($kept:tt)*] [@twin { $($send:tt)* } { $($local:tt)* } $($rest:tt)*] then $($next:tt)*) => {
        $crate::__twin_select! { local [$($kept)* $($local)*] [$($rest)*] then $($next)* }
    };
    ($flavor:ident [$($kept:tt)*] [$token:tt $($rest:tt)*] then $($next:tt)*) => {
        $crate::__twin_select! { $flavor [$($kept)* $token] [$($rest)*] then $($next)* }
    };
}

/// Writes one associated type of a trait from its attributes, its name and its selected bounds.
#[doc(hidden)]
#[macro_export]
macro_rules! __twin_type {
    ([$($attr:tt)*] $name:ident []) => {
        $($attr)* type $name;
    };
    ([$($attr:tt)*] $name:ident [$($bound:tt)+]) => {
        $($attr)* type $name: $($bound)+;
    };
}

/// Writes one method of a trait: its head, its selected return type, its `where` clause and its
/// default body, or a `;` when it has none.
#[doc(hidden)]
#[macro_export]
macro_rules! __twin_fn {
    ([$($head:tt)*] [$($where:tt)*] [] []) => {
        $($head)* $($where)*;
    };
    ([$($head:tt)*] [$($where:tt)*] [] [$($ret:tt)+]) => {
        $($head)* -> $($ret)+ $($where)*;
    };
    ([$($head:tt)*] [$($where:tt)*] [$body:tt] []) => {
        $($head)* $($where)* $body
    };
    ([$($head:tt)*] [$($where:tt)*] [$body:tt] [$($ret:tt)+]) => {
        $($head)* -> $($ret)+ $($where)* $body
    };
}

/// Writes one associated type of the blanket impl: the Send trait's type.
#[doc(hidden)]
#[macro_export]
macro_rules! __twin_forward_type {
    ([$($cfg:tt)*] $name:ident { $($forward:tt)* }) => {
        $($cfg)* type $name = $($forward)*::$name;
    };
}

/// Writes one method of the blanket impl: the twin's signature, and a body that calls the Send
/// trait's method with the same arguments.
#[doc(hidden)]
#[macro_export]
macro_rules! __twin_forward {
    (
        [$($cfg:tt)*] $name:ident (&mut $receiver:tt $(, $arg:ident: $ty:ty)* $(,)?)
        [$($where:tt)*] { $($forward:tt)* } [$($ret:tt)*]
    ) => {
        $crate::__twin_select! {
            local [] [$($ret)*] then __twin_forward_fn {
                [$($cfg)* fn $name(&mut $receiver $(, $arg: $ty)*)]
                [$($where)*]
                [{ $($forward)*::$name($receiver $(, $arg)*) }]
            }
        }
    };
    (
        [$($cfg:tt)*] $name:ident (& $receiver:tt $(, $arg:ident: $ty:ty)* $(,)?)
        [$($where:tt)*] { $($forward:tt)* } [$($ret:tt)*]
    ) => {
        $crate::__twin_select! {
            local [] [$($ret)*] then __twin_forward_fn {
                [$($cfg)* fn $name(& $receiver $(, $arg: $ty)*)]
                [$($where)*]
                [{ $($forward)*::$name($receiver $(, $arg)*) }]
            }
        }
    };
    (
        [$($cfg:tt)*] $name:ident ($receiver:tt $(, $arg:ident: $ty:ty)* $(,)?)
        [$($where:tt)*] { $($forward:tt)* } [$($ret:tt)*]
    ) => {
        $crate::__twin_select! {
            local [] [$($ret)*] then __twin_forward_fn {
                [$($cfg)* fn $name($receiver $(, $arg: $ty)*)]
                [$($where)*]
                [{ $($forward)*::$name($receiver $(, $arg)*) }]
            }
        }
    };
}

/// Writes the forwarding method once its return type is selected.
#[doc(hidden)]
#[macro_export]
macro_rules! __twin_forward_fn {
    ([$($head:tt)*] [$($where:tt)*] [$body:tt] [$($ret:tt)+]) => {
        $($head)* -> $($ret)+ $($where)* $body
    };
    ([$($head:tt)*] [$($where:tt)*] [$body:tt] []) => {
        $($head)* $($where)* $body
    };
}
