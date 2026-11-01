//! The bounds a contract trait puts on its futures and its implementors: `Send` where the `send`
//! feature is on, nothing otherwise.

/// `Send` where the `send` feature is on, and nothing otherwise.
///
/// A contract trait names it where a multi-thread runtime needs a future it can move between
/// threads, so a build for a single-thread runtime (an embedded one) uses the same trait with
/// no `Send` asked of the implementor. The runtime crates that move work between threads enable
/// the feature; a build with both kinds takes the stricter bound.
#[cfg(feature = "send")]
#[doc(hidden)]
pub trait MaybeSend: Send {}

#[cfg(feature = "send")]
impl<Value: Send + ?Sized> MaybeSend for Value {}

/// See the `send` form: without the feature every type is `MaybeSend`.
#[cfg(not(feature = "send"))]
#[doc(hidden)]
pub trait MaybeSend {}

#[cfg(not(feature = "send"))]
impl<Value: ?Sized> MaybeSend for Value {}

/// `Send + Sync` where the `send` feature is on, and nothing otherwise: what a value shared
/// between the tasks of a multi-thread runtime must be.
#[cfg(feature = "send")]
#[doc(hidden)]
pub trait MaybeSendSync: Send + Sync {}

#[cfg(feature = "send")]
impl<Value: Send + Sync + ?Sized> MaybeSendSync for Value {}

/// See the `send` form: without the feature every type is `MaybeSendSync`.
#[cfg(not(feature = "send"))]
#[doc(hidden)]
pub trait MaybeSendSync {}

#[cfg(not(feature = "send"))]
impl<Value: ?Sized> MaybeSendSync for Value {}

/// `Sync` where the `send` feature is on, and nothing otherwise: what a value borrowed across a
/// thread-crossing future must be.
#[cfg(feature = "send")]
#[doc(hidden)]
pub trait MaybeSync: Sync {}

#[cfg(feature = "send")]
impl<Value: Sync + ?Sized> MaybeSync for Value {}

/// See the `send` form: without the feature every type is `MaybeSync`.
#[cfg(not(feature = "send"))]
#[doc(hidden)]
pub trait MaybeSync {}

#[cfg(not(feature = "send"))]
impl<Value: ?Sized> MaybeSync for Value {}
