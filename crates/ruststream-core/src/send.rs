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

/// Chooses between two types by the `send` feature: [`IfSend`] is `Yes` where it is on and `No`
/// where it is off.
///
/// A crate that cannot see this crate's features names its sharing primitive through it, so the
/// choice follows the feature as the build unifies it: a lock that is `Sync` next to a
/// multi-thread runtime, a lock that costs nothing without one. The choice is made at compile
/// time and costs nothing at run time.
#[doc(hidden)]
pub trait Pick<Yes, No> {
    /// The chosen type.
    type Picked;
}

/// The type [`IfSend`] picks through.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct WhenSend;

#[cfg(feature = "send")]
impl<Yes, No> Pick<Yes, No> for WhenSend {
    type Picked = Yes;
}

#[cfg(not(feature = "send"))]
impl<Yes, No> Pick<Yes, No> for WhenSend {
    type Picked = No;
}

/// `Yes` where the `send` feature is on, `No` otherwise.
#[doc(hidden)]
pub type IfSend<Yes, No> = <WhenSend as Pick<Yes, No>>::Picked;
