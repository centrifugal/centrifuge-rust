//! `Send`/`Sync` bounds that disappear on `wasm32`, where futures and
//! handlers run on one thread and aren't `Send` (docs/spec.md §9.11).
//!
//! Every public bound uses these instead of `Send`/`Sync` directly, so a
//! browser build later relaxes the bounds without changing any signature.

/// `Send` on native targets; implemented for every type on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` on native targets; implemented for every type on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on native targets; implemented for every type on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}

/// `Sync` on native targets; implemented for every type on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}

/// `dyn Trait + Send` on native targets, `dyn Trait` on `wasm32`: the stored
/// form of a `MaybeSend` handler or future, since a trait object can only add
/// auto traits.
#[cfg(not(target_arch = "wasm32"))]
macro_rules! dyn_send {
    ($($t:tt)+) => { dyn $($t)+ + Send };
}
#[cfg(target_arch = "wasm32")]
macro_rules! dyn_send {
    ($($t:tt)+) => { dyn $($t)+ };
}
pub(crate) use dyn_send;

/// `dyn Trait + Send + Sync` on native targets, `dyn Trait` on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
macro_rules! dyn_send_sync {
    ($($t:tt)+) => { dyn $($t)+ + Send + Sync };
}
#[cfg(target_arch = "wasm32")]
macro_rules! dyn_send_sync {
    ($($t:tt)+) => { dyn $($t)+ };
}
pub(crate) use dyn_send_sync;
