//! Tokio: thin wrappers.

use std::future::Future;
use std::time::{Duration, SystemTime};

pub(crate) use tokio::task::AbortHandle;
pub(crate) use tokio::time::{Instant, Interval, sleep_until, timeout, timeout_at};

pub(crate) fn spawn<F>(future: F) -> AbortHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future).abort_handle()
}

/// Ticks every `period`, the first at once; a late tick delays the next.
pub(crate) fn interval(period: Duration) -> Interval {
    let mut i = tokio::time::interval(period);
    i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    i
}

pub(crate) fn to_core(at: Instant) -> centrifuge_client_core::time::Instant {
    at.into_std()
}

pub(crate) fn from_core(at: centrifuge_client_core::time::Instant) -> Instant {
    Instant::from_std(at)
}

tokio::task_local! {
    /// The client whose token or state callback this task runs (§9.5).
    static IN_CALLBACK: u64;
}

pub(crate) fn callback_scope<F: Future>(id: u64, future: F) -> impl Future<Output = F::Output> {
    IN_CALLBACK.scope(id, future)
}

/// Whether the current task runs a token or state callback of client `id`.
pub(crate) fn in_callback(id: u64) -> bool {
    IN_CALLBACK.try_with(|c| *c == id).unwrap_or(false)
}

// Same signature as on wasm, where the clock can be invalid.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn system_now() -> Option<SystemTime> {
    Some(SystemTime::now())
}

/// Seeds backoff jitter differently per client and process, so many clients
/// restarted together don't reconnect in lockstep.
pub(crate) fn random_seed() -> u64 {
    use std::hash::{BuildHasher as _, Hasher as _};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    h.finish()
}
