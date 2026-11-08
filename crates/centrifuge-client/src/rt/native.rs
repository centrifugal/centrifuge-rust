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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::{Instant, callback_scope, in_callback, interval, sleep_until, spawn, timeout};

    #[tokio::test(start_paused = true)]
    async fn abort_stops_the_task_and_dropping_the_handle_does_not() {
        let ran = Arc::new(AtomicBool::new(false));
        let r = ran.clone();
        let h = spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            r.store(true, Ordering::SeqCst);
        });
        h.abort();
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(h.is_finished());
        assert!(!ran.load(Ordering::SeqCst));

        let r = ran.clone();
        drop(spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            r.store(true, Ordering::SeqCst);
        }));
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(ran.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_polls_the_future_first_and_cancels_on_expiry() {
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        assert_eq!(timeout(Duration::ZERO, async { 1 }).await.ok(), Some(1));
        let dropped = Arc::new(AtomicBool::new(false));
        let g = Guard(dropped.clone());
        let r = timeout(Duration::from_secs(1), async move {
            let _g = g;
            std::future::pending::<()>().await;
        })
        .await;
        assert!(r.is_err());
        assert!(dropped.load(Ordering::SeqCst));
        // A past deadline completes at once.
        sleep_until(Instant::now()).await;
    }

    #[tokio::test(start_paused = true)]
    async fn interval_ticks_at_once_then_delays_after_a_late_tick() {
        let mut i = interval(Duration::from_secs(2));
        let t0 = Instant::now();
        i.tick().await;
        assert_eq!(t0.elapsed(), Duration::ZERO);
        // Late by 5 s: the next tick is a full period after it, no burst.
        tokio::time::sleep(Duration::from_secs(5)).await;
        i.tick().await;
        let late = Instant::now();
        i.tick().await;
        assert_eq!(late.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test]
    async fn callback_scope_marks_only_its_task() {
        assert!(!in_callback(7));
        callback_scope(7, async {
            assert!(in_callback(7));
            assert!(!in_callback(8));
            tokio::task::yield_now().await;
            assert!(in_callback(7));
            // Another task never sees it.
            let other = tokio::spawn(async { in_callback(7) }).await.unwrap();
            assert!(!other);
            callback_scope(8, async {
                assert!(in_callback(8));
                assert!(!in_callback(7));
            })
            .await;
            assert!(in_callback(7));
        })
        .await;
        assert!(!in_callback(7));
    }
}
