//! The browser runtime's (`wasm.rs`) rules that need no JavaScript, built on
//! every target so host tests cover them: timer delays, the interval's
//! missed-tick rule, clock and random conversions, and the callback marker.

#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::ops::Add;
use std::pin::pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The longest single `setTimeout`: `2^31 - 1` ms (about 24.8 days); a
/// longer wait is rescheduled when it fires.
pub(super) const MAX_TIMEOUT_MS: i32 = i32::MAX;

/// The `setTimeout` delay for `after`: rounded up (an early wake would only be
/// rescheduled) and clamped.
pub(super) fn timeout_ms(after: Duration) -> i32 {
    let ms = after.as_micros().div_ceil(1000);
    i32::try_from(ms).unwrap_or(MAX_TIMEOUT_MS)
}

/// A late tick at least this much past its time delays the next one (Tokio's
/// threshold for a missed tick).
pub(super) const MISSED: Duration = Duration::from_millis(5);

/// `MissedTickBehavior::Delay`: after the tick due `at`, seen at `now`, the
/// next is a period after `at`, or after `now` if the tick was late, so
/// there's never a burst of catch-up ticks.
pub(super) fn next_tick<I>(at: I, now: I, period: Duration) -> I
where
    I: Add<Duration, Output = I> + PartialOrd + Copy,
{
    if now > at + MISSED {
        now + period
    } else {
        at + period
    }
}

/// The largest time value JavaScript's `Date` holds (ECMA-262 §21.4.1.22).
const MAX_DATE_MS: f64 = 8.64e15;

/// A `Date.now()` value as a `SystemTime`; `None` for an invalid one (NaN,
/// negative, beyond `Date`'s range) or one `SystemTime` can't represent.
pub(super) fn system_time_from_ms(ms: f64) -> Option<SystemTime> {
    if !(ms.is_finite() && (0.0..=MAX_DATE_MS).contains(&ms)) {
        return None;
    }
    // In range, checked just above.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    UNIX_EPOCH.checked_add(Duration::from_millis(ms as u64))
}

/// A `Math.random()` value (`[0, 1)`) as a `u32`, for the jitter seed.
pub(super) fn random_word(x: f64) -> u32 {
    // `x * 2^32` is in [0, 2^32) for x in [0, 1); clamped anyway.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let w = (x * 4_294_967_296.0).clamp(0.0, f64::from(u32::MAX)) as u32;
    w
}

thread_local! {
    /// The client whose token or state callback is being polled (§9.5); 0
    /// for none. One thread, so "set during each poll" is a task-local.
    static IN_CALLBACK: Cell<u64> = const { Cell::new(0) };
}

/// Restores the previous value however the poll ends (a return, `Pending`,
/// a panic): nested scopes unwind in order.
struct Scope(u64);

impl Scope {
    fn enter(id: u64) -> Self {
        Self(IN_CALLBACK.replace(id))
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        IN_CALLBACK.set(self.0);
    }
}

/// Marks each poll of `future` as running a callback of client `id`.
pub(crate) async fn callback_scope<F: Future>(id: u64, future: F) -> F::Output {
    let mut future = pin!(future);
    poll_fn(|cx| {
        let _scope = Scope::enter(id);
        future.as_mut().poll(cx)
    })
    .await
}

/// Whether the current poll runs a token or state callback of client `id`.
pub(crate) fn in_callback(id: u64) -> bool {
    id != 0 && IN_CALLBACK.get() == id
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::{
        MAX_TIMEOUT_MS, MISSED, callback_scope, in_callback, next_tick, random_word,
        system_time_from_ms, timeout_ms,
    };

    #[test]
    fn timeouts_round_up_and_clamp() {
        assert_eq!(timeout_ms(Duration::ZERO), 0);
        assert_eq!(timeout_ms(Duration::from_micros(1)), 1);
        assert_eq!(timeout_ms(Duration::from_millis(250)), 250);
        // Above 2^31 - 1 ms: one setTimeout of the maximum, rescheduled.
        assert_eq!(
            timeout_ms(Duration::from_secs(30 * 24 * 3600)),
            MAX_TIMEOUT_MS
        );
        assert_eq!(timeout_ms(Duration::MAX), MAX_TIMEOUT_MS);
    }

    #[test]
    fn a_late_tick_delays_the_next_without_a_burst() {
        let t0 = Instant::now();
        let p = Duration::from_secs(2);
        // On time (or within the threshold): the schedule holds.
        assert_eq!(next_tick(t0, t0, p), t0 + p);
        assert_eq!(next_tick(t0, t0 + MISSED, p), t0 + p);
        // Late, as after a background tab resumes: a full period after now.
        let now = t0 + Duration::from_secs(60);
        assert_eq!(next_tick(t0, now, p), now + p);
    }

    #[test]
    fn clock_values_convert_or_are_invalid() {
        assert_eq!(system_time_from_ms(0.0), Some(UNIX_EPOCH));
        assert_eq!(
            system_time_from_ms(1_700_000_000_123.0),
            Some(UNIX_EPOCH + Duration::from_millis(1_700_000_000_123))
        );
        for bad in [f64::NAN, f64::INFINITY, -1.0, 8.64e15 + 1.0, f64::MAX] {
            assert_eq!(system_time_from_ms(bad), None, "{bad}");
        }
        // Year 2100 is representable everywhere.
        assert!(system_time_from_ms(4.1e12).is_some_and(|t| t > SystemTime::UNIX_EPOCH));
        // The largest `Date` (year 275760) is past some platforms'
        // `SystemTime` (Windows: year 30828): `None` there, never a panic.
        let _ = system_time_from_ms(8.64e15);
    }

    #[test]
    fn random_words_span_the_range() {
        assert_eq!(random_word(0.0), 0);
        assert_eq!(random_word(0.5), 1 << 31);
        // Just below 1, the largest value Math.random() gives.
        assert_eq!(random_word(1.0 - f64::EPSILON), u32::MAX);
    }

    /// Polls `f` once.
    fn poll_once<F: Future>(f: std::pin::Pin<&mut F>) -> Poll<F::Output> {
        f.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn callback_scope_is_set_during_each_poll_only() {
        let mut ready = false;
        let mut f = pin!(callback_scope(7, async {
            assert!(in_callback(7));
            // Pending once: between polls, the marker is gone.
            std::future::poll_fn(|_| {
                if ready {
                    Poll::Ready(())
                } else {
                    ready = true;
                    Poll::Pending
                }
            })
            .await;
            assert!(in_callback(7));
            // Nested, for another client: the outer id comes back after.
            callback_scope(8, async { assert!(in_callback(8) && !in_callback(7)) }).await;
            assert!(in_callback(7));
        }));
        assert!(poll_once(f.as_mut()).is_pending());
        assert!(!in_callback(7), "set between polls");
        assert!(poll_once(f.as_mut()).is_ready());
        assert!(!in_callback(7) && !in_callback(0));
    }

    #[test]
    fn callback_scope_restores_after_a_panic() {
        let r = std::panic::catch_unwind(|| {
            let mut f = pin!(callback_scope(9, async { panic!("in a callback") }));
            let _ = poll_once(f.as_mut());
        });
        assert!(r.is_err());
        assert!(!in_callback(9), "the marker leaked past a panic");
    }
}
