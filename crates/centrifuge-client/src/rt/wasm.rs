//! The browser's event loop, reached through the global scope (`window` or a
//! worker's `self`), never through `window` (docs/wasm-plan.md, step 3).

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::{AbortHandle as Abort, Abortable};
use wasm_bindgen::prelude::*;
pub(crate) use web_time::Instant;

// ---- Tasks ----

/// Abort-only: the task stops the next time it would be polled.
#[derive(Clone, Debug)]
pub(crate) struct AbortHandle {
    abort: Abort,
    finished: Rc<Cell<bool>>,
}

impl AbortHandle {
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }

    /// The task completed or was aborted (its future is dropped).
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.get()
    }
}

/// Marks the task finished when its future is dropped, however it ends.
struct Finished(Rc<Cell<bool>>);

impl Drop for Finished {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

pub(crate) fn spawn<F>(future: F) -> AbortHandle
where
    F: Future<Output = ()> + 'static,
{
    let (abort, registration) = Abort::new_pair();
    let finished = Rc::new(Cell::new(false));
    let done = Finished(finished.clone());
    wasm_bindgen_futures::spawn_local(async move {
        let _done = done;
        let _ = Abortable::new(future, registration).await;
    });
    AbortHandle { abort, finished }
}

// ---- Timers ----

#[wasm_bindgen]
extern "C" {
    // The id is a number in browsers; kept opaque.
    #[wasm_bindgen(js_name = setTimeout)]
    fn set_timeout(handler: &js_sys::Function, ms: i32) -> JsValue;
    #[wasm_bindgen(js_name = clearTimeout)]
    fn clear_timeout(id: &JsValue);
}

/// The longest single `setTimeout`: `2^31 - 1` ms (about 24.8 days); a
/// longer wait reschedules.
const MAX_TIMEOUT_MS: u128 = i32::MAX as u128;

/// One pending `setTimeout`. Its callback only wakes a task through the
/// shared slot, so one that fires anyway (its `clearTimeout` raced the event
/// loop) finds nothing to do.
struct Timer {
    id: JsValue,
    fired: Rc<Cell<bool>>,
    waker: Rc<RefCell<Option<Waker>>>,
    // Freed after `clearTimeout` (in `Drop`, which runs before the fields
    // drop): the browser must never call a freed callback.
    _callback: Closure<dyn FnMut()>,
}

impl Timer {
    fn new(after: Duration, waker: Waker) -> Self {
        let fired = Rc::new(Cell::new(false));
        let slot = Rc::new(RefCell::new(Some(waker)));
        let callback = {
            let fired = fired.clone();
            let slot = slot.clone();
            Closure::<dyn FnMut()>::new(move || {
                fired.set(true);
                if let Some(w) = slot.borrow_mut().take() {
                    w.wake();
                }
            })
        };
        // Rounded up: an early wake is rescheduled anyway, but a late one is
        // cheaper than a wasted round trip.
        let ms = after.as_micros().div_ceil(1000).min(MAX_TIMEOUT_MS);
        let ms = i32::try_from(ms).unwrap_or(i32::MAX);
        let id = set_timeout(callback.as_ref().unchecked_ref(), ms);
        Self {
            id,
            fired,
            waker: slot,
            _callback: callback,
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        clear_timeout(&self.id);
    }
}

/// Deadline-based: a timer that fires early (clamping) or late (background
/// tabs) is checked against the clock, and rescheduled if early.
pub(crate) struct Sleep {
    deadline: Instant,
    timer: Option<Timer>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let now = Instant::now();
        if now >= this.deadline {
            this.timer = None;
            return Poll::Ready(());
        }
        match &this.timer {
            Some(t) if !t.fired.get() => {
                *t.waker.borrow_mut() = Some(cx.waker().clone());
            }
            // None yet, or fired before the deadline: one pending at a time,
            // the old one cleared (dropped) as it's replaced.
            _ => this.timer = Some(Timer::new(this.deadline - now, cx.waker().clone())),
        }
        Poll::Pending
    }
}

pub(crate) fn sleep_until(deadline: Instant) -> Sleep {
    Sleep {
        deadline,
        timer: None,
    }
}

/// The time was up before the future completed.
#[derive(Debug)]
pub(crate) struct Elapsed;

/// Tokio's order: the future is polled first, so one that completes on the
/// poll at the deadline wins.
pub(crate) async fn timeout_at<F: Future>(
    deadline: Instant,
    future: F,
) -> Result<F::Output, Elapsed> {
    let mut future = pin!(future);
    let mut sleep = sleep_until(deadline);
    poll_fn(|cx| {
        if let Poll::Ready(v) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        Pin::new(&mut sleep).poll(cx).map(|()| Err(Elapsed))
    })
    .await
}

pub(crate) async fn timeout<F: Future>(d: Duration, future: F) -> Result<F::Output, Elapsed> {
    match Instant::now().checked_add(d) {
        Some(deadline) => timeout_at(deadline, future).await,
        // Beyond the clock's range: never.
        None => Ok(future.await),
    }
}

/// A late tick at least this much past its time delays the next one (Tokio's
/// threshold for a missed tick).
const MISSED: Duration = Duration::from_millis(5);

pub(crate) struct Interval {
    period: Duration,
    next: Instant,
}

impl Interval {
    /// Cancel-safe: dropped before it completes, the tick stays due.
    pub(crate) async fn tick(&mut self) -> Instant {
        let at = self.next;
        sleep_until(at).await;
        let now = Instant::now();
        // `MissedTickBehavior::Delay`: a late tick schedules the next one a
        // full period after it, so there's never a burst of catch-up ticks.
        self.next = if now > at + MISSED {
            now + self.period
        } else {
            at + self.period
        };
        at
    }
}

/// Ticks every `period`, the first at once; a late tick delays the next.
pub(crate) fn interval(period: Duration) -> Interval {
    Interval {
        period,
        next: Instant::now(),
    }
}

pub(crate) fn to_core(at: Instant) -> centrifuge_client_core::time::Instant {
    at
}

pub(crate) fn from_core(at: centrifuge_client_core::time::Instant) -> Instant {
    at
}

// ---- Callback marker ----

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

pub(crate) async fn callback_scope<F: Future>(id: u64, future: F) -> F::Output {
    let mut future = pin!(future);
    poll_fn(|cx| {
        let _scope = Scope::enter(id);
        future.as_mut().poll(cx)
    })
    .await
}

/// Whether the current task runs a token or state callback of client `id`.
pub(crate) fn in_callback(id: u64) -> bool {
    id != 0 && IN_CALLBACK.get() == id
}

// ---- Wall clock and randomness ----

/// The largest time value JavaScript's `Date` holds (ECMA-262 §21.4.1.22).
const MAX_DATE_MS: f64 = 8.64e15;

/// The first invalid reading is a warning, later ones debug: a broken clock
/// can't flood the log.
static CLOCK_WARNED: AtomicBool = AtomicBool::new(false);

/// `Date.now()`, with millisecond precision; `None` for an invalid value.
pub(crate) fn system_now() -> Option<SystemTime> {
    let ms = js_sys::Date::now();
    let time = if ms.is_finite() && (0.0..=MAX_DATE_MS).contains(&ms) {
        // In range, checked just above.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        UNIX_EPOCH.checked_add(Duration::from_millis(ms as u64))
    } else {
        None
    };
    if time.is_none() {
        if CLOCK_WARNED.swap(true, Ordering::Relaxed) {
            tracing::debug!(target: "centrifuge", ms, "invalid wall-clock time");
        } else {
            tracing::warn!(target: "centrifuge", ms, "invalid wall-clock time");
        }
    }
    time
}

/// `Math.random()` (not cryptographic, as jitter needs) and the wall clock,
/// through the same hasher as native.
pub(crate) fn random_seed() -> u64 {
    use std::hash::{BuildHasher as _, Hasher as _};
    // `x` is in [0, 1), so `x * 2^32` fits a u32.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let word = || (js_sys::Math::random() * 4_294_967_296.0) as u32;
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64((u64::from(word()) << 32) | u64::from(word()));
    if let Some(t) = system_now() {
        h.write_u128(t.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos());
    }
    h.finish()
}
