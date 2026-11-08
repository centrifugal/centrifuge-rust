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

use super::logic;
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
        let id = set_timeout(callback.as_ref().unchecked_ref(), logic::timeout_ms(after));
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

pub(crate) struct Interval {
    period: Duration,
    next: Instant,
}

impl Interval {
    /// Cancel-safe: dropped before it completes, the tick stays due.
    pub(crate) async fn tick(&mut self) -> Instant {
        let at = self.next;
        sleep_until(at).await;
        self.next = logic::next_tick(at, Instant::now(), self.period);
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

pub(crate) use super::logic::{callback_scope, in_callback};

// ---- Wall clock and randomness ----

/// The first invalid reading is a warning, later ones debug: a broken clock
/// can't flood the log.
static CLOCK_WARNED: AtomicBool = AtomicBool::new(false);

/// `Date.now()`, with millisecond precision; `None` for an invalid value.
pub(crate) fn system_now() -> Option<SystemTime> {
    let ms = js_sys::Date::now();
    let time = logic::system_time_from_ms(ms);
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
    let word = || logic::random_word(js_sys::Math::random());
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64((u64::from(word()) << 32) | u64::from(word()));
    if let Some(t) = system_now() {
        h.write_u128(t.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos());
    }
    h.finish()
}
