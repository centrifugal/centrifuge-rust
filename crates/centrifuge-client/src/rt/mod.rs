//! The runtime the client runs on: tasks, timers, clocks and the callback
//! marker (docs/wasm-plan.md, step 3). Tokio on native; the browser's event
//! loop on wasm. The interface is what the call sites need, not a copy of
//! Tokio's.
//!
//! - `spawn(future) -> AbortHandle`: abort-only (`abort()`, `is_finished()`),
//!   never awaited; dropping the handle doesn't abort the task.
//! - `Instant`, `sleep_until`, `timeout`, `timeout_at` (an `Err` when the time is up), and
//!   `interval(period)` (first tick at once; a late tick delays the next, no
//!   catch-up burst).
//! - `to_core` / `from_core`: to and from the core's `Instant`. Natively the
//!   driver's clock is Tokio's, so a runtime with paused time pauses the
//!   core's clock too.
//! - `callback_scope(id, future)` / `in_callback(id)`: "this task runs a token
//!   or state callback of client `id`" (§9.5).
//! - `system_now()`: wall-clock time; `None` only where the platform's clock
//!   value is invalid.
//! - `random_seed()`: a jitter seed, not cryptographic.

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::*;
#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub(crate) use wasm::*;
