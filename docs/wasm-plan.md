# Wasm (browser) support: implementation plan

Status: in progress. Steps 1–5 are done: the client builds for wasm32, without browser transports yet.

## Summary

- **One client, not two.** Protocol, reconnects, subscriptions, recovery and
  the driver stay shared. Only two layers differ by target: an internal
  `rt` module (tasks, timers, clocks, the callback marker: tokio on native,
  the browser event loop on wasm) and the transports (browser `WebSocket`
  and `fetch` behind the existing internal trait).
- **No tokio runtime in the browser**: only `tokio::sync` and `select!`.
- **Native API and behavior unchanged.** The wasm differences: `Inline`
  event delivery by default, no `runtime()` and no
  `TlsConfig::rustls_client_config` (feature `rustls-config`).
- **Platform checks in one `platform` module**: what can never work fails
  at `build()`; what can become true later is checked per attempt and
  retried.
- **Browser cleanup on drop**: a closed session aborts its `fetch`es and
  POSTs and detaches its WebSocket handlers.
- **Centrifugal semantics unchanged**: close codes, reconnect rules and call
  failures go through the shared core.
- **Order**: build baseline and audit, then `rt` and `platform` with native
  unchanged, then WebSocket (with a real-browser test), HTTP-streaming,
  browser lifecycle events (network, back/forward cache), the API, full
  tests, docs.
- **Open until implemented**: binary size (estimated, measured in step 9),
  Safari (not claimed until tested).

## Goal

Run `centrifuge-client` on `wasm32-unknown-unknown` with both transports
(WebSocket and HTTP-streaming), **preserving the native public API and its
behavior** (spec §9.11). On wasm, the API may differ in defaults and trait
bounds where the browser execution model requires it; each difference is
listed and documented (the `EventDelivery` default, decision 8; the
native-only `runtime()`, decision 6; and `TlsConfig::rustls_client_config`,
step 8). `close()` keeps its `Send` future on both: it only awaits a
`tokio::sync::oneshot` receiver, which is `Send` on wasm too (step 3).

## Verified starting point

- `centrifuge-client-core` already builds for `wasm32-unknown-unknown`.
- `MaybeSend` / `MaybeSync` exist (`src/maybe_send.rs`) and every public
  handler bound uses them.
- The `Transport` trait is internal (spec §9.6, §9.11).
- The core's `StreamDecoder` (`push` / `finish`) already handles arbitrary
  chunk boundaries and reports EOF with an incomplete frame as an error; the
  native HTTP-stream transport uses it.
- Every transport message to the driver carries a `TransportId`
  (`Inbound::Opened` / `Item` / `Closed`), and the driver matches state by it,
  so a replaced transport's late messages can't reach the new connection.
- `cargo check --target wasm32-unknown-unknown -p centrifuge-client` fails
  first on `tokio`'s `net` feature (`mio` is unsupported on wasm). Later errors
  are unknown until the native-only dependencies are gated.
- **Android support (#2, #4) set the pattern for platform checks.**
  - `centrifuge::android` is Android-only, and `jni` is an Android-only
    dependency, so neither reaches wasm.
  - Its preconditions are checked in two places. In `build()`,
    `configured_tls` plus `Net::android_tls_ready()` check TLS endpoints, an
    `https://` emulation endpoint and an explicit `https://` proxy. At the
    start of `Net::connect()`, before any DNS or TCP, the same check covers
    proxies known only per attempt.
  - That is 8 `cfg(target_os = "android")` sites across `lib.rs`,
    `client.rs` and `transport/net.rs`.
  - Helpers: `TlsConfig::uses_platform_verifier()` and `net::uses_tls(url)`.
- **Threads.** `EventDelivery::Thread`, the default, spawns the
  `centrifuge-events` thread. On wasm32-unknown-unknown
  `std::thread::Builder::spawn` returns `Err`; this was checked with a small
  module run under Node. Since #5, a failed spawn makes `build()` fail with
  `Error::Configuration`. Before, the client ran and silently delivered no
  events.

### Runtime and time usage (full audit, step 2)

Every call site in the client's library code (tests excluded), and the core's
time API.

| Use | Call sites | On wasm |
|---|---|---|
| `tokio::spawn` | `driver.rs`: the transport task (476) and the three callback tasks (509, 530, 543) | `rt::spawn` |
| `JoinHandle` / `AbortHandle` | `driver.rs`: `TransportHandle::task` (63), `tasks` (88); only `.abort()`-ed (492, 570), never awaited | abort-only handle |
| `tokio::runtime::Handle` | `client.rs`: builder field (630), `runtime()` (695), `try_current()` in `build()` (882); `dispatch.rs`: `Dispatcher::new` / `with_spawn` (613, 621), entered by the event thread | native-only: the thread variant isn't built on wasm |
| `std::thread::Builder::spawn` | `dispatch.rs`: the event thread (565–580) | native-only (decision 8) |
| `tokio::task_local!` | `dispatch.rs`: `IN_CALLBACK` (34), read by `in_callback()` (46); `.scope(..)` in `driver.rs` (509, 530, 543) | `rt::callback_scope` / `rt::in_callback` |
| `tokio::time::{Instant, sleep_until, timeout, timeout_at}` | `calls.rs`: call deadlines (108, 193, 229–239, 252, 266), `ready()` / `sub.ready()` timeouts (314, 368) | `rt` |
| `tokio::time::{interval, Interval, MissedTickBehavior::Delay}`, `sleep_until` | `driver.rs`: the 2 s watchdog (155–156, 179), the core's next deadline (184) | `rt` |
| `std::time::Instant` | `driver.rs`: `now()` for the core (150–151), the watchdog's monotonic half (83, 142, 271); `transport/mod.rs`: `LastReceived` (118, 125, 137) | the driver's `now()`: `rt::Instant`; the watchdog and `LastReceived`, which read the real clock: the core's `Instant` (`std::time::Instant::now()` panics on wasm) |
| `std::time::Instant` in the **core's** API | `centrifuge-client-core`: `Core::step(input, now)`, `next_deadline()`, `last_received`, the timer wheel; the core itself only calls `now()` in tests | `centrifuge_client_core::time::Instant`: `std::time::Instant` on native, `web_time::Instant` on wasm (step 3) |
| `SystemTime::now()` | `driver.rs`: watchdog (142, 271), `connected_since` (429); `client.rs`: `random_seed` (1000) | `rt::system_now()` |
| `SystemTime` in the **public** API | `Statistics::connected_since` (`stats.rs`), `Shared::connected_since` | stays `std::time::SystemTime` |
| `RandomState` | `client.rs`: `random_seed` (998) | not randomly seeded on wasm: `rt::random_seed()` |

`Send` requirements beyond those of `tokio::spawn`:

| Where | What | On wasm |
|---|---|---|
| `dispatch.rs:63` | handler storage `H<T, E> = Option<Box<dyn FnMut(&T, E) + Send>>` | `+ Send` per target: a `dyn` type can't name `MaybeSend` |
| `driver.rs:29–46` | `TokenFuture`, `SubTokenFuture`, `StateFuture` (`+ Send`) and their getters (`+ Send + Sync`) | the same |
| `dispatch.rs:565–580` | the event thread's `Box<dyn FnOnce() + Send>` | native-only with the thread |
| `client.rs:187` | `close()` returns `impl Future + Send` | unchanged: the future only awaits a `oneshot` receiver, `Send` on wasm too |
| `error.rs` | `BoxError`, `ArcError`, `From<E: Error + Send + Sync>` for `TokenError` | unchanged: public types, and errors are plain data. A browser error (`JsValue`) is converted to a message first |

No `select!` holds a non-`Send` value across an `.await` once the aliases
above are per target: on wasm nothing requires `Send` (`spawn_local`).
`std::sync` locks, `catch_unwind` and `thread_local!` (`HandlerScope`) are
available on wasm unchanged.

## Supported environments

| Environment | Initial support |
|---|---|
| Browser main thread | Supported |
| Dedicated Worker | Supported, integration tested |
| Shared Worker | Expected to work; not initially verified. Same global APIs as a Dedicated Worker, but it lives as long as any connected page does, so one client may outlive the page that created it. The SDK doesn't manage the Shared Worker's lifetime or track the pages connected to it; the app decides when to close the client |
| Service Worker | Not supported: the browser stops it when idle, so it can't hold a long-lived connection |
| Node.js | Not supported initially (separate decision) |
| Other JS runtimes | Not guaranteed |

- No dependency on `window`: `WebSocket`, `fetch`, `setTimeout`,
  `AbortController`, `Date` and `Math` are reached through the global scope
  (`js_sys::global()`).
- The global APIs are looked up and validated, not assumed from the target,
  and a missing one gives a clear "unsupported environment"
  `Error::Configuration` instead of a panic. They're checked in
  `platform::check_build` (decision 7), before anything uses them:
  `setTimeout` / `clearTimeout` always (timers run from the first
  connection attempt), and `WebSocket`, or `fetch` with `AbortController`,
  for the kind of every configured endpoint, fallbacks included (fallback is the configured endpoint list, tried in order). `Date` and `Math` are part of
  JavaScript itself, present in every engine, and not checked. The
  transport also checks its API when it creates a session, so a global
  removed after `build()` fails that attempt instead of panicking.
- **Browser calls that can throw are called with `catch`.** Some throw
  under their own spec: the `WebSocket` constructor (an invalid URL),
  `send()` and `close()` (wrong state, an invalid close code). Their
  bindings are declared with `#[wasm_bindgen(catch)]`, and an exception
  goes through step 5's error conversion; `fetch` rejections already arrive
  as `Err` through `JsFuture`. Built-ins replaced by the page with
  functions that throw (`Math.random`, `Date.now`, `setTimeout`) are out of
  scope, as for any library.
- Tasks run on `spawn_local`, which needs a running JavaScript event loop;
  document that.
- Initially tested browsers: Chrome and Firefox. Safari / WebKit is not
  claimed until it is tested.

## Design decisions

1. **One internal runtime abstraction; no separate wasm driver.** Tokio calls
   are spread over driver, calls and dispatch (about 2,000 lines of glue); a
   wasm driver would duplicate them. Similar architectural approach to
   matrix-rust-sdk (`matrix_sdk_common::executor`) and iroh (`n0-future`).
   reqwest's separate wasm implementation shows the alternative's risk: its
   wasm API has drifted (missing `wrap_stream`, timeouts).
2. **No Tokio runtime on wasm.** Wasm uses Tokio's synchronization primitives
   and macros where supported, but never instantiates a Tokio runtime. Task
   execution, task-locals, timers and clocks come from the internal `rt`
   module.
3. **A small `rt` interface driven by actual call sites,** not a copy of
   tokio's API.
4. **Transport behind the existing internal trait.** The browser transports
   carry bytes only; protocol, emulation, auth and token refresh stay shared.
5. **No runtime or JavaScript types in the public API.**
6. **An option the app explicitly sets, but that means nothing on the platform,
   is a configuration error at `build()`** (spec §9.11). This is the same rule
   Android follows for extra roots.
   - Setters such as `proxy`, `tls` and `http_headers` keep their native
     signatures on wasm, and `ClientBuilder::build()` returns a
     configuration error if one was set.
   - "Explicitly" matters. A default documented as best effort stays best
     effort: `ProxyConfig::System` is the default and already means
     "environment variables only" on Linux, iOS and Android. In a browser it
     means "the browser decides", with no error.
   - An option that another option overrides is documented on both, as extra
     roots under a custom rustls config are today.
   - The one exception is `runtime(tokio::runtime::Handle)`. Its parameter
     type doesn't exist without a Tokio runtime, so it's native-only.
   - `emulated_headers` is the browser replacement for `http_headers`: those
     headers travel inside the connect command (Centrifugo header emulation),
     not as HTTP headers.
7. **Platform preconditions live in one internal `platform` module.** It has
   two hooks with a body per platform (native no-op, Android, wasm):
   - `platform::check_build(..)`, called from `build()`, checks everything
     known at build time;
   - `platform::check_attempt(..)`, called at the start of every connection
     attempt before any I/O, checks what's only known then, and fails that
     attempt so it's retried with backoff.

   The split is also the line between permanent and transient. A condition
   that can't change for the life of the client (an unsupported option, a
   missing browser API, `EventDelivery::Thread` on wasm) is checked in
   `check_build`, so it never starts a reconnect loop. `check_attempt` is
   only for conditions that can become true later, which is why retrying is
   right: Android's verifier can be initialized after `build()`, and the
   proxy is chosen per attempt. A browser global deleted after `build()`
   is the only permanent case `check_attempt` can meet; retrying it is
   harmless (backoff caps the rate, each attempt reports the error), and
   giving it a terminal path of its own isn't worth the core change.

   Android's existing checks move there first (step 3), so the wasm checks
   (unsupported options, `EventDelivery::Thread`, a missing browser API; see
   "Supported environments") don't
   add more scattered `cfg` blocks to `build()` and `connect()`.
8. **`EventDelivery::Inline` is the default on wasm.** There are no threads,
   and since #5 a failed spawn fails `build()`, so keeping `Thread` as the
   wasm default would make every default-configured browser client fail.
   - On wasm, `EventDelivery::default()` returns `Inline` (a target-dependent
     default).
   - An explicit `Thread` fails in `platform::check_build` with a
     wasm-specific message, before any spawn is attempted.
   - Native defaults are unchanged.
   - Inline means callbacks run on the browser's event loop, the same thread
     that reads sockets, fires timers and renders the page. A slow callback
     delays incoming messages, timers and reconnects, and blocks other work
     on the page. Document that callbacks must not block (on native, an
     `Inline` callback already delays the driver the same way).

## Steps

### 1. Wasm compilation baseline
- Move native-only dependencies under `cfg(not(target_arch = "wasm32"))`:
  `tokio` `net` and `time`, `tokio-tungstenite`, `tokio-rustls`,
  `rustls-platform-verifier`, `hyper`, `hyper-util`, `http-body-util`, and
  `rustls` itself if `TlsConfig` can be kept without it (see step 8); gate the
  code that uses them.
- Tokio on wasm gets `sync` and `macros` only, without `rt`. One used
  symbol needs `rt`: `tokio::task_local!`, for `IN_CALLBACK`. Once the `rt`
  module provides a task-local of its own (step 3), nothing else does. A
  prototype outside this repository reported the same; confirm it here.
- Re-run `cargo check --target wasm32-unknown-unknown -p centrifuge-client`
  and list every remaining error.
- `cargo tree --target wasm32-unknown-unknown -e features` to catch native-only
  dependencies leaking into the wasm graph.
- `getrandom` (a prototype outside this repository reported the expected
  result, an empty `cargo tree -i` for every version after gating; confirm it
  here): `Cargo.lock` has 0.2.17, 0.3.4 and 0.4.3. In the client's
  normal dependency graph only 0.4.3 appears, through
  `tokio-tungstenite -> tungstenite -> rand`, which is native-only after
  gating. So the wasm graph is expected to contain **no** `getrandom`.
  Confirm with `cargo tree -i getrandom@<v> --target wasm32-unknown-unknown`
  for each version. If one does appear, do not enable its JavaScript backend
  unconditionally (`js` / `wasm_js`): the maintainers discourage that in
  libraries, and wasm32-unknown-unknown is not always a browser. Gate it
  behind an explicit feature instead, and document it.
- Search for blocking tokio APIs (`spawn_blocking`, `block_in_place`,
  `blocking_lock` / `blocking_recv` / `blocking_send`) on the browser path.
  None exist today; keep it that way.

**Result (done).**
- Native-only: `hyper`, `hyper-util`, `http-body-util`, `rustls`,
  `rustls-platform-verifier`, `tokio-rustls`, `tokio-tungstenite`, and
  tokio's `rt`, `time`, `net` and `io-util`. Tokio on wasm resolves to
  `sync` and `macros` only.
- `rustls` can't be a wasm dependency: the `aws-lc-rs` (default) and `ring`
  features enable `rustls/aws_lc_rs` / `rustls/ring` on every declaration of
  `rustls`, which pulls `aws-lc-sys` into the wasm build, and Cargo rejects
  the same crate under a second name. `TlsConfig` uses `rustls-pki-types`
  directly instead, the crate `rustls` re-exports its certificate types
  from. So `TlsConfig` is the same type on both targets, with no `cfg`
  variant. `rustls_client_config` is native-only (step 8).
- `transport::net` (the native connector) is replaced on wasm by a stand-in
  holding the settings the shared code reads (`proxy`); `ws` and
  `http_stream` are native-only. `uses_tls` moved to `transport`.
- `getrandom`: no version is in the wasm graph. No native-only crate
  either.
- Remaining errors (82), all in shared code, none from dependencies:
  - `tokio::time`, `tokio::spawn`, `tokio::task::{JoinHandle, AbortHandle}`,
    `tokio::runtime::Handle`, `task_local!`: the `rt` module (step 3);
  - handler and callback storage requires `Send` (41 + 2 errors): the
    `dyn` types in `dispatch.rs` (`H<T, E>`) and `driver.rs` (token and
    state getters and their futures) say `+ Send`, though the public bounds
    are `MaybeSend`. A trait object can't name `MaybeSend` (only auto
    traits can be added), so these aliases get a definition per target
    (step 2 lists them);
  - two type-inference errors that follow from the missing `tokio::time`.

### 2. Runtime audit
Replace the sample table above with the full list of call sites. Cover:
- every `tokio::spawn`, `tokio::runtime::Handle`, task handle and how it is
  used;
- every `tokio::time` item, `std::time::Instant`, `SystemTime`;
- `Send` requirements: `select!` branches holding values across `.await`,
  futures stored in structs, trait objects containing futures, futures passed
  between tasks;
- randomness: `random_seed()` mixes `RandomState` and wall-clock time to
  seed backoff jitter, so clients restarted together don't reconnect in
  lockstep. It is not used for anything security-relevant. On
  wasm32-unknown-unknown `RandomState` is not randomly seeded, so the wasm
  build needs another source.

**Result (done):** the table under "Runtime and time usage". Beyond the
sample: the core's `Instant` API, `LastReceived`'s clock, and the `Send`-only
`dyn` aliases.

Only what this audit finds goes into `rt`. `tokio::sync` stays unchanged at
first; its implementations are available on wasm, but each composition still
has to meet the target's `Send` constraints.

### 3. Minimal `rt` module (written in-house)

First, move Android's checks into the `platform` module (decision 7), with no
change in behavior. Native and Android tests must stay green, and the Android
CI jobs must still build. Two parts:
- the `build()` check is computed from `TlsConfig::uses_platform_verifier()`
  and the init flag, not through `Net::android_tls_ready()`, because `Net`,
  the native connector, doesn't exist on wasm;
- the attempt check in `Net::connect()` calls `platform::check_attempt`.

Then the core's clock type: `centrifuge-client-core` names its `Instant`
through one alias, `std::time::Instant` on native and `web_time::Instant` on
wasm (a target-specific dependency). On native `web_time::Instant` is
`std::time::Instant` re-exported, so nothing changes there; on wasm the
client can produce one, which it can't for `std::time::Instant`.

The interface, fixed by step 2's audit. Its only additions: the core's
`Instant` above, which the watchdog and `LastReceived` (`transport/mod.rs`)
also read: they use the real clock natively, not Tokio's, and stay so.

```rust
// Tasks: an abort-only handle with `abort()` and `is_finished()` (the driver
// prunes finished callback tasks)
pub(crate) fn spawn<F>(future: F) -> AbortHandle
where
    F: Future<Output = ()> + MaybeSend + 'static;

// Monotonic time
pub(crate) struct Instant { /* tokio::time::Instant on native, web_time::Instant on wasm */ }
// To and from the core's `Instant`: `into_std()` / `from_std()` on native
// (Tokio's clock, so tests with paused time also move the core's), identity
// on wasm. Free functions: Tokio's `Instant` can't get methods.
pub(crate) fn to_core(at: Instant) -> centrifuge_client_core::time::Instant;
pub(crate) fn from_core(at: centrifuge_client_core::time::Instant) -> Instant;
pub(crate) async fn sleep_until(deadline: Instant);
pub(crate) async fn timeout<F: Future>(d: Duration, f: F) -> Result<F::Output, Elapsed>;
pub(crate) async fn timeout_at<F: Future>(deadline: Instant, f: F) -> Result<F::Output, Elapsed>;
pub(crate) struct Interval; // with tick() and missed-tick "Delay" behaviour
pub(crate) fn interval(period: Duration) -> Interval;

// Task-local marker for "this task runs a token or state callback of client
// `id`" (§9.5), replacing `tokio::task_local!` for `IN_CALLBACK`
pub(crate) fn callback_scope<F: Future>(id: u64, f: F) -> impl Future<Output = F::Output>;
pub(crate) fn in_callback(id: u64) -> bool;

// Wall-clock time (None only if the browser clock value is invalid)
pub(crate) fn system_now() -> Option<std::time::SystemTime>;

// Jitter seed (not cryptographic)
pub(crate) fn random_seed() -> u64;
```

- Native: thin wrappers over `tokio::spawn`, `tokio::time`,
  `tokio::task_local!` and `SystemTime::now()`.
- Wasm:
  - `spawn`: `wasm_bindgen_futures::spawn_local` wrapped in
    `futures_util::future::Abortable` (`futures-util` is already a
    dependency). `spawn_local` takes a `'static` future and runs it from the
    microtask queue; test both.
  - `callback_scope`: a future wrapper that sets a thread-local `Cell<u64>`
    to `id` before each poll of the inner future and restores the previous
    value after it, also when the poll panics, the same way `HandlerScope`
    does for handlers. Because the browser runs everything on one thread,
    "set during each poll" means the same as tokio's task-local. It doesn't
    leak into other tasks, which only run between polls.
    A prototype was checked three ways:
    - natively, side by side with tokio's real `task_local!` on a
      `current_thread` runtime. Three interleaved tasks (scoped, unscoped,
      differently scoped), nested scopes, and checks before and after awaits
      gave identical results.
    - built for `wasm32-unknown-unknown` with tokio limited to `sync` and
      `macros`, and run under Node: every check held, and `select!` over a
      `tokio::sync` channel worked.
    - with the restore broken on purpose, the checks failed.

    Guarantees the implementation must keep, each with a test:
    - the previous value is restored by a guard's `Drop`, not by resetting
      it by hand, so a return, a `Pending` and a panic all restore it;
    - nested scopes, including scopes for two different clients, restore
      the outer client's id when the inner poll returns;
    - an unscoped future never sees an id: it's either polled in another
      task, between polls of the scoped one, or nested inside a scoped poll,
      where seeing the outer id is the same as tokio's task-local;
    - reentrancy can't corrupt it: a callback that synchronously polls
      another scoped future (or calls `close()`) only nests a guard, and
      guards unwind in order.

    Panics abort on wasm32, so restoring after a panic only matters
    natively, where it was tested. The prototype boxes the inner future
    rather than use `unsafe` pinning (the crate forbids `unsafe`). Callbacks
    are rare, so the allocation doesn't matter.
  - timers: backed by browser scheduling (`setTimeout` via the global
    scope), with these rules:
    - every sleep is deadline-based: when the timer fires, compare
      `Instant::now()` with the deadline and reschedule if it has not
      arrived (timers can fire late in background tabs, and early is
      possible after clamping);
    - a single `setTimeout` is clamped to `2^31 - 1` ms (about 24.8 days);
      longer waits reschedule;
    - a past deadline or zero duration completes on the next poll without
      waiting for a timer;
    - dropping a sleep, timeout or interval calls `clearTimeout` and only
      then frees the JavaScript callback (`Closure`). A callback freed while
      its `setTimeout` is pending makes the browser call freed code, which
      throws from the event loop;
    - the callback reaches Rust state only through a shared waker slot
      (`Rc<RefCell<Option<Waker>>>`); the future owns the slot and the
      `Closure`. A callback that fires anyway (its `clearTimeout` raced the
      event loop) only wakes a task, which finds nothing to do;
    - rescheduling replaces the pending `setTimeout` (clear, then set); at
      most one is pending per sleep;
    - `timeout` / `timeout_at` keep tokio's order: poll the wrapped future
      first, and return `Elapsed` only if it's still pending once the
      deadline has passed. A future that completes on the poll at the
      deadline wins. Dropping the timeout drops the wrapped future and
      clears the timer;
  - `Interval` keeps tokio's semantics: the first tick completes
    immediately; later ticks follow the period; with `Delay`, a late tick
    schedules the next one a full period after it, so there is never a
    burst of catch-up ticks;
  - `Instant`: `web_time::Instant`;
  - `system_now()`: `UNIX_EPOCH + Duration::from_millis(ms)` with
    `ms = Date.now()`. This gives a real `std::time::SystemTime`, so the
    public `Statistics::connected_since` type does not change. Validate `ms`
    before converting (finite, non-negative, within `u64` range) instead of
    casting the `f64` blindly, and build the result with
    `UNIX_EPOCH.checked_add(Duration::from_millis(ms as u64))`, not `+`,
    so an unrepresentable time can't panic. On an invalid value or a failed
    `checked_add`, return `None` (native
    always returns `Some`), and each caller handles it without inventing a
    time. There are three callers today:
    - the clock-jump watchdog (`driver.rs`, its start and each check) skips
      that check, and takes the next valid reading as its new base;
    - `connected_since` (`driver.rs`) stays `None` instead of showing 1970;
    - `random_seed()` (`client.rs`) uses only the `Math.random()` part.

    Wall-clock time feeds nothing else: backoff, timeouts and pings use
    `Instant`. Step 2 confirms there are no other callers. Log the first
    `None` at `warn` through `tracing`, and later ones at `debug`, so a
    broken clock can't flood the log. The precision is milliseconds,
    coarser than native; document it on `connected_since`;
  - `random_seed()`: take two `Math.random()` values, convert each to a
    `u32` as `(x * 2^32) as u32` (`x` is in `[0, 1)`, so this is in range;
    it gives enough non-cryptographic variation for backoff jitter, and
    `Math.random()` promises nothing more), combine them into a `u64`, and
    feed that
    plus `Date.now()` into the same hasher native uses. One JavaScript
    entropy path, no `getrandom` and no `Crypto` binding; this is enough for
    jitter, which is not security-relevant. If the SDK ever needs
    cryptographic randomness on wasm, that needs its own decision.
- **Task handle: abort-only.** The driver only stores and aborts handles, so
  there is no awaitable `JoinHandle` and no completion channel. Extension
  point: if a call site ever needs a task's result, add a separate
  `rt::spawn_with_result` returning an awaitable handle (a `oneshot` from
  `tokio::sync` on both targets), and leave `spawn` abort-only.
  - `abort()` requests cancellation. The task stops the next time its future
    is polled; synchronous code that is running cannot be preempted.
  - Dropping the handle does not abort the task (tokio behavior); the driver
    aborts explicitly on shutdown.
  - Panics are fatal on wasm; no promise to propagate them.
- Reference: `n0-future` (MIT OR Apache-2.0) implements the same pattern;
  model the wasm side on its `task.rs` / `time.rs`, with attribution if code
  is copied. Not taken as a dependency: it adds 7 crates not in our tree
  (`derive_more`, `futures-buffered`, `futures-lite`, `pin-project`,
  `tokio-util`, `send_wrapper`, `cfg_aliases`), forces tokio `test-util` on
  native users, and is 0.x with a much broader surface than we need.

**Result (done).** `platform` (Android's checks moved, same errors at the
same points), the core's `time::Instant`, and `rt` with both halves; on wasm
the handler and callback storage drops `Send` (`dyn_send!`), and the event
thread and runtime handle are native-only. The client builds for
`wasm32-unknown-unknown`; until steps 6–7 a wasm client has no transport.
Deviations from the sketch above: `close()` needs no wasm signature (see
Goal), the handle also has `is_finished()`, and the `Instant` conversions
are free functions.

### 4. Verify native behavior
- Full native test suite, `cargo semver-checks`.
- Unit tests for `rt`: abort, dropped handle, timer cancellation, shutdown
  with pending timers, interval missed-tick behavior.

**Result (done).** The native suite and clippy pass; `cargo semver-checks`
against `main` finds no semver change for either crate; the real-server
suite ran 400 times (4 in parallel) with no hang or failure; `rt`'s native
wrappers have unit tests (abort, dropped handle, timeout order, interval
missed ticks, callback scope).

### 5. Browser error conversion
Done before the transports, because both use it. `JsValue` is not
`Send`/`Sync`, so it never goes into SDK errors:
1. read `name` and `message` from the JS value at the boundary; if the value
   is not an `Error` or both are empty, fall back to its `toString()`
   (truncated to a few hundred bytes);
2. store them in an owned type, e.g.
   `struct BrowserError { name: Option<String>, message: String }`;
3. drop the `JsValue`;
4. build the existing SDK error, so `BoxError` / `ArcError` stay
   `Send + Sync`.

Distinct cases: fetch rejection, HTTP status error, missing body,
stream-reader error, WebSocket construction error, WebSocket runtime error,
abort / cancellation, unsupported environment.

Aborts never need classifying from the browser's error. The SDK only aborts
an operation because the driver or the writer decided to, and that code
already knows why. It reports that reason itself, and the abort's rejection
is never turned into an error:
- `disconnect()` / `close()`: the user's request; no reconnect;
- session replacement: nothing is reported, since the old session's
  results are dropped by `TransportId`;
- a connect or request timeout: the driver's existing timeout error, as on
  native;
- a rejection the SDK didn't cause (network failure, CORS): the
  fetch-rejection error, then reconnect with backoff.

Limits, to document rather than work around:
- CORS failures reach JavaScript as generic network errors; they map to the
  fetch-rejection error, and there is no separate CORS error. The browser's
  developer tools show the detailed reason.
- WebSocket `error` events carry no reason; the close code and reason (if
  any) are all the SDK can report.

**Result (done).** `transport/browser_error.rs`: `BrowserError` (owned
`name` / `message`, each at most 300 bytes) read from a `JsValue` at the
boundary, and `failed(Failure, Option<BrowserError>) -> CloseReason` for each
case above (an unsupported environment included); the browser error becomes
the `Send + Sync` source. Everything but reading the `JsValue` builds on every
target, so host tests cover the mapping.

### 6. Browser WebSocket transport (`web-sys::WebSocket`)

The deliverable includes the first **real-browser smoke test**: headless
Chrome against a real Centrifugo, which connects, subscribes, receives a
publication and closes. Android showed that a real run settles more than
review rounds do. The emulator run confirmed the setup in one go, after
several reviews of the same code. So this test comes now, not in step 9;
step 9 widens the coverage.

- Subprotocol `centrifuge-protobuf` for protobuf (same string as
  `transport/ws.rs`); after open, check `websocket.protocol` and fail with a
  clear error if the server did not accept it.
- `binaryType = "arraybuffer"`; protobuf as binary frames, JSON as text.
- Token only in the connect command, never in the URL.
- Close codes:
  - codes the SDK passes to `WebSocket.close()`: only 1000 or 3000-4999;
  - codes received in `CloseEvent.code` can be any valid code, plus
    browser-synthesized ones that no endpoint sends in a close frame: 1005
    (no status received), 1006 (abnormal closure), 1015 (TLS failure). Map
    each to the `CloseReason` the native transport produces for the same
    situation (`transport/ws.rs`), not to `Frame { code }`:

    | `CloseEvent` | Native equivalent | `CloseReason` |
    |---|---|---|
    | a code an endpoint may send (1000-1003, 1007-1014, 3000-4999) | close frame with that code | `Frame { code, reason }` |
    | 1005 | close frame without a code | `Eof` |
    | 1006, no `error` event before it | stream ended without a close frame | `Eof` |
    | 1006 or 1015 after an `error` event | read or connect error | `Failed { .. }` |
    | 1015 without an `error` event | TLS failure | `Failed { .. }` |
    | any other code no endpoint may send (1004, 1016-2999, below 1000, above 4999) | tungstenite replaces an invalid code with 1002 "Protocol violation" | `Frame { code: 1002, reason: "Protocol violation" }` |

    Browsers fail the connection on an invalid code in a close frame, so in
    practice the last row is reported as 1006; it is there so every code
    has an answer.

    Keep the raw browser code in the `Failed` message, for diagnostics.
    The mapping is one function over every `u16`, unit-tested over the
    whole range, so no code falls through.
  - **Error-event ordering.** An `error` event is not terminal on its own:
    the transport records it as pending session state and reports nothing.
    Browsers always fire `close` after `error` (WHATWG WebSocket spec), and
    the `close` handler uses that state to classify synthetic codes per the
    table. The session reports **at most one** terminal event
    (`ctx.closed(..)`), guarded by a per-session "closed" flag, so that a
    late event, an abort and a close can't produce duplicate or
    contradictory reports.
  - **Session ownership and cleanup.** The session owns the `WebSocket` and
    its four `Closure`s, and cleans up in its `Drop`, which runs both on a
    normal end and when the driver aborts the task. In order:
    1. set the "closed" flag, so an event already queued reports nothing;
    2. clear `onopen`, `onmessage`, `onerror` and `onclose`;
    3. call `close()` (1000) if the socket is connecting or open;
    4. only then free the `Closure`s.

    A `Closure` freed while the socket can still call it makes the browser
    call freed code, which throws from the event loop. The handlers reach
    the session's state through an `Rc` they share with it, never by
    borrowing the session, so cleanup during a handler is safe. Events
    queued for a replaced session go nowhere, and anything already sent
    carries its `TransportId` and is dropped by the driver.
  - An abort before `open` (the connect timeout, `disconnect()`) runs the
    same cleanup; the browser then cancels the handshake.
- No control-frame ping/pong in browsers; rely on Centrifugo application
  pings (already in the shared logic).
- Get connect, subscribe, publish, pings, reconnect and shutdown working
  before step 7.

### 7. Browser HTTP-streaming transport (`fetch`)
- **Per-session ownership.** Each streaming session owns its
  `AbortController` and reader and reports with its own `TransportId`, as
  native transports do. Reconnecting creates a new session with a new id;
  the old session is aborted, and anything it still reports is dropped by
  the driver's id check. Aborting an old session cannot touch the new one,
  because they share no state.
- **Cancellation is cleanup on drop.** On wasm, aborting a task only drops
  its Rust future. Dropping a future that awaits a `fetch` promise does
  **not** cancel the request, so the browser would keep the HTTP connection.
  The session therefore holds a guard whose `Drop` calls
  `AbortController.abort()`, which is idempotent, so a normal end followed
  by an abort is harmless.
  - `abort()` is the whole cancellation. It is synchronous and covers both
    the request and a response body already streaming: the body stream
    errors, and a pending `read()` rejects. The SDK doesn't call
    `reader.cancel()`: it returns a promise that `Drop` can't await, and
    after `abort()` it adds nothing.
  - No promise the SDK creates is left without a rejection handler.
    `wasm_bindgen_futures::JsFuture` attaches one when it's created, also
    if the future is later dropped; any promise not wrapped in a
    `JsFuture` gets an explicit no-op `catch`. So an abort never shows up
    as an unhandled rejection.

  It runs on every path: normal end, error, abort by the driver, and the
  client being dropped. A rejection that arrives after the abort finds no
  task left to wake, or a "closed" flag already set, so it can't produce a
  second terminal event.
- **Outbound POSTs belong to their session.** The session's emulation
  writer sends them one at a time, in order, as on native, so at most one
  is in flight per session.
  - Each POST gets its own `AbortController`, created with the request and
    held by the writer's future until the response is read. It never
    outlives its POST, so there is no list of controllers to keep or
    clean up.
  - Closing the session makes the writer drop that future, and the
    controller's guard aborts the request. Its calls fail with
    `ClientDisconnected`, the same error used when a POST's outcome is
    unknown. Native does the same (#7).
  - POSTs are never retried, so a command can't be sent twice. A command
    interrupted mid-POST may or may not have reached the server, like any
    write a disconnect interrupts.
  - A POST that completes late carries the old `TransportId`, so it can't
    affect the new session.
- POST with the connect command, read `response.body.getReader()`, feed every
  chunk to the core's `StreamDecoder`; EOF calls `finish()`.
- Client-to-server messages go through the same emulation writer path as
  native (separate `fetch` POSTs), so the session and auth model is the same.
- Backpressure: read the next chunk only after the decoded replies have been
  reported (`ctx.report(...).await`), as native does; no unbounded inbound
  queue.
- Lifecycle: server-ended stream, inactivity and reconnects go through the
  shared driver, as on native.
- Reject non-2xx responses and a missing body with distinct errors.
- **CORS preflight.** Cross-origin requests trigger a preflight, because the
  request content types (`application/octet-stream` for protobuf,
  `application/json` for JSON) are not CORS-safelisted. Same-origin requests
  need no preflight. `Accept` is safelisted and needs no allow-listing. For
  cross-origin, the server must answer:
  - `OPTIONS`, with allowed method `POST` and allowed header
    `Content-Type`, plus any other non-safelisted header actually sent;
  - `Access-Control-Allow-Origin`, and `Access-Control-Allow-Credentials`
    when credentials are enabled;
  - with credentials, `Access-Control-Allow-Origin` must echo the request's
    exact `Origin`; `*` is rejected by the browser for credentialed
    responses. Cross-site cookies also need a compatible `SameSite` setting.
- Browser fetch options: offer `credentials` only. Do not expose `mode`:
  `no-cors` gives an opaque response the protocol can't read, and the other
  values add nothing for this SDK.
- Keep the WebSocket to HTTP-stream fallback logic shared.
- **Authentication differs by transport in a browser**, and the docs
  (step 10) say so:
  - a token (`token` / `get_token`) travels in the connect command, so it
    works the same over WebSocket and HTTP-streaming, same- or
    cross-origin. It's the recommended setup;
  - cookies: a browser `WebSocket` always sends the target's cookies,
    subject to their `SameSite` rules, and the SDK can't turn that off;
    `fetch` sends them only as `credentials` allows (`same-origin` by
    default). So a cookie-authenticated app that's served cross-origin
    connects over WebSocket but fails after falling back to HTTP-streaming,
    unless `credentials` is `include` and the server's CORS setup allows
    credentials (above);
  - `Origin`: the browser sets it on both and the app can't change it;
    Centrifugo checks WebSocket origins against `allowed_origins`, and
    `fetch` goes through CORS;
  - custom HTTP headers can't be set on a browser WebSocket, which is why
    `http_headers` is rejected and `emulated_headers` exists (decision 6).

### 7a. Browser lifecycle events (network, back/forward cache)

Browsers tell a page about network changes and about entering the
back/forward cache; centrifuge-js acts on both, and the browser build
does the same. Spec §13's "no network-change API" is about native targets,
where std and Tokio offer no such events; it doesn't apply here.

Same behavior as centrifuge-js (`_setNetworkEvents`, `_setPageEvents`;
the bfcache part from centrifuge-js#445):
- `offline`, while connected or connecting: close the transport and go to
  connecting with code 1 ("transport closed"), as on any transport loss.
- `online`, while connecting: reconnect at once instead of waiting out the
  backoff. If an attempt opened a transport while offline, abort it first
  (mobile Safari may never fire its close event); its late events carry
  the old `TransportId` and are dropped.
- `pagehide` with `persisted` (the page enters the cache), while connected
  or connecting: close the transport, connecting with code 1 and the reason
  "page entered back/forward cache". A frozen page can't answer pings, and
  its socket may stay open until the page is restored or evicted, so the
  server would only drop it with `3012 no pong`.
- `pageshow` with `persisted` (restored): reconnect at once. The usual
  reconnect is still scheduled: its timer doesn't run in a frozen page, and
  it reconnects a page restored without a `pageshow`.
- `persisted` false (a normal load or unload): nothing.

Where:
- The listeners go on the global scope (`js_sys::global()`): a window gets
  all four events; a Dedicated Worker gets `online` / `offline` only.
  Attached on connect and removed once the client is disconnected or
  closed, so a disconnected client holds no listener; the `Closure`s are
  freed after `removeEventListener`, as for timers and sockets.
- The core gets the two inputs the driver feeds from them: "the transport
  is gone, connecting with code 1 and this reason" and "reconnect now"
  (it already has a close in kind: `Input::Resumed` reconnects after a
  long sleep). Native never sends them.
- No option at first, and no JavaScript types in the API (decision 5), so
  no `networkEventTarget` / `pageEventTarget`. If apps need to turn it off
  (e.g. a `publish()` from the app's own `pagehide` handler, which the
  closed connection no longer carries), a plain builder `bool`, added when
  asked.

Tests (in step 9's browser suite): `offline` / `online` dispatched to the
global scope while connected, while connecting and while disconnected; a
transport opened while offline is aborted on `online`; the back/forward
cache in Chrome through Playwright (bfcache enabled; Playwright turns it
off by default), as in centrifuge-js#445: the server sees the disconnect
right after navigating away, and the client reconnects, with recovery, on
going back.

### 8. Public API and configuration (non-breaking)

The `cfg` gating needed to compile (`runtime()`, the builder's `runtime`
field, `Dispatcher::new`) landed with `rt` in step 3, since the wasm build
fails without it. On wasm, `Dispatcher::new` already refuses
`EventDelivery::Thread`; this step moves that to `platform::check_build`
and makes `Inline` the default. This step adds what is left:
validation in `build()`, the `TlsConfig` variant, and browser options.

- `ClientBuilder::runtime(tokio::runtime::Handle)`: native only
  (`cfg(not(target_arch = "wasm32"))`), the single exception to decision 6.
  The builder's `runtime` field and `Dispatcher::new`'s handle become
  native-only too; on wasm the dispatcher spawns through `rt`.
- `proxy(ProxyConfig)`, `tls(TlsConfig)`, `http_headers(HeaderMap)`: stay with
  native signatures; `build()` returns a configuration error on wasm if any
  was **explicitly** set (decision 6), through `platform::check_build`.
  - `proxy` defaults to `ProxyConfig::System`, so the builder has to record
    whether `proxy()` was called. A bare `ProxyConfig` value can't tell the
    default from an explicit `System`.
  - `danger_accept_invalid_certs` is rejected too, because a browser can't
    turn verification off.
  - `Client::set_proxy` already returns `Result<(), Error>` ("leaves room
    for validation"). On wasm it returns `Error::Configuration` for any proxy
    other than `ProxyConfig::System`, so changing the proxy at runtime can't
    be silently ignored either.
  - `TlsConfig::uses_platform_verifier()` (from #4) isn't used on wasm: the
    browser verifies, and the client never sees a rustls config.
- `EventDelivery`: `Inline` by default on wasm, and an explicit `Thread`
  rejected with a wasm-specific message (decision 8). Document the
  target-dependent default on `EventDelivery` and on
  `ClientBuilder::event_delivery`.
- `TlsConfig` is the same type on wasm, through `rustls-pki-types` (step 1);
  `build()` rejects it there anyway. `TlsConfig::rustls_client_config`
  (feature `rustls-config`) is native-only: it takes a
  `rustls::ClientConfig`.
- Extend the compile-time assertion in `client.rs` (`send_sync`) to the
  futures the SDK returns, so their native `Send` contract is tested, not
  just checked by `semver-checks`.
- The reverse check, for wasm: a compile-only test (built in the wasm
  `cargo check` job) that registers handlers capturing `Rc<RefCell<_>>` and
  awaits `close()` from a `!Send` context. It proves the `MaybeSend`
  relaxation works end to end, so an accidental `Send` requirement fails the
  build there and not later during transport work.
- `Statistics::connected_since` keeps its `Option<std::time::SystemTime>`
  type, filled from `rt::system_now()`.

### 9. Browser tests and CI
- `wasm-bindgen-test` with `wasm-pack test --headless` on Chrome and Firefox,
  against a real Centrifugo in `docker-compose`.
- Base coverage: connect, subscribe, publish, recovery, reconnect, application
  pings (server ping, client pong, ping timeout, reconnect after timeout),
  HTTP-stream fallback, JSON and protobuf, delta.
- Additional cases:

| Area | Test |
|---|---|
| Runtime | Abort a spawned task; abort a task while it awaits a timer (the timer is cleared); drop a task handle; shutdown with pending timers; `'static` futures on `spawn_local`; `callback_scope`: `in_callback(id)` true during each poll of the wrapped future, false in another task between polls, previous value restored after a poll and after a panic; nested scopes for two different clients |
| Timers | Past deadline; zero-duration sleep; duration above `2^31 - 1` ms (with a fake clock or by testing the clamping logic directly); cancellation on drop; first interval tick immediate; delayed tick without a catch-up burst; a timer firing long after its deadline, as after a background tab resumes (fake clock: the deadline re-check completes it once, without a burst); `timeout` at the boundary (the wrapped future ready on the deadline poll wins); drop a `timeout` while the wrapped future is pending (both dropped, timer cleared) |
| Time | `system_now()` conversion tested on its own, including invalid inputs (NaN, negative, too large) and an unrepresentable time (`checked_add` fails) returning `None`; each of the three callers with `None` (watchdog skips, `connected_since` stays `None`, seed still varies); `connected_since` is set; seeds differ between clients; the `Math.random()` to `u32` conversion at 0 and just below 1 |
| WebSocket | Server close with a non-1000 code; subprotocol not accepted; the close-code mapping over every `u16` (a unit test, since browsers make synthetic codes hard to trigger on demand), plus the cases that can be triggered checked in both Chrome and Firefox: a server close with a code (`Frame`), the server dropping the TCP connection without a close frame (1006: `Eof`, or `Failed` if the browser fires `error` first; both reconnect, so the test records which one each browser gives and asserts exactly one terminal event), a refused connection (`error` then 1006, `Failed`), and a close frame without a code (1005, `Eof`); the sequence `error` then `close(1006)` gives exactly one terminal event (`Failed`); `close` after an abort gives no second terminal event; abort before `open`; a late event after the session was replaced reaches nothing; handlers cleared after a normal close and after an abort (no "closure invoked after being dropped" error) |
| HTTP-stream | Non-2xx response; missing body; EOF with an incomplete frame; abort while waiting for the response headers; abort while waiting for a chunk; after each abort the request is cancelled in the browser (the server sees the connection close), not just forgotten by Rust |
| Outbound | Close a session while a POST is in flight: the POST is aborted and its `send()` fails at once with `ClientDisconnected`; an old session's POST that completes after the new session opened doesn't affect it |
| Callbacks | A callback calls `close()`; a callback's work starts another callback; a slow callback delays later events but loses or duplicates none |
| CORS | Same-origin request (no preflight); cross-origin preflight succeeds with the right server config; a CORS failure rejects the request and maps to the fetch-rejection error; credentialed requests with an explicit origin |
| Reconnect | A replaced streaming session's late chunks never reach the new session |
| Lifecycle | Drop the client while connecting; drop it while streaming |
| Protocol | Several frames in one chunk; one frame split across many chunks |
| Config | Each explicitly set unsupported option makes `build()` fail on wasm; the defaults (`ProxyConfig::System`, no TLS options) don't; an explicit `EventDelivery::Thread` fails; the default client builds and delivers events (`Inline`) |
| Errors | No `JsValue` reaches a public SDK error type |
| Environment | A missing `setTimeout`, `WebSocket` or `fetch` (deleted from the global scope in the test) makes `build()` fail with `Error::Configuration`, not a panic |

- **Dedicated Worker test:** create a real Dedicated Worker, load the wasm
  module in it, build the client there, connect to Centrifugo, subscribe and
  receive a publication. Then two endings, as separate runs:
  1. close the client inside the Worker and check the server sees a clean
     disconnect;
  2. call `worker.terminate()` with the client still running and check the
     server sees the connection drop: an eventual assertion polling the
     server with a bounded timeout (for example 10 s), since the browser
     closes the socket asynchronously. Termination runs no Rust code, so no
     `Drop` cleanup happens; the browser closes the Worker's sockets
     itself. The docs must not suggest otherwise.

  A normal `wasm-bindgen-test` run does not prove Worker support.
- CI: `cargo check --target wasm32-unknown-unknown` for core and client,
  `cargo semver-checks`, `cargo tree --target wasm32-unknown-unknown -e features`,
  a grep that fails on `spawn_blocking`, `block_in_place` and `blocking_*`,
  `wasm-pack test`, the Worker test, MSRV 1.85 over the whole resolved wasm
  graph, and binary size (release, `opt-level = "z"`, LTO, then
  `wasm-opt -Oz`).

### 10. Docs and release
- README section and a browser example.
- Table of native-only and browser-specific options, including the
  `http_headers` / `emulated_headers` difference.
- CORS notes: `allowed_origins`, preflight for cross-origin requests,
  credentials needing an explicit origin, CORS errors being opaque to the
  SDK.
- Timing notes: `connected_since` has millisecond precision on wasm; timers
  can fire late in background tabs.
- Event-loop requirement, supported environments, tested browsers.
- Callbacks run on the browser event loop and must not block (decision 8).
- CHANGELOG entry; update spec §9.11, decision 3 and the roadmap row.

## Dependencies

The initial planned direct wasm-specific dependencies are `wasm-bindgen`,
`wasm-bindgen-futures`, `js-sys`, `web-sys` and `web-time`, plus `tokio` with
`sync` and `macros` (and `rt` only if step 1 shows it is needed). `time`
stays off on wasm, so a stray tokio timer fails to compile instead of
panicking. No `futures-timer` and no `getrandom` backend.

For `setTimeout`, either works:
- a hand-written binding through the global scope, which needs no extra crate;
- `gloo-timers` 0.4, which reportedly binds `setTimeout` on the global object,
  so Workers work. Check that before choosing it; the Dedicated Worker test
  (step 9) covers it either way.

Either way, the deadline check, rescheduling and `2^31 - 1` ms clamp stay in
`rt`. The step 1 audit may change this list.

## Expected size

Measured with a scratch `cdylib` (core frame decoding only, `opt-level = "z"`,
LTO, `panic = "abort"`, stripped, no `wasm-opt`):

| Variant | Raw | Gzipped |
|---|---|---|
| Core decode only | 177 KB | 65.4 KB |
| + tokio `sync` and `select!` | 193 KB (+16 KB) | 72.0 KB (+6.6 KB) |
| + `futures-channel` and `select` | 181 KB (+3 KB) | 66.7 KB (+1.3 KB) |

Keeping tokio `sync` costs about 5 KB gzipped over a futures-only design, so it
is not a reason for a separate driver. Full client estimate (not measured):
300-500 KB raw, 100-180 KB gzipped before `wasm-opt`. Measure the real build
in step 9, then set a soft CI budget from it (for example, warn when the
gzipped size after `wasm-opt -Oz` grows more than 10% over the recorded
baseline). It should be a warning, not a failure, and the baseline is
updated deliberately.

## Order

1. Wasm compilation baseline.
2. Runtime and dependency audit.
3. `platform` module (Android's checks moved, no behavior change), then the
   minimal `rt`, including wall-clock time.
4. Native regression and semver tests.
5. Browser error conversion.
6. Browser WebSocket transport, with the first real-browser smoke test.
7. Browser HTTP-streaming transport.
7a. Browser lifecycle events: `online` / `offline`, back/forward cache.
8. Public API and configuration validation.
9. Browser integration tests, Worker test and CI.
10. Documentation and release.

## Risks

- Further compile errors after the `mio` one, from threads, `Instant`, or
  `Send` assumptions; step 2 exists to find them before refactoring.
- `rt` touches all runtime code, so native regressions are possible; step 4
  runs the full native suite before any browser work.
- Streaming `fetch` and close-code handling differ slightly across browsers.

## Future optimization

- If measurements show `url` (its `idna` Unicode tables) is a large share of
  the binary, evaluate browser-native URL parsing. This needs behavioral
  compatibility tests against the `url` crate, since parsing and
  normalization can differ.
