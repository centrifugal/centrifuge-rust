# Wasm (browser) support: implementation plan

Status: proposal. Nothing here is implemented yet.

## Goal

Run `centrifuge-client` on `wasm32-unknown-unknown` with both transports
(WebSocket and HTTP-streaming), with **no breaking change** to the native
public API (spec §9.11).

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

### Runtime and time usage found so far (a grep, not yet the full audit)

| Use | Where | Wasm note |
|---|---|---|
| `tokio::spawn` | `driver.rs` (4), `transport/ws.rs`, `transport/http_stream.rs` (2) | goes through `rt::spawn` |
| `tokio::runtime::Handle` | `ClientBuilder.runtime`, `Dispatcher::new` | does not exist on wasm; the dispatcher must spawn through `rt` |
| `JoinHandle` / `AbortHandle` | `driver.rs`: stored and only `.abort()`-ed, never awaited | an abort-only handle is enough |
| `tokio::time::{Instant, sleep_until, timeout, timeout_at, interval}` + `MissedTickBehavior::Delay` | driver, calls | `rt` must provide these, including deadline forms |
| `std::time::Instant::now()` | `driver.rs` (clock-jump watchdog) | panics on wasm |
| `SystemTime::now()` | `driver.rs` (watchdog, `connected_since`), `client.rs` (`random_seed`) | panics on wasm |
| `SystemTime` in the **public** API | `Statistics::connected_since: Option<SystemTime>` | type must stay `std::time::SystemTime` |

## Supported environments

| Environment | Initial support |
|---|---|
| Browser main thread | Supported |
| Dedicated Worker | Supported, integration tested |
| Shared Worker | Expected to work; not initially verified. Same global APIs as a Dedicated Worker, but it lives as long as any connected page does, so one client may outlive the page that created it |
| Service Worker | Not supported: the browser stops it when idle, so it can't hold a long-lived connection |
| Node.js | Not supported initially (separate decision) |
| Other JS runtimes | Not guaranteed |

- No dependency on `window`: `WebSocket`, `fetch`, `setTimeout`,
  `AbortController`, `Date` and `Math` are reached through the global scope
  (`js_sys::global()`).
- The global APIs are looked up and validated when a transport is created,
  not assumed from the target. A missing API gives a clear "unsupported
  environment" error.
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
   execution, timers and clocks come from the internal `rt` module.
3. **A small `rt` interface driven by actual call sites,** not a copy of
   tokio's API.
4. **Transport behind the existing internal trait.** The browser transports
   carry bytes only; protocol, emulation, auth and token refresh stay shared.
5. **No runtime or JavaScript types in the public API.**
6. **Unsupported options stay in the API and fail at `build()`** (spec §9.11).
   Setters such as `proxy`, `tls` and `http_headers` keep their native
   signatures on wasm, and `ClientBuilder::build()` returns a configuration
   error if one was set; options are never silently ignored. The one
   exception is `runtime(tokio::runtime::Handle)`: its parameter type does not
   exist without a Tokio runtime, so it is native-only. `emulated_headers` is
   the browser replacement for `http_headers`: those headers travel inside
   the connect command (Centrifugo header emulation), not as HTTP headers.

## Steps

### 1. Wasm compilation baseline
- Move native-only dependencies under `cfg(not(target_arch = "wasm32"))`:
  `tokio` `net` and `time`, `tokio-tungstenite`, `tokio-rustls`,
  `rustls-platform-verifier`, `hyper`, `hyper-util`, `http-body-util`, and
  `rustls` itself if `TlsConfig` can be kept without it (see step 8); gate the
  code that uses them.
- Try tokio on wasm with `sync` and `macros` only, without `rt`. The plan
  needs no tokio task or runtime types on wasm, so `rt` should be
  unnecessary; keep it only if a used symbol needs it.
- Re-run `cargo check --target wasm32-unknown-unknown -p centrifuge-client`
  and list every remaining error.
- `cargo tree --target wasm32-unknown-unknown -e features` to catch native-only
  dependencies leaking into the wasm graph.
- `getrandom`: `Cargo.lock` has 0.2.17, 0.3.4 and 0.4.3. In the client's
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

### 2. Runtime audit
Complete the table above:
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

Only what this audit finds goes into `rt`. `tokio::sync` stays unchanged at
first; its implementations are available on wasm, but each composition still
has to meet the target's `Send` constraints.

### 3. Minimal `rt` module (written in-house)
Expected interface, from the call sites found so far:

```rust
// Tasks
pub(crate) fn spawn<F>(future: F) -> AbortHandle
where
    F: Future<Output = ()> + MaybeSend + 'static;

// Monotonic time
pub(crate) struct Instant { /* tokio::time::Instant on native, web_time::Instant on wasm */ }
pub(crate) async fn sleep_until(deadline: Instant);
pub(crate) async fn timeout<F: Future>(d: Duration, f: F) -> Result<F::Output, Elapsed>;
pub(crate) async fn timeout_at<F: Future>(deadline: Instant, f: F) -> Result<F::Output, Elapsed>;
pub(crate) struct Interval; // with tick() and missed-tick "Delay" behaviour

// Wall-clock time (None only if the browser clock value is invalid)
pub(crate) fn system_now() -> Option<std::time::SystemTime>;

// Jitter seed (not cryptographic)
pub(crate) fn random_seed() -> u64;
```

- Native: thin wrappers over `tokio::spawn`, `tokio::time` and
  `SystemTime::now()`.
- Wasm:
  - `spawn`: `wasm_bindgen_futures::spawn_local` wrapped in
    `futures_util::future::Abortable` (`futures-util` is already a
    dependency). `spawn_local` takes a `'static` future and runs it from the
    microtask queue; test both.
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
    - dropping a sleep, timeout or interval calls `clearTimeout`;
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
    time: `connected_since` stays `None` instead of showing 1970, and the
    clock-jump watchdog skips that check. Log it at `warn` through
    `tracing`. The precision is milliseconds, coarser than native; document
    it on `connected_since`;
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

### 4. Verify native behavior
- Full native test suite, `cargo semver-checks`.
- Unit tests for `rt`: abort, dropped handle, timer cancellation, shutdown
  with pending timers, interval missed-tick behavior.

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

Limits, to document rather than work around:
- CORS failures reach JavaScript as generic network errors; they map to the
  fetch-rejection error, and there is no separate CORS error. The browser's
  developer tools show the detailed reason.
- WebSocket `error` events carry no reason; the close code and reason (if
  any) are all the SDK can report.

### 6. Browser WebSocket transport (`web-sys::WebSocket`)
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
    | a real code (1000, 3000-4999, other valid codes) | close frame with that code | `Frame { code, reason }` |
    | 1005 | close frame without a code | `Eof` |
    | 1006, no `error` event before it | stream ended without a close frame | `Eof` |
    | 1006 or 1015 after an `error` event | read or connect error | `Failed { .. }` |

    Keep the raw browser code in the `Failed` message, for diagnostics.
  - **Error-event ordering.** An `error` event is not terminal on its own:
    the transport records it as pending session state and reports nothing.
    Browsers always fire `close` after `error` (WHATWG WebSocket spec), and
    the `close` handler uses that state to classify synthetic codes per the
    table. The session reports **at most one** terminal event
    (`ctx.closed(..)`), guarded by a per-session "closed" flag, so that a
    late event, an abort and a close can't produce duplicate or
    contradictory reports.
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

### 8. Public API and configuration (non-breaking)

The `cfg` gating needed to compile (`runtime()`, the builder's `runtime`
field, `Dispatcher::new`, the `close()` signatures) lands with `rt` in step 3,
since the wasm build fails without it. This step adds what is left:
validation in `build()`, the `TlsConfig` variant, and browser options.

- `ClientBuilder::runtime(tokio::runtime::Handle)`: native only
  (`cfg(not(target_arch = "wasm32"))`), the single exception to decision 6.
  The builder's `runtime` field and `Dispatcher::new`'s handle become
  native-only too; on wasm the dispatcher spawns through `rt`.
- `proxy(ProxyConfig)`, `tls(TlsConfig)`, `http_headers(HeaderMap)`: stay with
  native signatures; `build()` returns a configuration error on wasm if any
  was set. `TlsConfig` stores `rustls` types internally. On wasm, prefer an
  empty `cfg` variant: the type and its constructors keep their signatures,
  but store only "TLS was configured", since `build()` rejects it anyway.
  Keep `rustls-pki-types` only if a constructor's signature needs one of its
  types.
  `TlsConfig::rustls_client_config` (feature `rustls-config`) is native-only:
  it takes a `rustls::ClientConfig`.
- `close()`: two `cfg`-gated signatures. Native stays exactly
  `impl Future<Output = ()> + Send + 'static`; wasm drops `Send`. Do not
  switch to a boxed named future: that would change the API and add an
  allocation.
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
| Runtime | Abort a spawned task; drop a task handle; shutdown with pending timers; `'static` futures on `spawn_local` |
| Timers | Past deadline; zero-duration sleep; duration above `2^31 - 1` ms (with a fake clock or by testing the clamping logic directly); cancellation on drop; first interval tick immediate; delayed tick without a catch-up burst |
| Time | `system_now()` conversion tested on its own, including invalid inputs (NaN, negative, too large) and an unrepresentable time (`checked_add` fails) returning `None`; `connected_since` is set; seeds differ between clients; the `Math.random()` to `u32` conversion at 0 and just below 1 |
| WebSocket | Server close with a non-1000 code; subprotocol not accepted; close-event mapping for 1005, 1006 with and without a prior `error` event, and 1015 (unit-test the mapping function, since browsers make these hard to trigger on demand); the sequence `error` then `close(1006)` gives exactly one terminal event (`Failed`); `close` after an abort gives no second terminal event |
| HTTP-stream | Non-2xx response; missing body; EOF with an incomplete frame; abort while waiting for a chunk |
| CORS | Same-origin request (no preflight); cross-origin preflight succeeds with the right server config; a CORS failure rejects the request and maps to the fetch-rejection error; credentialed requests with an explicit origin |
| Reconnect | A replaced streaming session's late chunks never reach the new session |
| Lifecycle | Drop the client while connecting; drop it while streaming |
| Protocol | Several frames in one chunk; one frame split across many chunks |
| Config | Each unsupported option makes `build()` fail on wasm |
| Errors | No `JsValue` reaches a public SDK error type |
| Environment | Unsupported environment gives a clear error |

- **Dedicated Worker test:** create a real Dedicated Worker, load the wasm
  module in it, build the client there, connect to Centrifugo, subscribe and
  receive a publication, then terminate the Worker and check the server sees
  the disconnect. A normal `wasm-bindgen-test` run does not prove Worker
  support.
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
- CHANGELOG entry; update spec §9.11, decision 3 and the roadmap row.

## Dependencies

The initial planned direct wasm-specific dependencies are `wasm-bindgen`,
`wasm-bindgen-futures`, `js-sys`, `web-sys` and `web-time`, plus `tokio` with
`sync` and `macros` (and `rt` only if step 1 shows it is needed). `time`
stays off on wasm, so a stray tokio timer fails to compile instead of
panicking. No `gloo-*`, `futures-timer` or `getrandom` backend. The step 1
audit may change this list.

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
3. Minimal `rt`, including wall-clock time.
4. Native regression and semver tests.
5. Browser error conversion.
6. Browser WebSocket transport.
7. Browser HTTP-streaming transport.
8. Public API and configuration validation.
9. Browser integration tests, Worker test and CI.
10. Documentation and release.

## Risks

- Further compile errors after the `mio` one, from threads, `Instant`, or
  `Send` assumptions; step 2 exists to find them before refactoring.
- `rt` touches all runtime code, so native regressions are possible; step 4
  runs the full native suite before any browser work.
- Keeping `TlsConfig` available on wasm may pull in more of `rustls` than
  wanted; step 1 decides.
- Streaming `fetch` and close-code handling differ slightly across browsers.

## Future optimization

- If measurements show `url` (its `idna` Unicode tables) is a large share of
  the binary, evaluate browser-native URL parsing. This needs behavioral
  compatibility tests against the `url` crate, since parsing and
  normalization can differ.
