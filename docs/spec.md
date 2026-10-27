# centrifuge-rust: specification

Status: draft, 2026-10-26

This document specifies `centrifuge-rust`, a Rust client SDK for Centrifugo and the Centrifuge library, using the bidirectional protocol. It covers scope, public API, protocol behaviour, internal architecture and how we verify correctness. It is the reference for the implementation and for code review: when behaviour here differs from the code, one of them is a bug.

## 0. Sources of truth

In order of precedence:

1. **Protocol schema:** `centrifugal/protocol/definitions/client.proto`. It defines field names, numbers and types for both encodings.
2. **SDK behaviour spec:** centrifugal.dev `docs/transports/client_api.md` (states, options, methods, codes), `client_protocol.md` (framing) and `docs/server/codes.md` (error, unsubscribe and disconnect codes).
3. **Reference implementations:** `centrifuge-go`, which this SDK follows most closely (a server-side language with real concurrency, and recently hardened), and `centrifuge-js`, the most complete feature set.
4. **SDK TLA+ model:** `tla/sdk` in the Centrifugal Labs internal repository (`SDK.tla`, README "Decisions"). It is the formal contract for steps, events and their order, and the core implements its steps (§9.2).
5. **Bug catalogue:** `sdk_validation/SKILL.md`, "Catalogue of bug patterns". Every pattern that applies to a WebSocket client with a reconnect state machine is a requirement of this spec (§12).

Prior art: the community crate `IntrepidAI/tokio-centrifuge`. This SDK is an official implementation that follows the spec above. It does not depend on that crate.

## 1. Goals and non-goals

### Goals

- Behave the same as the other official SDKs. The same server input should produce the same sequence of states, events and errors, so the feature matrix on centrifugal.dev can list Rust.
- **Behaviour parity with centrifuge-js comes first.** Where JS behaviour and Rust idiom disagree, the behaviour stays as in centrifuge-js: non-waiting `connect`/`subscribe` with `ready()`, calls waiting for the connection, reconnect and resubscribe semantics. It fits long-lived reconnecting connections and enables batching. Rust idiom applies only where it changes no behaviour: types, error enums, builders, docs. Any intentional behaviour difference is listed in §1.1 with its reason.
- An idiomatic async Rust API on Tokio, within that limit: `Send + Sync` cheap-to-clone handles, `async fn` for request/reply calls, typed errors, no panics in library code.
- Correct by construction under concurrency: a sans-IO state machine owned by one driver task, implementing the SDK TLA+ model step by step (§9). This rules out most of the lock-ordering and re-entrancy bug classes from the catalogue, and calling the SDK from handlers can't deadlock.
- Both wire formats: JSON and Protobuf.
- Usable from services, CLIs, bots, load-testing tools and desktop apps.
- **Primary first use case: desktop apps built with Tauri** (§13). Users asked to move the connection out of the webview for two reasons. First, closing the window destroys the webview, and with it centrifuge-js, while the app keeps running in the system tray and must keep receiving messages. Second, corporate networks need proxy settings, which the browser WebSocket API can't express. So phase 1 must cover running headless for the whole process lifetime, proxies (§5.2), and corporate TLS (custom or system root CAs).

### 1.1 Intentional differences from centrifuge-js

| Behaviour | centrifuge-js | This SDK | Why |
|---|---|---|---|
| A call made inside an event handler (`unsubscribe()`, `disconnect()`, ...) | Runs nested at once: its events fire inside the call | `state()` changes at once, as in JS (§9.2), but the op is applied and its events delivered right after the handler returns, before anything else is processed | Steps in the core are atomic, and handlers can run on another thread (§9.4). Nested execution would need the "emit, then re-check" pattern behind catalogue patterns 7, 21 and 46. Both orders are allowed by the TLA+ model ("the application may act between any two steps"). |
| Reconnect backoff reset | On the connect reply, except over emulation | After the first server ping or command reply, on every transport | A failure that always follows the connect would otherwise reconnect at the minimum delay forever (centrifuge-python F4, pattern 67) (§6.2) |
| `refresh` push (server API `refresh`) | Ignored | Re-arms or cancels the client's refresh timer, only in client-side refresh mode | The client's schedule follows the server's new expiry (§6.6) |
| After falling back to HTTP-streaming (phase 2) | Stays on the fallback for good | Returns to WebSocket on the app's next `connect()` after `disconnect()`, or after 1 h | Long-lived processes move between networks (§5.8) |

### Non-goals (for now)

- Unidirectional transports (SSE, HTTP-streaming, gRPC, uni-WebSocket). They need no SDK.
- SSE with bidirectional emulation, WebTransport and SockJS. HTTP-streaming with emulation **is** in scope, as the fallback transport (§5.7).
- `wasm32-unknown-unknown` / browser target. The transport is behind a trait (§9.6) so it can be added later.
- Server API client (HTTP/gRPC server API). That belongs in a separate crate.

## 2. Feature scope and phases

| Feature | Phase |
|---|---|
| WebSocket transport (ws/wss, custom HTTP headers, TLS config) | 1 |
| Headers emulation (`ConnectRequest.headers`, Centrifugo v6+) next to real HTTP headers, both updatable for the next attempt (§5.1) | 1 |
| HTTP-streaming transport with bidirectional emulation (`/connection/http_stream` + `/emulation`) | 1 |
| Transport fallback: an ordered list of transports, e.g. WebSocket then HTTP-streaming (like centrifuge-js and centrifuge-csharp, §5.8) | 1 |
| Proxies: explicit HTTP(S) CONNECT and SOCKS5, with auth; `HTTPS_PROXY`/`ALL_PROXY`/`NO_PROXY` env vars (§5.2) | 1 |
| System root certificates (OS trust store) and extra custom CAs | 1 |
| OS system proxy settings (macOS, Windows), via `hyper-util`'s matcher (§3.2) | 1 |
| PAC files / WPAD | 3 (no maintained Rust PAC evaluator; revisit on demand) |
| Fallback reset: return to the first transport on an app `connect()` after `disconnect()`, or after 1 h on the fallback (§5.8) | 2 |
| JSON and Protobuf protocols | 1 |
| Connect, disconnect, close, client states and events | 1 |
| Automatic reconnect with jittered exponential backoff | 1 |
| Command/reply with timeouts, async pushes | 1 |
| Server ping / client pong, no-ping detection | 1 |
| Connection token, `get_token`, connection token refresh | 1 |
| Connect `data` | 1 |
| Disconnect advice from the server (reconnect vs terminal) | 1 |
| Client-side subscriptions: states, events, registry | 1 |
| Automatic resubscribe with backoff | 1 |
| Subscription token, `get_token`, subscription token refresh | 1 |
| Recovery (`recoverable`, `positioned`, `since`), join/leave | 1 |
| Publish, history, presence, presence stats, RPC, send | 1 |
| Async server messages (`message` push) | 1 |
| Server-side subscriptions | 1 |
| State invalidation (unsubscribe 2502, disconnect 3014) | 1 |
| Channel compaction (Centrifugo PRO) | 1 |
| Fossil delta compression | 1 |
| Publication tags filter | 1 |
| `get_state` for stream subscriptions (reject unrecovered, error 112) | 1 |
| `set_data()` on the client and on subscriptions: replaces the connect / subscribe `data` for the next attempt (as centrifuge-js `setData`; the `getData` callback is deprecated and not offered) | 1 |
| Stream-based event API adapters (`impl Stream`, §9.4) | 1 |
| Blocking facade for non-async apps (`blocking` feature, §9.5) | 2 |
| Map subscriptions, shared poll | 3 (needs a separate spec addendum) |
| Automatic command batching: commands queued together go out in one frame or emulation request (§9.10) | 1 |
| `client.statistics()`: connection and traffic counters (§4.3) | 1 |
| Manual batching API (`start_batching`/`stop_batching` as in centrifuge-js), client publish debouncing, optimistic subscriptions | 3 |
| wasm32 browser transport | 3 |

- **Phase 1** (releasable `0.1`) has every feature that all official SDKs share in the centrifugal.dev feature matrix (`client_sdk.md`), plus what the first users need: the HTTP-streaming fallback, proxies, OS trust store, and `Stream` adapters.
- **Phase 2** adds features no other SDK has as a whole, or only centrifuge-js has: the blocking facade, model-based tests, and the fallback reset rule (§5.8).
- **Phase 3** is driven by demand.

Left out of phase 1 on purpose, with the matrix's own rows:
- Manual batching API, client publish debouncing, optimistic subscriptions, map subscriptions, shared poll: only centrifuge-js has them.

## 3. Crate layout and dependencies

### 3.1 Crate basics

- **Name: `centrifuge-client`**, with `[lib] name = "centrifuge"`, so code says `use centrifuge::Client`. The repository stays `centrifuge-rust`, matching the org's `centrifuge-<lang>` repos.
- **Two crates in one workspace**, as `quinn` / `quinn-proto` do:
  - `centrifuge-client-core`: the sans-IO state machine and codecs, with no IO and no runtime.
  - `centrifuge-client`: the driver, transports and public API.

  Both are published, because crates.io needs every dependency published, and both are released **in lockstep** with one version (one `[workspace.package]` version, and the client pins the core exactly with `=`). Users depend only on `centrifuge-client`, which re-exports the core's data types. The core's other items carry no stability promise, and the client's semver check covers what it re-exports. The split keeps the core provably free of IO dependencies (CI builds it for wasm32), and lets the model-based tests (§11.1) target it directly.

  What users see is unchanged:
  - one dependency to add
  - types documented under `centrifuge::` (`#[doc(inline)]` re-exports)
  - no runtime cost; the few hot codec helpers get `#[inline]`

  Compiler errors may name `centrifuge_client_core::...` paths, as `quinn_proto` ones do for quinn users.

  **Rules the split imposes on the implementation:**
  - Inherent methods on re-exported types, e.g. `Publication::json::<T>()`, must be defined in `centrifuge-client-core`. The client can't add methods to another crate's types. So the core has a `serde` feature, and the client's `serde` feature enables it (`serde = ["centrifuge-client-core/serde"]`). The same goes for trait impls on core types that the orphan rule would block in the client.
  - Every client feature that touches core types forwards to the matching core feature.
  - Nothing in the public client API names a core item that isn't re-exported.
- **About the name:** it has no `-rs`/`-rust` suffix, per the Rust API Guidelines ("Every crate is Rust!"). `centrifuge` itself is a third-party `0.0.0` placeholder (`richard-w`, Feb 2025), so we don't use it. Reserve `centrifuge-client` on crates.io before the first release (it was free on 2026-10-26).
- **Edition 2024, MSRV 1.85.** That's the floor already set by the core dependencies (`tokio` 1.54, `hyper-util` 0.1.21, `tokio-tungstenite` 0.30 and `rustls-platform-verifier` 0.7 all declare `rust-version = 1.85`). CI checks it with `cargo +1.85 check --all-features`. Raising the MSRV is a minor-version change, noted in the changelog.
- Lints live in `[workspace.lints]`: `unsafe_code = "forbid"`, `missing_docs` and `clippy::pedantic` at `warn` (with documented allows). CI builds with `-D warnings`, so warnings fail CI but don't break users' builds on newer toolchains.
- **License:** MIT, the same as the other Centrifugal SDKs. `cargo deny` keeps every dependency permissive (C-PERMISSIVE).

### 3.2 Technology choices

Researched 2026-10-26. Versions are the latest stable releases on crates.io that day.

| Concern | Choice | Why | Rejected |
|---|---|---|---|
| Async runtime | `tokio` 1.x (`rt`, `sync`, `time`, `net`, `macros`) | The de facto runtime. Tauri's `async_runtime` is Tokio, and hyper and the WebSocket crates are built on it. | Runtime-agnostic designs: more code for no current user. A wasm build will need its own transport and timers anyway (§9.11). |
| Concurrency model | A sans-IO `Core` state machine, owned by one driver task per client and fed through `tokio::sync::mpsc`. Handlers run on a dispatcher. Stateless calls take a fast path through a shared call table (§9). | This is the owner-task ("actor") pattern (Alice Ryhl, "Actors with Tokio": one task owns the state, it stops when every handle is dropped, oneshot replies can't deadlock) combined with sans-IO (`quinn-proto`, `rustls`), so the state machine is testable without a runtime. It also matches the single-loop TLA+ model, and centrifuge-csharp's single-loop refactor (#33) shows the payoff. | `Arc<Mutex<State>>` shared across tasks, which is how centrifuge-go works and the source of most of its race fixes. |
| WebSocket | `tokio-tungstenite` 0.30, **no TLS features**, used via `client_async_with_config` over a stream we build ourselves | The most used and actively maintained crate (~99M downloads in 90 days). It runs over any `AsyncRead + AsyncWrite`, so proxy and TLS stay in our connector. Its throughput gap to the fastest crates doesn't matter for a client. | `tokio-websockets` 0.13: good, but needs MSRV 1.89 and is less widely used. `fastwebsockets`: server-focused. |
| HTTP (stream + emulation) | `hyper` 1.12 (`client`, `http1`) over the SDK's own connector | Low-level control over a long-lived streaming body, abortable requests, and per-request deadlines (§5.7). HTTP/1.1 only (§15 #25). The connector is shared with WebSocket. | `reqwest` 0.13: easy, but its proxy logic sits inside its own client and can't serve the WebSocket path. With it, proxy behaviour could differ between the two transports. |
| Proxy | Hand-written HTTP CONNECT (Basic auth) and SOCKS5 (RFC 1928/1929, local or remote DNS) handshakes; `hyper-util` `proxy::matcher::Matcher` for the decision (`from_env`, `from_system` = env + macOS/Windows system settings) | One proxy stack for both transports. The handshakes are small and fully tested; the matcher, maintained by the hyper team, gives curl-compatible env semantics and reads OS system proxies without extra crates. | `hyper-util` `Tunnel`/`SocksV5`: tied to hyper's connector service stack, awkward under the WebSocket path. |
| TLS | `rustls` 0.23 via `tokio-rustls` 0.26. Verification with `rustls-platform-verifier` 0.7. Crypto provider `aws-lc-rs` (rustls default), `ring` as an alternative feature. | The platform verifier uses the OS trust store **and its trust decisions** (macOS Security framework, Windows CryptoAPI, Android). That's what corporate TLS-inspecting proxies need, and the rustls team recommends it over `rustls-native-certs`. On Linux it falls back to the system bundle (honours `SSL_CERT_FILE`). We add `webpki-roots` as extra roots there for minimal containers. `aws-lc-rs` needs only a C compiler for non-FIPS builds, which desktop toolchains already have. `ring` is there for exotic cross-compiles. | `native-tls`: a different behaviour per OS, and another feature combination to test. Revisit if users ask. `rustls-native-certs` alone: no OS trust decisions. |
| Protobuf | `prost` 0.14. Code generated by `prost-build` from `client.proto` and **committed**, regenerated by `cargo xtask proto` and checked in CI | The ecosystem standard. Committing the code means users don't need `protoc`. | Google's `protobuf` v4 (upb-based) isn't stable on crates.io yet (latest stable is 3.7). |
| JSON | `serde` + `serde_json` with `raw_value`. Hand-written wire structs mirroring the proto field names. | `bytes` fields must be embedded raw JSON (§5.4), which proto3-JSON generators (`pbjson`) don't do. `RawValue` avoids re-parsing payloads. | `pbjson`, `sonic-rs` (no benefit at client message rates). |
| Payloads | `bytes` 1.x | Cheap clones of payloads between the codec, events and the app. A stable 1.x public dependency. | |
| Fossil delta | Our own port of `centrifugal/fdelta` (~300 lines), with its test vectors and a fuzz target | The `fossil-delta` crate hasn't been updated since 2023, and a broken delta silently corrupts data. | `fossil-delta` crate |
| Errors | Hand-written public enums (§4.7). `thiserror` 2 for internal derive convenience only. | C-GOOD-ERR: matchable, `Send + Sync + 'static`, `source()` chains. | `anyhow` in the public API (what `tokio-centrifuge` does): callers can't match on errors. |
| Logging | `tracing` 0.1 | What hyper and Tokio use. Apps get spans for free. | `log`: no spans. |
| Jitter | `fastrand` 2 | Tiny, no OS entropy needed. | `rand`: heavy for one call. |
| Futures glue | `futures-util` 0.3 (`Stream`, `SinkExt`, `FutureExt::catch_unwind`) | | |
| Tests | `tokio` `test-util` (`time::pause`), an in-process fake server (hyper server + tungstenite), `turmoil` 0.7 for seeded network-fault simulation, `cargo-fuzz`, `proptest` for codecs | `turmoil` is the Tokio project's deterministic simulator. It gives the seeded chaos runs (§11.3) reproducible timelines. | `madsim`: intercepts libc, heavier than we need. |
| Release hygiene | `cargo-semver-checks`, `cargo-hack --feature-powerset`, `cargo-deny`, `cargo-msrv` | Catches API breaks and feature-combination build failures before publishing. | |

**Public dependency policy (C-STABLE).** Only types from stable crates appear in the public API: `bytes`, `http` (1.x, for `HeaderMap`), `url` (2.x) and `tokio` (for `runtime::Handle`). `hyper`, `hyper-util`, `tokio-tungstenite` and `prost` are private implementation details. `rustls` is pre-1.0, so `TlsConfig::rustls_client_config(rustls::ClientConfig)` is the only place it appears. That method is behind the `rustls-config` feature and documented as exempt from the crate's semver guarantees.

**Comparison with `tokio-centrifuge` 0.2.6** (community crate, last release 2025-09-29, ~2.8k lines):
- What it has: WebSocket via `tokio-tungstenite` with native-tls by default, JSON and Protobuf, reconnect with a backoff strategy, client-side subscriptions, publish, RPC, and callbacks with `anyhow::Error`.
- What it lacks: `get_token` and token refresh, recovery (epoch/offset), history, presence, server-side subscriptions, proxies, HTTP-streaming fallback and delta. Its public API also exposes `anyhow`.
- So this SDK is a new implementation, not a fork.

### 3.3 API design guidelines applied

From the Rust API Guidelines checklist (rust-lang.github.io/api-guidelines), the items that shape this SDK:

- **C-BUILDER, C-STRUCT-PRIVATE.** `ClientBuilder` and `SubscriptionBuilder` with private fields, so options can be added in minor releases. §4.2 lists options with defaults. They are builder methods, not public struct fields.
- **C-SEND-SYNC.** All handles are `Send + Sync`, and all futures returned by the SDK are `Send`, on native targets (the bounds are `MaybeSend`/`MaybeSync`, §9.11). A compile test asserts this.
- **C-COMMON-TRAITS, C-DEBUG.** Events and results derive `Debug`, `Clone` and (where meaningful) `PartialEq`/`Eq`. `Debug` for `Client` and `Subscription` prints channel and state, and never tokens.
- **C-GOOD-ERR.** See §4.7. `#[non_exhaustive]` on every public enum, and on every public struct that may gain fields: events, results (`HistoryResult`, `PresenceResult`, `RpcResult`), `Publication`, `ClientInfo`, `Statistics`. Types apps construct themselves get constructors, because `#[non_exhaustive]` blocks struct literals outside the crate: `StreamPosition::new(offset, epoch)`, returned from `get_state` and passed to `since`.
- **C-VALIDATE.** Configuration is checked when it's built (§4.1).
- **C-DTOR-BLOCK.** `Drop` never blocks. `close().await` is the graceful alternative.
- **C-CRATE-DOC, C-EXAMPLE, C-QUESTION-MARK.** Every public item has a rustdoc example. Examples use `?`, not `unwrap`.
- **C-FEATURE.** Feature names are descriptive (`http-stream`, `delta`, `aws-lc-rs`, `ring`), with no `use-` or `with-` placeholders.
- **Cancel safety.** Every async method documents what happens if its future is dropped (§4.3).

Cargo features:

| Feature | Default | Effect |
|---|---|---|
| `json` | yes | JSON protocol codec. Protobuf, the default protocol, is always built in: its types are the core's wire model (there's no `protobuf` feature). |
| `http-stream` | yes | HTTP-streaming transport and fallback (pulls in the `hyper` HTTP/1.1 client) |
| `system-proxy` | yes | OS proxy settings via `hyper-util/client-proxy-system` (the matcher; the CONNECT and SOCKS5 handshakes are the SDK's own) |
| `aws-lc-rs` | yes | rustls crypto provider |
| `ring` | no | Alternative rustls crypto provider. With both enabled, `aws-lc-rs` is used. With neither, `compile_error!`. |
| `delta` | yes | Fossil delta decoding |
| `rustls-config` | no | Accept a raw `rustls::ClientConfig` (semver-exempt) |
| `serde` | yes | Typed JSON payload helpers (`publish_json`, `Publication::json`), §4.6 |
| `test-util` | no | Public constructors for events and results (`#[non_exhaustive]` blocks struct literals), so apps can unit-test their handlers |

The `compile_error!` combination (no crypto provider) is excluded from CI's feature-powerset check with `cargo hack --at-least-one-of aws-lc-rs,ring`.

Workspace layout (internal, not a public contract):

```
Cargo.toml                    workspace: shared version, MSRV, lints
crates/centrifuge-client-core/       sans-IO core (§9.2). No IO, no runtime, no clock; builds for wasm32
  src/proto/                  generated Protobuf types
  src/codec/                  JSON and Protobuf codecs, frame splitting (pure functions)
  src/delta.rs                fossil delta apply
  src/machine/                state machine: client, subscriptions, server subs, flows, deadlines
  src/types.rs                Publication, ClientInfo, StreamPosition, events, codes (re-exported)
  src/filter.rs               tags filter builder (re-exported)
crates/centrifuge-fake-server/ in-process fake Centrifugo for tests and the validation harness (publish = false, §11.2)
crates/centrifuge-client/     the SDK users depend on (lib name `centrifuge`)
  src/client.rs               Client handle, ClientBuilder, public methods
  src/subscription.rs         Subscription handle, SubscriptionBuilder
  src/error.rs                Error, ErrorKind, ServerError
  src/driver.rs               the driver task: select loop, IO, snapshots, callback tasks
  src/dispatch.rs             event dispatcher (thread / inline), WouldDeadlock markers
  src/calls.rs                call fast path and call table
  src/transport/              Transport trait, WebSocket and HTTP-streaming, shared connector
                              (proxy, TLS), fallback selection, emulation writer
  src/maybe_send.rs           MaybeSend / MaybeSync (§9.11)
```

## 4. Public API

The whole public surface is listed on one page in [`api.md`](api.md), for comparing the implementation against. Behaviour is defined here; when they disagree, this spec wins and `api.md` gets fixed. Code here is normative for names and semantics. Exact signatures (e.g. `impl Into<Bytes>` vs `Bytes`) may change during implementation if the result reads better.

### 4.1 Client construction

```rust
use centrifuge::{Client, Protocol};

let client = Client::builder("ws://localhost:8000/connection/websocket")
    .protocol(Protocol::Protobuf) // the default; Protocol::Json is the alternative
    .token("JWT")
    .build()?; // Err only on configuration errors (bad URL scheme, empty endpoint list, ...).
```

With fallback (§5.8), transports are tried in the given order:

```rust
use centrifuge::{Client, TransportEndpoint};

let client = Client::builder_with_transports([
        TransportEndpoint::websocket("wss://example.com/connection/websocket"),
        TransportEndpoint::http_stream("https://example.com/connection/http_stream"),
    ])
    .build()?;
```

`Client::builder(url)` with an `http://`/`https://` URL uses HTTP-streaming alone. With a `ws://`/`wss://` URL it uses WebSocket alone. A `TransportEndpoint` whose URL scheme doesn't match its transport is rejected at construction, and so is an empty list.

`build()` validates the configuration **before** anything starts (pattern 74). It spawns the driver on the runtime given with `.runtime(handle)`, or else on the current Tokio runtime. With neither available, it returns `Error::Configuration`. An explicit handle matters for Tauri, where setup hooks may run outside a runtime context (`tauri::async_runtime::handle()`). No connection is opened until `connect()`.

`Client` is `Clone + Send + Sync`. Clones share one connection. Handlers receive the handle as an argument (§9.4). `WeakClient` (from `client.downgrade()`) is for other long-lived places that must not keep the client alive (§9.8).

### 4.2 Options

Options are set through `ClientBuilder` methods with the same names (`.timeout(d)`, `.get_token(f)`, ...). The fields are private (C-STRUCT-PRIVATE, §3.3). The listing below is the logical option set with defaults, not a public struct.

```rust
struct ClientOptions {
    runtime: Option<tokio::runtime::Handle>,      // default: Handle::try_current() at build(); none → Error::Configuration
    protocol: Protocol,                 // Protobuf (default) | Json
    token: Option<String>,              // initial connection token
    get_token: Option<TokenGetter>,     // fetch or refresh connection token
    data: Option<Bytes>,                // connect data
    name: String,                       // default "rust", max 16 chars
    version: String,                    // app version, max 64 chars
    http_headers: http::HeaderMap,      // real HTTP headers: WebSocket upgrade, HTTP-stream request,
                                        // every emulation request (server: `http_headers`)
    emulated_headers: HashMap<String, String>, // sent inside the connect command (server: `emulated_headers`)
    emulation_endpoint: Option<Url>,    // default: "/emulation" on the HTTP-stream endpoint's origin
    timeout: Duration,                  // command timeout, default 5s
    handshake_timeout: Duration,        // TCP + TLS + upgrade, default 5s
    min_reconnect_delay: Duration,      // default 500ms
    max_reconnect_delay: Duration,      // default 20s
    max_server_ping_delay: Duration,    // default 10s
    tls: Option<TlsConfig>,             // custom TLS connector config
    proxy: ProxyConfig,                 // default ProxyConfig::System, see §5.2
    max_message_size: usize,            // read limit, default 64 MiB: large enough for big history and
                                        // recovery replies; apps expecting small messages lower it
}
```

Rules:
- Options that need a disabled Cargo feature are a configuration error at `build()`: an `http(s)://` endpoint without `http-stream`, `delta: Some(Fossil)` without `delta`, `Protocol::Json` without `json`.
- A duration of zero is rejected as a configuration error rather than meaning "no timeout" or "0 ms" (pattern 63).
- `name` and `version` longer than their limits are rejected at construction.
- If `token` is empty and `get_token` is set, the SDK fetches a token before the first connect (`client_api.md`, "Client connection token").

Callback types:

```rust
// MaybeSendBoxFuture<'a, T>: a boxed future that is Send on native targets (§9.11).
// Ok(None) = no token: anonymous on connect, unauthorized on refresh. Ok(Some("")) is treated as Ok(None).
pub type TokenGetter = Arc<dyn Fn(ConnectionTokenContext) -> MaybeSendBoxFuture<'static, Result<Option<String>, TokenError>> + MaybeSend + MaybeSync>;
// An empty string is treated as TokenError::Unauthorized (logged at warn).
pub type SubTokenGetter = Arc<dyn Fn(SubscriptionTokenContext) -> MaybeSendBoxFuture<'static, Result<String, TokenError>> + MaybeSend + MaybeSync>;
pub type StateGetter = Arc<dyn Fn(SubscriptionGetStateContext) -> MaybeSendBoxFuture<'static, Result<StreamPosition, BoxError>> + MaybeSend + MaybeSync>;

#[derive(Debug)]
#[non_exhaustive]
pub enum TokenError {
    /// Stop: the user isn't allowed. Client -> disconnected(unauthorized),
    /// subscription -> unsubscribed(unauthorized).
    Unauthorized,
    /// Any other failure: retried with backoff.
    Other(BoxError),
}

// `?` works inside getters: any standard error becomes `Other` (retry).
// Same shape as anyhow::Error's From impl, which compiles because the target
// type doesn't implement std::error::Error itself.
impl<E: std::error::Error + Send + Sync + 'static> From<E> for TokenError {
    fn from(e: E) -> Self { TokenError::Other(Box::new(e)) }
}
```

`TokenError` deliberately does **not** implement `std::error::Error`. It's an instruction returned to the SDK (stop or retry), not an error apps propagate. Implementing `Error` would make the blanket `From` above overlap with the standard `impl<T> From<T> for T`, so `?` wouldn't work. Errors that aren't `std::error::Error` (a `String`, say) use `TokenError::Other(e.into())` explicitly. The SDK reports a failed getter through the `error` event, with the cause as `source()`. A typical getter:

```rust
.get_token(|_ctx| async move {
    let token = my_backend::connection_token().await?;   // network error → retry later
    if token.is_empty() { return Err(TokenError::Unauthorized); }
    Ok(Some(token))
})
```

`get_state` returns `Result<StreamPosition, BoxError>`, so `?` already works there.
```

The builder method `ClientBuilder::get_token(|ctx| async move { ... })` box the closures, so users don't write `Box::pin` themselves.

### 4.3 Client methods

| Method | Kind | Semantics |
|---|---|---|
| `connect(&self) -> Result<(), Error>` | sync, non-blocking | `disconnected` → `connecting`. A no-op when connecting or connected. `Err(ClientClosed)` after close. |
| `disconnect(&self)` | sync, non-blocking | → `disconnected` with code 0 "disconnect called". Subscriptions → `subscribing` and keep their positions. |
| `close(&self) -> impl Future<Output = ()> + 'static` | starts sync, waits async | → `closed`. All subscriptions → `unsubscribed` (code 2 "client closed"). The driver stops. Resolves once the connection is closed and queued events are delivered. Calling it from an event handler doesn't wait for itself (pattern 43). |
| `state(&self) -> State` | sync | Snapshot of the current state. |
| `ready(&self, timeout: Option<Duration>) -> Result<(), Error>` | async | Resolves when connected. Fails at once with `ClientDisconnected` / `ClientClosed` in those states (pattern 75). The waiter is removed when it times out or its future is dropped (pattern 62). |
| `set_token(&self, token)` | sync | Replaces the connection token used on the next connect (pattern 58 rules, §6.6). |
| `set_http_headers(&self, HeaderMap) -> Result<(), Error>` / `set_emulated_headers(&self, map) -> Result<(), Error>` | sync | Replace the headers used from the next connect attempt (§5.1). Managed headers are rejected with `Error::Configuration`. |
| `set_proxy(&self, ProxyConfig) -> Result<(), Error>` | sync | Replaces the proxy from the next connect attempt (§5.2). |
| `downgrade(&self) -> WeakClient` | sync | A handle that doesn't keep the client alive. `WeakClient::upgrade() -> Option<Client>`. |
| `set_data(&self, data)` | sync | Replaces the connect `data` sent with the next connect attempt (not the current connection), as centrifuge-js `setData`. In the JSON protocol it's validated like any payload (§4.6). |
| `rpc(&self, method, data) -> Result<RpcResult, Error>` | async | |
| `send(&self, data) -> Result<(), Error>` | async | Fire-and-forget `send` command. Resolves once written. |
| `publish(&self, channel, data)` | async | Any channel the server allows, with no client-side subscription needed (as centrifuge-js `client.publish`). That makes it the way to publish into server-side subscriptions. |
| `history(&self, channel, HistoryOptions)` | async | Any channel the server allows, no subscription needed. |
| `presence(&self, channel)` / `presence_stats(&self, channel)` | async | The same. |
| `new_subscription(&self, channel) -> SubscriptionBuilder`, then `.build() -> Result<Subscription, Error>` | sync | `Err(DuplicateSubscription)` if the channel is registered. |
| `get_subscription(&self, channel) -> Option<Subscription>` | sync | |
| `remove_subscription(&self, &Subscription) -> Result<(), Error>` | sync | `Ok` also when it's not registered (a no-op); `Err(ClientClosed)` after close. Unsubscribes, then removes. A no-op if the registered entry for the channel is a different object (pattern 59). The removed handle becomes unusable at once (pattern 60). |
| `subscriptions(&self) -> HashMap<String, Subscription>` | sync | A copy (pattern 61). |
| `statistics(&self) -> Statistics` | sync | A snapshot of counters for status displays, logs and support tickets (below). Cheap: atomics updated by the reader, writer and driver, never a round trip. |

**Why `connect()`, `disconnect()`, `subscribe()` and `unsubscribe()` don't wait.** They only change the desired state. The SDK then works out the commands, and that is what lets it batch (§9.10). An app sets up its subscriptions, calls `subscribe()` on each and then `connect()`, and every subscribe goes out in one frame right after the connect reply. Awaiting each call would serialize them: one round trip per channel. Code that needs to know when the client is connected awaits `ready()` once at the end. A `connect_and_wait()` is deliberately not offered, because it would encourage the serialized pattern.

```rust
for ch in ["news", "chat:42", "alerts"] {
    client.new_subscription(ch).build()?.subscribe(); // queued, nothing sent yet
}
client.connect()?;                                    // connect, then all three subscribes in one frame
client.ready(None).await?;                            // optional: wait once
```

Request/reply methods called while not connected wait for `connected` up to `timeout`. That includes `disconnected`: they wait for the next connect, as centrifuge-js `_methodCall` does and as the TLA+ decision says. A later entry into `disconnected` (including another `disconnect()`) fails them with `ClientDisconnected`. After `close()` they fail with `ClientClosed`. `ready()` is different: it fails at once in `disconnected` (pattern 75). The mechanics are in §9.3, and what can be called from handlers is in §9.5.

**Cancel safety:**
- `close()` starts closing when it's **called**, not when its future is first polled. The future only waits for the close to finish, so dropping it, or never awaiting it, still closes the client.
- Dropping a `ready()` future removes its waiter, with no other effect.
- Dropping a request future before it completes removes its entry from the request table. A late reply to it is ignored. A dropped `publish` may still have been delivered; this is documented on the method.

`Statistics` (`#[non_exhaustive]`, all counters since the client was built):

```rust
pub struct Statistics {
    pub state: State,
    pub transport: Option<TransportKind>,      // current transport, if connected
    pub connected_since: Option<SystemTime>,
    pub connects: u64,                         // successful connects, including reconnects
    pub reconnect_attempts: u64,               // attempts that didn't end in a connect
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub messages_in: u64,                      // decoded replies and pushes
    pub messages_out: u64,                     // commands written
    pub publications_delivered: u64,           // deliveries committed (§9.4): counts with no handler too,
                                               // never a publication whose handler panicked
    pub subscriptions: usize,                  // registry size
}
```

### 4.4 Client events

Handlers are registered with `client.on_connecting(|client, e| ...)`, etc., or on the builder (`Client::builder(url).on_connected(...)`, `client.new_subscription(ch).on_publication(...)`), so they're in place before the first event. There's one handler per event kind; registering again replaces it. Names follow centrifuge-js. The client-level `on_subscribed`, `on_subscribing`, `on_unsubscribed`, `on_publication`, `on_join` and `on_leave` are for **server-side** subscriptions, and their docs say so first. Handlers are `FnMut(&Client, Event) + MaybeSend + 'static` (subscription handlers get `&Subscription`; §9.11). They run one at a time on the client's dispatcher (§9.4).

| Event | Fields |
|---|---|
| `connecting` | `code: u32`, `reason: String` |
| `connected` | `client_id: String`, `version: String`, `data: Option<Bytes>`, `transport: TransportKind` |
| `disconnected` | `code: u32`, `reason: String` |
| `error` | `error: ClientError` (§4.7) |
| `message` | `data: Bytes` |
| `subscribed` (server-side) | `channel`, `was_recovering`, `recovered`, `recoverable`, `positioned`, `stream_position: Option<StreamPosition>`, `data` |
| `subscribing` (server-side) | `channel` |
| `unsubscribed` (server-side) | `channel` |
| `publication` (server-side) | `channel`, `Publication` |
| `join` / `leave` (server-side) | `channel`, `ClientInfo` |

Each event is a `#[non_exhaustive]` struct named after it: `ConnectingEvent`, `ConnectedEvent`, `DisconnectedEvent`, `ErrorEvent` (holds a `ClientError`), `MessageEvent`, and for server-side subscriptions `ServerSubscribedEvent`, `ServerSubscribingEvent`, `ServerUnsubscribedEvent`, `ServerPublicationEvent`, `ServerJoinEvent`, `ServerLeaveEvent`. The handler names stay JS-style (`on_subscribed`, ...). Only the type names say "server".

`client.events()`, `sub.events()` and `sub.publications()` return `impl Stream` adapters on top of the same dispatcher (phase 1). Callbacks remain the primary API, because only they give the "no step runs while you handle the event" guarantee (§9.4).

### 4.5 Subscription

```rust
let sub = client
    .new_subscription("chat:index")
    .recoverable(true)
    .join_leave(true)
    .build()?; // Err(DuplicateSubscription) or a configuration error
sub.on_publication(|_sub, e| println!("{:?}", e.data));
sub.subscribe();
sub.ready(Some(Duration::from_secs(5))).await?; // as centrifuge-js ready(timeout?)
sub.publish(Bytes::from_static(br#"{"text":"hi"}"#)).await?;
```

```rust
struct SubscriptionOptions {             // set via SubscriptionBuilder methods
    token: Option<String>,
    get_token: Option<SubTokenGetter>,
    data: Option<Bytes>,
    positioned: bool,
    recoverable: bool,
    join_leave: bool,
    since: Option<StreamPosition>,          // recover from a known position on first subscribe
    delta: Option<DeltaType>,               // Some(DeltaType::Fossil)
    tags_filter: Option<FilterNode>,        // can't be combined with delta
    get_state: Option<StateGetter>,
    min_resubscribe_delay: Duration,        // default 500ms
    max_resubscribe_delay: Duration,        // default 20s
}
```

`delta` together with `tags_filter` is rejected by `build()` as a configuration error.

| Method | Kind | Semantics |
|---|---|---|
| `channel(&self) -> &str` | sync | |
| `state(&self) -> SubState` | sync | `Unsubscribed` / `Subscribing` / `Subscribed` |
| `subscribe(&self)` | sync | `unsubscribed` → `subscribing`. A no-op when subscribing or subscribed. Works before `connect()`. |
| `unsubscribe(&self)` | sync | → `unsubscribed` (code 0 "unsubscribe called"). The position is kept for a later `subscribe()`. |
| `ready(&self, timeout: Option<Duration>) -> Result<(), Error>` | async | Resolves when subscribed, after the `subscribed` handler returned (§9.4). Fails at once with `SubscriptionUnsubscribed` when unsubscribed. While subscribing it waits, also while the client is disconnected or connecting. It fails when `unsubscribes` changes (§9.3), or with `ClientClosed`. |
| `publish`, `history`, `presence`, `presence_stats` | async | Wait for `subscribed` up to `timeout`. Fail with `SubscriptionUnsubscribed` when unsubscribed. |
| `set_data(&self, data)` | sync | Replaces the subscribe `data` sent with the next subscribe attempt, including automatic resubscribes. |
| `set_tags_filter(&self, Option<FilterNode>) -> Result<(), Error>` | sync | Only while unsubscribed, and not with `delta` set; otherwise `Err(Error::Configuration)`. |

Subscription events: `subscribing {code, reason}`, `subscribed {was_recovering, recovered, recoverable, positioned, stream_position, data}`, `unsubscribed {code, reason}`, `error {SubscriptionError}` (kinds: `Subscribe`, `Token`, `GetState`, `Refresh`, `Configuration`; §4.7), `publication`, `join`, `leave`. They're the structs `SubscribingEvent`, `SubscribedEvent`, `UnsubscribedEvent`, `SubscriptionErrorEvent` (holds a `SubscriptionError`), `PublicationEvent` (derefs to `Publication`), `JoinEvent` and `LeaveEvent` (deref to `ClientInfo`).

### 4.6 Data types

```rust
pub struct Publication {
    pub data: Bytes,                     // JSON protocol: the raw JSON bytes
    pub offset: u64,                     // 0 = not positioned
    pub tags: HashMap<String, String>,
    pub info: Option<ClientInfo>,
    pub channel: Option<String>,         // set for wildcard-style deliveries
    pub time: Option<i64>,               // ms since epoch, when sent
}
pub struct ClientInfo { pub user: String, pub client: String, pub conn_info: Option<Bytes>, pub chan_info: Option<Bytes> }
pub struct StreamPosition { pub offset: u64, pub epoch: String }
// Built with HistoryOptions::new().limit(10) / .all() / .position_only(), .since(pos), .reverse(true).
// Wire mapping (client.proto): position_only → limit 0, all → limit -1, limit(n) → n. Default: position_only, as centrifuge-js history().
pub struct HistoryOptions { /* private: limit, since, reverse */ }
pub struct HistoryResult { pub publications: Vec<Publication>, pub offset: u64, pub epoch: String }
pub struct PresenceResult { pub clients: HashMap<String, ClientInfo> }
pub struct PresenceStatsResult { pub num_clients: u32, pub num_users: u32 }
pub struct RpcResult { pub data: Bytes }
```

**Payload type: `bytes::Bytes` everywhere, in both protocols.** This covers publication `data`, publish, RPC request and result, `send`, connect and subscribe `data`, `message` pushes, `conn_info` and `chan_info`. Methods take `impl Into<Bytes>`, so `Vec<u8>`, `String`, `&'static [u8]` and `&'static str` all work. Switching the protocol changes no app code. The crate re-exports the type as `centrifuge::Bytes`, so apps don't need a `bytes` dependency to name it.

| | Protobuf (default) | JSON |
|---|---|---|
| What a payload may contain | any bytes: JSON, Protobuf, MessagePack, raw binary | valid JSON only |
| What the app receives | the bytes exactly as published | the JSON value's bytes exactly as they appeared in the frame (raw JSON, not re-serialized, not base64) |
| Invalid payload on send | — | `Error::InvalidData`, checked with `serde_json::value::RawValue` before sending, instead of a frame the server would answer with disconnect 3501 |
| Delta compression | applied by the SDK; the app gets full bytes | the same (the JSON string wrapping of deltas, §7.5, is handled inside) |
| Copies on receive | zero-copy: prost decodes `bytes` fields as `Bytes` slices of the frame buffer (`prost-build` `.bytes(["."])`) | zero-copy: a borrowed `&RawValue` is turned into a `Bytes` slice of the frame buffer |

Interop: JSON and Protobuf clients can share a channel as long as the payloads are JSON. A non-JSON payload published by a Protobuf client can't be represented to JSON clients (Centrifugo FAQ, "both binary and JSON clients in one channel"). This is documented next to `Protocol`.

**Typed helpers** (feature `serde`, phase 1). These are conveniences on top of `Bytes`, the same in both protocols. Most apps send JSON with serde, so the README and examples show these first and raw `Bytes` second:
- `publish_json(&T)`, `rpc_json::<Req, Resp>(method, &req)`
- `Publication::json::<T>() -> Result<T, serde_json::Error>`
- the same for `RpcResult` and `MessageEvent`

Other field types: offsets and versions are `u64`, `time` is `Option<i64>` (ms; `None` when the server didn't set it), `tags` is `HashMap<String, String>`, and `epoch` and ids are `String`. They're identical in both protocols. The JSON codec accepts 64-bit numbers as a JSON number or a string (§5.4).

### 4.7 Errors

```rust
#[non_exhaustive]
pub enum Error {
    Timeout,
    ClientDisconnected,
    ClientClosed,
    SubscriptionUnsubscribed,
    DuplicateSubscription,
    Server(ServerError),          // { code: u32, message: String, temporary: bool }
    InvalidData(String),          // payload invalid for the chosen protocol
    Transport { status: Option<u16>, message: String }, // e.g. an emulation request rejected with 413
    Configuration(String),
    WouldDeadlock,                // §9.5
}
```

Background failures that don't belong to a call go to the `error` events:

```rust
pub struct ClientError { pub kind: ClientErrorKind, pub source: Option<BoxError> }   // client `error` event
#[non_exhaustive]
pub enum ClientErrorKind {
    Transport { transport: TransportKind, status: Option<u16> }, // open/read/write failures, proxy errors (407 too)
    ConnectToken,          // get_token failed before a connect (§6.2)
    Connect,               // connect error reply
    Refresh,               // connection refresh (§6.6)
    Configuration,         // e.g. expired token and no get_token, proxy auth missing
    Protocol,              // undecodable data after the first message, message size limit (§5.3)
    Delta { channel: String, offset: u64 },  // §7.5
    HandlerPanicked,       // §9.4
}
pub struct SubscriptionError { pub kind: SubscriptionErrorKind, pub source: Option<BoxError> }
#[non_exhaustive]
pub enum SubscriptionErrorKind { Subscribe, Token, GetState, Refresh, Configuration }
```

Errors implement `std::error::Error` with `source()`. `TokenError` is the one exception, because it's a signal returned *to* the SDK (§4.2). Every public enum is `#[non_exhaustive]`.

## 5. Wire protocol

### 5.1 Transport

- Two transports: WebSocket (`ws://`, `wss://`) and HTTP-streaming with emulation (`http://`, `https://`, §5.7). Other schemes are rejected at construction. The rest of this section is about WebSocket.
- Protobuf: request the `centrifuge-protobuf` subprotocol. Frames are binary.
- JSON: no subprotocol. Frames are text.
- **Two kinds of headers.** They're named after the server options that forward them to the connect proxy, because Centrifugo keeps them apart: "the `http_headers` list never forwards emulated values". No option is called just `headers`, so the two can't be confused. In the Rust API, `headers` appears only as the wire field of `ConnectRequest`, which `emulated_headers` fills.
  - `http_headers`: real HTTP headers on the WebSocket upgrade, the HTTP-stream request and every emulation request. They're also visible to anything in front of Centrifugo (an API gateway checking `Authorization`, a load balancer routing on a header, a WAF). Forwarded by the server via `http_headers`.
  - `emulated_headers`: the `headers` map of the connect command (Centrifugo v6+ headers emulation), the same as centrifuge-js and centrifuge-csharp `headers`. Forwarded via `emulated_headers`. Use it to keep the server config that a centrifuge-js setup already has (the Tauri migration case, §13), or when some clients of the same server are browsers.
  - Both are set in the builder and can be replaced for the **next** connect attempt with `set_http_headers()` / `set_emulated_headers()` (centrifuge-js `setHeaders`), e.g. to put a refreshed bearer token in place before a reconnect.
  - Headers the transport manages are rejected at build time as a configuration error: `Host`, `Connection`, `Upgrade`, `Sec-WebSocket-*`, `Content-Length`, `Transfer-Encoding`.
  - For a native client both kinds are client-controlled. The server docs' advice to treat emulated headers as untrusted applies to real ones from a native client too.
  - Cookies are the app's: a native client has no browser cookie jar, and a Tauri webview's cookies aren't visible to the Rust side. Put a session cookie in `http_headers` (`Cookie`) or use `emulated_headers`.
- The whole handshake (DNS, TCP, TLS, upgrade) runs under `handshake_timeout`. On timeout, or on `disconnect()`/`close()` during the handshake, the in-progress connect future is dropped, which aborts the socket (patterns 34, 51, 52).
- **Closing never waits for the peer.** A teardown changes state and schedules the reconnect at once. The old socket gets a close frame (1000 on `disconnect()`/`close()`) in the background, after the write in progress, within 1 s, and is then dropped. During a handshake it's dropped at once. A dead link or a full send buffer can't hold the next attempt (centrifuge-java F3, centrifuge-python F5, centrifuge-csharp B7).
- **A WebSocket write error** first lets the reader finish what's already buffered, so a server close code that explains it (e.g. 3501) wins. Then it closes the transport with `connecting(1)` (centrifuge-csharp A14).
- The SDK never closes or reconfigures app-provided resources (e.g. a passed-in TLS config or connector; pattern 53).

### 5.2 Proxies and corporate TLS (phase 1)

```rust
#[non_exhaustive]
pub enum ProxyConfig {
    None,                                   // direct connection
    FromEnv,                                // HTTPS_PROXY / HTTP_PROXY / ALL_PROXY, NO_PROXY only
    System,                                 // default: env vars, then OS settings (macOS System Configuration,
                                            // Windows registry) via hyper-util Matcher::from_system; no PAC
    #[non_exhaustive] Http { url: Url, auth: Option<ProxyAuth> },   // ProxyConfig::http(url, auth): CONNECT tunnel
    #[non_exhaustive] Socks5 { url: Url, auth: Option<ProxyAuth>, remote_dns: bool },  // ProxyConfig::socks5(..)
    Custom(CustomProxy),                    // ProxyConfig::custom(|url| -> ProxyTarget): app decides per endpoint
}
pub struct ProxyAuth { pub username: String, pub password: String }  // Basic for HTTP, user/pass for SOCKS5; Debug prints the password as "***"
#[non_exhaustive]
pub enum ProxyTarget { Direct, Http { .. }, Socks5 { .. } }  // ProxyTarget::http(url, auth), ProxyTarget::socks5(url, auth, remote_dns)
```

- The tunnel is set up **before** TLS to the Centrifugo host, so `wss://` through a proxy validates the Centrifugo certificate, not the proxy's. For an `https://` proxy, TLS to the proxy comes first.
- A CONNECT response other than 2xx is a transport error (retried with backoff), reported with the proxy status in the `error` event (`ClientErrorKind::Transport { status: Some(407), .. }` for missing or wrong credentials). The client keeps retrying, because the app may update the config with `set_proxy()`.
- The whole proxy handshake counts toward `handshake_timeout`.
- Plain `ws://`/`http://` endpoints are tunnelled with CONNECT too (to port 80), never sent as absolute-URI proxy requests, so both transports share one path. Proxies that allow CONNECT only to TLS ports (Squid's default) refuse it; the docs say to use `wss://`/`https://` there.
- `NO_PROXY` matching follows curl rules (host suffixes, `*`, IPs; CIDR in phase 2).
- Proxy credentials are never logged.
- **Proxy auth is Basic (HTTP) and username/password (SOCKS5) only.** NTLM and Kerberos (Negotiate) aren't supported. The documented workaround for NTLM-only corporate proxies is a local helper such as Px or Cntlm, used as a plain HTTP proxy. Native NTLM/Kerberos is added only if users confirm they need it. The users who asked for proxy support are being asked.
- Changing the proxy at runtime: `client.set_proxy(ProxyConfig)` takes effect on the next connect attempt (phase 1). The app can call `disconnect()` + `connect()` to apply it at once.
- TLS trust: by default the OS verifier via `rustls-platform-verifier`, which uses the OS store and its trust settings. On Linux it uses the system bundle (`SSL_CERT_FILE` honoured); no roots are bundled (§15 #26). Corporate TLS-inspecting proxies install their CA in the OS store, so the OS store must be used (§3.2). `TlsConfig` can add PEM CAs (`Verifier::new_with_extra_roots`), skip verification for development only (`danger_accept_invalid_certs`, which logs a warning on every connect), or take a full `rustls::ClientConfig` behind the `rustls-config` feature.
- The proxy and TLS stack is one connector: DNS and TCP (each address in turn, with a share of the handshake timeout), the CONNECT or SOCKS5 handshake, then `tokio-rustls`. Both transports use it, so proxy and TLS behaviour can't differ between WebSocket and HTTP-streaming.

### 5.3 Framing

- **JSON:** one frame carries one or more messages separated by `\n`. When decoding, split on `\n`, skip empty lines, decode each line. When encoding a batch, join with `\n` and add no trailing newline.
- **Protobuf:** each message is prefixed with its length as a varint, and messages are concatenated. A length above `max_message_size` follows the size-limit rule below (pattern 72), and a truncated frame is undecodable data (§5.3, before/after the first message). WebSocket frames are complete, so no cross-frame buffering is needed.
- A frame that fails to decode ends the transport, and **no message after it in the same frame is dispatched** (patterns 73, 81). What happens next depends on whether the connection was proven:
  - **Before the first decoded message** (no connect reply yet), it's a **transport failure**: reconnect with backoff, counted as an unproven transport for fallback (§5.8). A captive portal, a misrouted proxy or a TLS-inspecting box answering with HTML must never stop a desktop app for good (centrifuge-dart #160; centrifuge-js treats a stream decode error as a transport close).
  - **After it**, the server is known to speak the protocol, so undecodable data is `disconnected(2, "bad protocol")` with no reconnect (`centrifuge-go`).
- A JSON line longer than `max_message_size` without a `\n` is a decode error, so a body without newlines can't buffer without limit (pattern 72).
- **Size limits:** tungstenite's `max_message_size` and `max_frame_size` are both set from `max_message_size`. An incoming message, or a length prefix or JSON line, over it:
  - **before the first decoded message** is undecodable data, i.e. a transport failure. An HTML page read as a varint gives a huge "length" (pattern 72).
  - **after it** is `disconnected(3, "message size limit")` with an `error` event, the same as a server close 1009. It must never be a silent reconnect loop (centrifuge-swift F14, centrifuge-python F4).

### 5.4 Encoding rules (JSON)

- Field names are exactly the proto field names (`snake_case`: `presence_stats`, `sub_refresh`, `join_leave`, `was_recovering`). The server decodes strictly since Centrifugo v6.10 (exact case, no duplicate keys, valid UTF-8, nothing after the object).
- Omit fields that have default values (proto3 JSON semantics). Never send `null`.
- `bytes` fields (`data`, `info`, `conn_info`, `chan_info`) are embedded raw JSON, not base64. They are located as a borrowed `&RawValue` within the frame buffer, and handed to the app as a `Bytes` slice of that buffer: their exact bytes, with no copy.
- 64-bit integers: encode as JSON numbers. Decode leniently from a number or a string.
- An empty command `{}` is the pong.

The Protobuf encoding uses prost-generated types. An empty `Command` (zero-length body) is the pong.

### 5.5 Command ids

- `id: u32` increments per client, starting at 1. It skips 0 on wraparound and skips ids still in the request table.
- `send` and pong carry no id.
- A reply with `id > 0` is matched against the request table. Unknown ids are ignored (the request timed out or was cancelled).
- A reply with `id == 0` and no `push` is a server ping.
- A reply with `id == 0` and a `push` is an async push.

### 5.6 Malformed replies

A reply whose result is missing a field the SDK would dereference ends the connection as "bad protocol" and fails the call with `ClientDisconnected`. Examples: a successful connect reply without `connect`, a subscribe reply without `subscribe`, or a `None` publication in a list (`centrifuge-go` `malformedReply`). Rust types are `Option<T>` for messages, so this check is explicit in the codec layer.

### 5.7 HTTP-streaming with bidirectional emulation

Wire-compatible with `centrifuge-js` (`transport_http_stream.ts`) and `centrifuge-csharp` (`HttpStreamTransport.cs`). The server needs `http_stream.enabled: true`. HTTP-streaming is one-way (server → client). Commands from the client go as separate HTTP requests to the emulation endpoint, and Centrifugo routes each one to the node that holds the stream (`session` + `node`).

**Opening the stream**

- `POST {http_stream endpoint}` with the **connect command as the body**. A connection over this transport starts with connect, so the stream request carries it, and the transport counts as open once response headers arrive.
  - JSON: `Content-Type: application/json`, `Accept: application/json`. The body is the encoded command line.
  - Protobuf: `Content-Type: application/octet-stream`, `Accept: application/octet-stream`. The body is the varint-length-delimited `Command`.
  - `http_headers` are added (the emulated ones travel in the connect command in the body). Proxy and TLS come from §5.2, through the same connector as WebSocket.
- Opening (connect, TLS, response headers) is bounded by `handshake_timeout`. A non-2xx response is a transport error (retried, and counted as an unproven transport for fallback, §5.8).
- The stream body has the same framing as §5.3, but as a **byte stream**: messages can be split across reads anywhere, including inside a varint length prefix.
  - JSON: newline-delimited replies. Blank lines are skipped (keep-alive filler from intermediaries).
  - Protobuf: varint-length-delimited `Reply` messages.
  - The decoder keeps a growable buffer. It scans only newly appended data, compacts consumed bytes, and releases a buffer that grew for one large reply (pattern 71). A length prefix above `max_message_size` follows the size-limit rule (§5.3, pattern 72). A partial message at EOF is a transport error, not a decode panic (pattern 24).
  - Only decoded messages re-arm the no-ping timer (pattern 72).
- EOF or a read error closes the transport with `connecting(1, "transport closed")`. Messages already decoded from that read are dispatched **before** the close is handled, so a final `disconnect` push and its terminal code are honoured (pattern 18). A non-protocol body (a captive portal answering 200 with HTML), or a response whose `Content-Type` isn't the expected one, is a transport failure before the first decoded message (§5.3), and never a panic (pattern 25).

**Sending commands (emulation)**

- `POST {emulation_endpoint}` with an `EmulationRequest { session, node, data }`. `session` and `node` come from the connect reply. They are stored **before** the `connected` event, so commands sent from its handler reach the right node.
  - JSON: `Content-Type: application/json`, body `{"session":"..","node":"..","data":"<encoded commands as a JSON string>"}`. `data` is a JSON string holding the newline-joined commands, as centrifuge-js sends it. A golden test against a real server pins this down.
  - Protobuf: `Content-Type: application/octet-stream`, body the encoded `EmulationRequest` (`data` = varint-delimited commands).
- Commands go **one request at a time, in order**: the next request is sent only after the previous one was answered (`centrifuge-csharp` `TransportWriter`). Commands queued meanwhile are batched into the next request. Pongs go the same way.
- Each emulation request has a deadline of `timeout`, with the late-timer grace of §9.7. When it expires, the transport closes, because the outcome of its commands is unknown. Without the deadline, one request hung in an intermediary would block every later command and pong (pattern 83).
- Response handling:
  - 2xx: delivered. A pending `send()` completes.
  - 404 (the server has no node for the session, e.g. it restarted), 401, 403, a 3xx redirect, ≥ 500, or a network error: close the transport so the client reconnects, instead of waiting for command timeouts (pattern 27).
  - Other statuses (e.g. 413 body too large): fail only the commands in that request with a write error.
  - A session gone from a running node answers 204. Its stream ends, and that path handles it.
- Closing the transport aborts the stream request **and** every emulation request in flight. No request outlives `disconnect()`/`close()`, so a connection pool can't fill with abandoned requests (pattern 26).
- Emulation requests run on a separate pooled connection from the stream (HTTP/1.1 keep-alive, or a multiplexed HTTP/2 connection when ALPN gives `h2`).
- Headers emulation works over HTTP-streaming as over WebSocket: `emulated_headers` go in the connect command that opens the stream (§5.1).

**Backoff over emulation.** A connect reply over HTTP-streaming proves only the stream, not the emulation path. The reconnect backoff is reset only after the server replied to a command sent through emulation, or sent a ping, over this connection. Otherwise a broken emulation endpoint (wrong path, unrouted proxy) would reconnect at the minimum delay forever (pattern 67).

### 5.8 Transport fallback

The rules match centrifuge-js and centrifuge-csharp, so the behaviour is the same across SDKs:

- Transports are tried in the order given. The client keeps `current_index`, `transport_was_open` and `tried_all_transports`.
- **A transport is "proven"** (`transport_was_open = true`) once it has shown that it reaches the server:
  - a WebSocket that completed its upgrade (a slow first connect reply must not push the client onto the fallback for good). Undecodable data before the first decoded message overrides this: it marks the transport unproven again and advances the fallback (§5.3). A TLS-inspecting box that completes the upgrade and then sends garbage must not pin the client to a broken WebSocket. A slow reply isn't garbage, so the pattern 66 trade-off is unchanged.
  - any reply from the server over the transport, including an error reply
  - a close with a server code (≥ 3000)
  - for HTTP-streaming, the first decoded message (a connect reply or a push)
- **A failure of an unproven transport advances to the next one.** That covers a failed or timed-out open, a close with a transport-level code (< 3000), undecodable data before the first message (§5.3), and failed creation (patterns 4, 5). Each failed transport advances **exactly once**, even when an open error and a `Closed` both arrive for it (the generic form of pattern 12). After the last transport, it wraps to the first and sets `tried_all_transports`.
- **The first round is immediate.** Until `tried_all_transports`, a reconnect after an unproven failure uses delay 0, and doesn't advance the backoff counter. This covers only transport failures. Any error reply or server disconnect code proves the transport, and is then retried with backoff (TLA+ "an error is never retried without the delay"). The round is bounded by the length of the list. After that, the normal backoff applies.
- **A proven transport is kept** for the client's lifetime, and later failures retry it with backoff. This is the released behaviour of both reference SDKs, with the pattern 66 trade-off: a socket that upgrades and then never works doesn't fall back.
- **Fallback reset (phase 2).** A client running on a transport other than the first one goes back to the start of the list, which reruns the immediate first round:
  - on the app's next `connect()` after its own `disconnect()` (how an app signals "the network changed", §13), or
  - on any reconnect that starts more than 1 hour after the fallback transport was chosen.

  Within a network it stays on the fallback, so a network where WebSocket hangs never pays the handshake timeout on every reconnect. This differs from centrifuge-js on purpose: desktop and service processes live for days and move between networks, where browser tabs don't (§1.1). Phase 1 keeps the JS behaviour.
- A temporary connect error or a reconnect disconnect over a transport that reached the server proves it. The client retries that transport after the backoff instead of rotating (pattern 66: no zero-delay loop against a connect proxy).
- The state machine, codes and events don't depend on the transport. The `error` event of a transport failure says which transport failed (`ClientErrorKind::Transport { transport: TransportKind, .. }`, §4.7). `connected` carries `transport: TransportKind` for diagnostics.
- With a single endpoint, the fallback rules are off and normal backoff applies from the first failure.

## 6. Connection behaviour

### 6.1 States

`disconnected` (initial) → `connecting` → `connected`; `closed` is terminal.

| From | Trigger | To | Event |
|---|---|---|---|
| disconnected | `connect()` | connecting | `connecting(0, "connect called")` |
| connecting | connect reply OK | connected | `connected` |
| connecting | transient failure (transport, timeout, temporary error, 109 with `get_token`) | connecting (retry after backoff) | `error` only |
| connecting | 109 with a static token and no `get_token` | disconnected | `error(Configuration)`, `disconnected(1, "unauthorized")` (§6.6) |
| connecting | terminal failure (non-temporary server error, terminal disconnect code, `TokenError::Unauthorized`) | disconnected | `disconnected(code, reason)` |
| connected | transport closed / reconnect advice / no ping / subscribe timeout / unsubscribe error | connecting | `connecting(code, reason)` |
| connected | terminal advice / refresh unauthorized / bad protocol | disconnected | `disconnected(code, reason)` |
| connecting, connected | `disconnect()` | disconnected | `disconnected(0, "disconnect called")` |
| disconnected | `disconnect()` | disconnected | none: it only fails calls waiting for a connect (§9.3) |
| any | `close()` | closed | `unsubscribed(2)` for each subscription, then `disconnected(0, ...)` if it wasn't disconnected |

Invariants:
- No event without a state change (TLA+ `LegalEvents`). `connecting` → `connecting` emits no second `connecting` event. Retries inside `connecting` are silent apart from `error` events.
- **Event order inside one step**, as in centrifuge-js and centrifuge-go: subscription events (client-side `subscribing`, then server-side `subscribing`) come before the client's `connecting` / `disconnected`. On `close()`, every `unsubscribed(2)` comes before `disconnected`.

### 6.2 Connect attempt

Steps, each checked against the current **connect attempt id** (§9.2) when its result comes back:

1. If a token is needed (`token` empty and `get_token` set, or `refresh_required`), call `get_token` in a separate task, **before dialing** (`centrifuge-go` #165: a connection that sends no connect command for a while gets closed by Centrifugo).
   - `Ok(Some(t))`: store it. `Ok(None)` connects without a token (anonymous), as an empty token does in centrifuge-js.
   - `Err(Unauthorized)`: → `disconnected(1, "unauthorized")`.
   - `Err(Other)`: `error(ConnectToken)`, schedule a retry with backoff.
   - `refresh_required` without `get_token`: `error(Configuration)` and `disconnected(1, "unauthorized")`, unless the app called `set_token` meanwhile.
2. Pick the transport (§5.8) and open it with `handshake_timeout`. DNS resolution is part of it. At most one lookup runs per client, and a new attempt reuses a lookup still running, because a dropped future doesn't stop a blocking `getaddrinfo` (`getaddrinfo` runs in `spawn_blocking`). On failure: `error(Transport)`, retry: immediately with the next transport during the first fallback round, otherwise after backoff.
3. Write the `connect` command **as the first frame**. Over HTTP-streaming it is the body of the request that opens the stream (§5.7). It carries the token, data, name, version and `emulated_headers` (as `headers`), plus `subs` for recoverable server-side subscriptions (`recover: true, epoch, offset`). No other command may be written on this transport before it (`centrifuge-go`: the transport becomes current only after connect is written). Over emulation, no command can be sent before the connect reply anyway, because `session`/`node` come from it.
4. Wait for the reply up to `timeout`. On timeout: close the transport and retry. If the connect command can't be written, that's the same as a timeout.
5. On an error reply:
   - 109 (token expired): set `refresh_required` (with `get_token`, or when the token is still the one this attempt sent; pattern 58), retry.
   - Temporary: retry with backoff.
   - Otherwise: → `disconnected(code, message)`.
6. On success, **one core step** does the following. Its events are delivered after it, as a batch (§9.4):
   - Set `connected`, and store `session`/`node`.
   - Mark the reconnect backoff for reset. It resets on the first server ping or command reply over this connection, not on the connect reply, over every transport. Otherwise a failure that always follows the connect (a 3008 after a huge recovery reply, a broken emulation endpoint) would reconnect at the minimum delay forever (pattern 67; centrifuge-python F4).
   - Arm the token refresh if `expires`, and the ping timer.
   - Emit `connected`.
   - Remove server-side subscriptions missing from the reply (`unsubscribed`).
   - Queue each server-side subscription of the reply as a pending delivery (§8).
   - Resubscribe each client-side subscription in `subscribing` that has **no flow in progress and no armed resubscribe timer**. A subscription waiting for its retry delay keeps it, so an error is never retried without its delay, also across a reconnect (TLA+ decision "the resubscribe timer survives it").

   Nothing in the step re-checks for handler effects: handlers run after it. Only the pending deliveries that follow it (server-side subscriptions and their recovered publications, §8) are separate steps, with app ops between them.

### 6.3 Reconnect backoff

`delay(attempt) = min(max, min * 2^attempt)`, with jitter: a uniformly random duration in `[min, delay(attempt)]`, never below `min_reconnect_delay`. `attempt` is reset on the first server ping or reply to any command (a subscribe reply counts) after the connect reply. With pings disabled and no commands sent, it resets once the connection has stayed open for `max_reconnect_delay` (§6.2 step 6). This is not the same as a transport being "proven" for fallback (§5.8). Subscription resubscribe uses the same function, with its own counters and limits, reset after a successful subscribe. The zero-delay first fallback round (§5.8) neither uses nor advances `attempt`.

### 6.4 Ping/pong

- `connect.ping` (seconds) > 0 enables the no-ping timer: `ping + max_server_ping_delay`.
- Every **decoded** message (ping, reply or push) re-arms it. Undecodable data and filler don't (pattern 72).
- On a ping, if `connect.pong` is true, answer with an empty command **on the transport the ping came from, and only if it's still current** (pattern 29).
- When the timer fires: disconnect with `connecting(2, "no ping")` and reconnect. Before acting, process frames already read from the socket (pattern 55, §9.7).
- **Liveness is judged by what was received, not what was processed.** The reader stores the instant of its last decoded message in shared state. The no-ping timer, and the connect and subscribe reply timeouts, check it before acting: if data arrived within the window but waits behind a slow handler or a full read-ahead window (§9.2), they re-arm instead of tearing the connection down (centrifuge-java F12b).
- A pong that can't be written closes the transport (`connecting(1)`).

### 6.5 Disconnect codes and advice

Sources: a `disconnect` push, or a WebSocket close frame.

- **Close frame:** if the reason text parses as JSON `{"code","reason","reconnect"}` (legacy servers), use it. Otherwise use the close code and reason.
- **Reconnect rule:** reconnect if `code < 3500 || code >= 5000 || (4000..4500).contains(&code)`. Otherwise it's terminal.
- Transport close codes below 3000 are hidden behind client code 1 "transport closed" (reconnect), except 1009 "message too big", which maps to code 3 "message size limit" (terminal).
- **3014 state invalidated:** with `get_token`, drop the connection token and set `refresh_required`. Without `get_token`, keep the token (pattern 22). Invalidate every subscription's state (§7.7). Reset server-side subscription positions that exist to the sentinel (pattern 57). Then reconnect.
- A disconnect that arrives on a transport that is no longer current is ignored (pattern 18).
- When the server closes with a terminal code while calls are pending, handle the close first, then fail the pending calls (pattern 47).
- **3005 (connection expired):** with `get_token`, set `refresh_required`, so the reconnect fetches a fresh token instead of sending the expired one and getting a 109 first.
- A `connect` push is for unidirectional transports and is ignored here. A `refresh` push can arrive on any connection: the server API `refresh` sends one (`centrifuge` `Client.Refresh`). It's handled by §6.6.
- **Errors the SDK doesn't report** (no `error` event): the close or read error of a transport the SDK closed itself (patterns 14, 18), the failure of a superseded connect attempt (pattern 11), and a subscribe or call cut off by a teardown. Each real failure produces exactly one `error` event, with the cause in `source()`.

Client-side codes (public constants in `codes`):

| Kind | Code | Meaning |
|---|---|---|
| connecting | 0 | connect called |
| connecting | 1 | transport closed |
| connecting | 2 | no ping |
| connecting | 3 | subscribe timeout |
| connecting | 4 | unsubscribe error |
| disconnected | 0 | disconnect called |
| disconnected | 1 | unauthorized |
| disconnected | 2 | bad protocol (the reason names the cause: `"bad protocol"`, `"delta error"`, `"handler panicked"`, `"malformed reply"`) |
| disconnected | 3 | message size limit |
| subscribing | 0 | subscribe called |
| subscribing | 1 | transport closed |
| unsubscribed | 0 | unsubscribe called |
| unsubscribed | 1 | unauthorized |
| unsubscribed | 2 | client closed |

Server error codes the SDK reacts to: 109 token expired, 112 unrecoverable position, `temporary` flag. Server unsubscribe codes: `< 2500` → unsubscribed, `>= 2500` → resubscribe, 2502 → state invalidated. Disconnect codes 3000–4999 follow the rule above.

### 6.6 Connection token refresh

- Armed when the connect or refresh reply has `expires`, after `ttl` seconds (minimum 1 s, so `ttl: 0` doesn't loop).
- Each refresh belongs to the **connected session** (`conn_generation`) it was armed in. Every continuation checks that session is still current (pattern 9).
- `get_token`:
  - `Ok(None)` or `Err(Unauthorized)`: → `disconnected(1, "unauthorized")`.
  - `Err(Other)` (a panic in `get_token` included): `error(Refresh)`, retry after a jittered 5–10 s (centrifuge-js `backoff(0, 5000, 10000)`). The retry is never scheduled past the token's known expiry.
  - `Ok(Some(t))`: store `t` (so a later reconnect uses it), send `refresh`.
- Refresh reply:
  - Temporary error, or the command can't be written: `error(Refresh)`, retry after a jittered 5–10 s.
  - Non-temporary error: → `disconnected(code, message)`.
  - Success: re-arm if `expires`.
- The SDK keeps the token's expiry as wall-clock time (`SystemTime`). A reconnect that starts after it has passed sets `refresh_required`, so the stale token isn't tried first after an outage or a laptop sleep.
- **`refresh` push** (server API `refresh`). It adjusts only a refresh schedule the client already runs, within the current `conn_generation`:
  - If the connection is in **client-side refresh** (the connect or last refresh reply had `expires: true`): with `expires: true`, re-arm the refresh timer for the new `ttl`. With `expires: false`, cancel it, because the connection no longer expires.
  - Otherwise, ignore it. Either the server refreshes the connection itself (its connect reply always says `expires: false` then, and the push can still say `expires: true`), or the connection had no expiry. Starting a client refresh there could make the server reject the refresh command and disconnect. If the server did make a non-expiring connection expire, it closes it with 3005 at expiry, and the reconnect fetches a fresh token (§6.5).
  - centrifuge-js ignores this push entirely (§1.1).
- `set_token(t)`: replaces the stored token. It clears `refresh_required` only when there's no `get_token` (pattern 58).
- A static token with `exp` and no `get_token`: on 109 the SDK reports `error(Configuration)` and stops with `disconnected(1, "unauthorized")`, rather than retrying the expired token forever (pattern 36), unless `set_token` supplied a new one.

## 7. Client-side subscription behaviour

### 7.1 States

| From | Trigger | To | Event |
|---|---|---|---|
| unsubscribed | `subscribe()` | subscribing | `subscribing(0, "subscribe called")` |
| subscribing | subscribe reply OK | subscribed | `subscribed`, then recovered publications |
| subscribing | temporary error / 109 with `get_token` / transport issue | subscribing (retry after the resubscribe delay) | `error` |
| subscribing | 109 without `get_token` | unsubscribed | `error(Configuration)`, `unsubscribed(1, "unauthorized")` (pattern 82) |
| subscribing | non-temporary error, `Unauthorized`, empty token from `get_token` | unsubscribed | `unsubscribed(code, reason)` only, as centrifuge-js `_subscribeError` |
| subscribed | client leaves `connected` | subscribing | `subscribing(1, "transport closed")` |
| subscribed | unsubscribe push `>= 2500` | subscribing | `subscribing(code, reason)` |
| subscribed | unsubscribe push `< 2500` | unsubscribed | `unsubscribed(code, reason)` |
| subscribing, subscribed | `unsubscribe()` | unsubscribed | `unsubscribed(0, "unsubscribe called")` |
| unsubscribed | `unsubscribe()` | — | none (no-op) |
| subscribing, subscribed | `close()` | unsubscribed | `unsubscribed(2, "client closed")` |

After `close()`, `subscribe()`, `unsubscribe()` and `set_data()` are no-ops, `new_subscription(..).build()` returns `Err(ClientClosed)`, calls fail with `ClientClosed`, and no callback result is acted on (TLA+ `ClosedIsFinal`).

Subscriptions move to `subscribing` **as part of the same core step** that moves the client out of `connected`. That way a `connect()` from a handler always finds them subscribing (`centrifuge-go` `moveToDisconnectedIf`).

**What a teardown does to subscription bookkeeping** (TLA+ `TdSub`): leaving `connected` clears `inflight`, forgets subscribe replies, deferred unsubscribes and waits for unsubscribe replies (the server state is gone with the connection), and drops pending recovered deliveries. It keeps the position, the token and an armed resubscribe timer. Nothing of the old connection can block or time out the next one (centrifuge-dart #134, centrifuge-java F17).

### 7.2 Subscribe flow

Runs when the subscription is `subscribing`, the client is `connected`, no resubscribe timer is armed, and no earlier unsubscribe of the **channel** is still waiting for its reply (§7.3). At most one subscribe flow runs per subscription (`inflight`).

Each continuation is tagged with `(subscribe_attempt, invalidations)`:
- A result whose `subscribe_attempt` changed (`unsubscribe()` happened) is dropped (patterns 33, 46; TLA+ `FlowCurrent`).
- A result dropped only because `invalidations` changed **restarts the flow** at step 1, so the subscription never stays stuck in `subscribing` (TLA+ `SubscribingEnds`).
- `unsubscribe()` during a `get_token`/`get_state` step clears `inflight` at once (no command was sent), so a following `subscribe()` starts a fresh flow (centrifuge-csharp B6).
- Callbacks for different subscriptions may run concurrently. Each one's task always settles: a panic (`catch_unwind`) or a `JoinError` counts as `Err(Other)`.

Steps:

1. If `get_state` is set and there's no saved position (`recover == false`), call `get_state` in a task. On `Ok(pos)` set `recover = true` and the position. On `Err` (always temporary, TLA+ decision), emit `error(GetState)` and retry after the resubscribe delay.
2. If the token is empty and `get_token` is set, call it in a task. On `Ok("")` or `Unauthorized`, go to `unsubscribed(1, "unauthorized")`. On `Err(Other)`, emit `error(Token)` and retry after the resubscribe delay.
3. If the client is no longer connected, stop. The next connect resubscribes (don't fetch tokens for a subscribe that can't be sent).
4. Send `subscribe` with:
   - `channel`, `token`, `data`
   - `recover: true` with `epoch`/`offset` when there's a position (`offset` only if > 0)
   - `positioned`, `recoverable`, `join_leave`, `delta`, `tf`
   - `flag`: `1` (channel compaction) plus `2` (reject unrecovered) when `get_state` is set

   Capture `conn_generation` with it. If the command can't be written, that counts as a temporary error: retry after the resubscribe delay.
5. Reply handling:
   - Timeout of the current subscribe, **or of a subscribe whose unsubscribe is deferred** (whether the server subscribed is unknown): reconnect the client with `connecting(3, "subscribe timeout")`, if that `conn_generation` is still connected. A local timeout doesn't mean failure on the server, so a clean connection is the safe way out. The timeout of any other outdated subscribe is dropped.
   - Error 112 with `get_state`: clear the position and delta base, then retry **after the resubscribe delay**, which calls `get_state` again.
   - 109 with `get_token`: clear the token, emit `error(Subscribe)`, retry after the resubscribe delay. Without `get_token`: `error(Configuration)`, `unsubscribed(1, "unauthorized")` (pattern 82).
   - Temporary error: `error(Subscribe)`, retry after the resubscribe delay.
   - Non-temporary error: `unsubscribed(code, message)`, with no separate `error` event (centrifuge-js).
   - A reply of an outdated flow (different `subscribe_attempt` or `conn_generation`) is **ignored**, and no resubscribe is started from it (TLA+ `Sdk_SubscribeReply`). The subscription is resubscribed by the flow that made it outdated. If it's the subscribe whose unsubscribe was deferred, the deferred unsubscribe is sent if it succeeded (§7.3).
   - Success of the current flow:
     1. Apply delta negotiation and decode recovered publications (§7.5). On failure, stop the client with bad protocol before anything is emitted.
     2. Set `subscribed`, bump `sub_session`, reset the resubscribe backoff, arm the token refresh if `expires`, update the position (§7.4) and register the compaction id.
     3. Emit `subscribed`. Waiters of `sub.ready()` are released only after this event's handler has returned, and only if the subscription is still subscribed then (centrifuge-js; centrifuge-python F15).
     4. Queue the recovered publications as pending deliveries, one per step (§9.4). Before each, check that `state == subscribed` and `sub_session` is unchanged (pattern 21). Each one's position is committed when it's delivered (§7.4).

`subscribed` event: `stream_position` is set when `positioned || recoverable`. `recovered` is only meaningful when `was_recovering`.

### 7.3 Unsubscribe

- `unsubscribe()` while subscribed: write the `unsubscribe` command, then emit `unsubscribed`, then await the reply (pattern 44). Bump `subscribe_attempt` so in-flight replies and continuations become stale (pattern 33).
- **Unsubscribe bookkeeping is per channel, not per subscription object** (TLA+ `App_Replace`, `NoSubscribeOfSubscribedChannel`). The core tracks, per channel, unsubscribe replies still awaited and a deferred unsubscribe. `remove_subscription()` + `new_subscription()` of the same channel, or a new subscription from the old one's `unsubscribed` handler, gives a new object. That object:
  - over emulation, waits for those replies before its first subscribe, so the server never answers 105 (over WebSocket the server handles the unsubscribe first anyway, TLA+ `CanStart` with `Ordered`)
  - starts with no position (only `since`)
  - never receives the removed object's replies or callback results
- `unsubscribe()` while a subscribe is in flight, **over emulation**: emit `unsubscribed` at once, but **defer** the `unsubscribe` command until the subscribe reply. Send it only if that subscribe succeeded. A later `subscribe()` of the channel waits for the replies of earlier unsubscribes. There, separate requests can reach the server out of order. This is a TLA+ decision (`bug_no_unsub_defer`), and centrifuge-js does it since #443. **Over WebSocket** commands reach the server in order, so the unsubscribe goes at once and a later subscribe doesn't wait (TLA+ `Ordered`; the subscribe's reply is then stale). An earlier draft deferred on every transport; replaying the TLA+ traces showed the model doesn't allow that schedule, so the core follows the model.
- A flow waiting for `get_token` or `get_state` counts as in flight: `unsubscribe()` sends an `unsubscribe` command then too, as centrifuge-js (`_inflight`) and the TLA+ model (`NeedsUnsubCmd`) do. It's redundant (no subscribe was sent for the flow) but harmless, and keeps the server-side bookkeeping identical to the reference.
- An error or timeout on the unsubscribe reply, or an unsubscribe command that can't be written, reconnects the client with `connecting(4, "unsubscribe error")`, so the server ends up with no stale subscription (pattern 79). This applies **only while the `conn_generation` it was sent in is still connected**. A failure caused by a teardown (`ClientDisconnected` after `disconnect()` or a transport close) does nothing, and doesn't advance the backoff (centrifuge-js #402, centrifuge-dart #157, centrifuge-swift R2).
- An unsubscribe **push** is applied only when `subscribed`. Otherwise it is ignored (pattern 37). A resubscribe code arriving after the app's `unsubscribe()` must not move it back to subscribing. This includes a 2502, or a code below 2500, arriving while our subscribe is in flight. The server sends the unsubscribe push of a subscribe in progress only **after** its reply (TLA+ server guarantee), so a push that arrives first belongs to the previous subscription. Applying it is the TLA+ bug variant `unsub_push_any_state`.

### 7.4 Recovery position rules

They apply to client-side and server-side subscriptions alike (patterns 77, 78):

- On a publication: advance `offset` only when `pub.offset > 0`. Adopt the wire `Publication.epoch` (proto field 14, read by the core and not part of the public `Publication`) only if the stored epoch is empty, `pub.epoch` isn't empty, and `pub.channel` is empty.
- **Publications of another stream** move nothing: a publication whose `pub.channel` is set and differs from the subscription's channel (a wildcard-style delivery) never moves the position and is never checked by the dedupe below (centrifuge-go "wildcard offset follows concrete channel").
- On a subscribe/connect reply:
  - **Only a recoverable reply sets a position.** If `reply.recoverable` is false, the stored position is dropped and the recover flag cleared. Otherwise a `since` or `get_state` subscription to a channel without recovery would ask to recover, and report `recovered = false`, on every resubscribe (centrifuge-js #436, centrifuge-csharp A9).
  - Adopt the reply's epoch only if `reply.epoch != ""` or the stored epoch is empty. An empty reply epoch means the server has no stream position right now, so keep the known one.
  - **Take the reply's offset at once**, as centrifuge-js `_setSubscribed` does. After a successful recovery the server replies with the offset the client asked to recover from (`res.Offset = req.Offset` in `centrifuge` `client.go`), and the recovered publications then move it forward as they're delivered. Without recovery the reply has no publications and its offset is the stream top.
  - **The position never moves backward.** A publication delivered from a reply (e.g. initial state from a subscribe proxy) moves the offset only to `max(stored, pub.offset)`.
  - `subscribed.stream_position` reports the reply's position. After a recovery that's the position recovered from, not the top. Apps that checkpoint should checkpoint from delivered publications, which the docs say.
- The position moves **when the publication is delivered** (§9.4: its handler returned, or a stream consumer took it), and only for publications that are actually delivered (patterns 16, 68). Nothing the handler asks for runs before that commit, so a resubscribe started from the handler recovers *after* this publication (pattern 56). A publication that arrives while the subscription isn't `subscribed` is dropped and doesn't move the position.
- **Defensive dedupe:** a *live* publication of the subscription's own stream with `0 < offset <= committed offset` in the same epoch is a duplicate (the committed offset is the last delivered one, or the reply's). It's dropped and logged at `warn`. Publications carried in a subscribe reply aren't deduplicated; the server already merged them. The server shouldn't send one (TLA+ server spec), but delivering it twice would break exactly-once for the app.
- `unsubscribe()` keeps the position. `since` in the config seeds it for the first subscribe.
- **Join and leave** pushes, like publications, are delivered only while `subscribed` in the current `sub_session`. Any other push for a subscription that isn't subscribed is dropped (centrifuge-go E3, centrifuge-python F6).

### 7.5 Delta compression

- Requested via `delta: Some(Fossil)`. Applied only if the reply's `delta` field is true (negotiated, pattern 69).
- The base is the previous full payload (`prev_data`).
- Protobuf: `data` is the raw delta or the full payload.
- JSON: `data` is a JSON **string** containing the delta or the payload. Decode the string, then apply.
- A delta publication without negotiation, or without a base, or a failing apply: emit `error(Delta {channel, offset})` and go to `disconnected(2, "delta error")` with no reconnect. A broken chain would give the app wrong data.
- **The server sends the first publication after every subscribe in full** (after a recovery, the first recovered one; TLA+ decision). The base is reset at each subscribe, and a delta arriving before a full payload of the current subscription is a delta error. A delta chain never crosses subscriptions.
- The delta base is cleared on state invalidation and on error 112.
- `since` on a delta channel needs Centrifugo v6.9.6 or newer. Older servers end it as a delta error, which the docs note.
- The fossil implementation is a port of `centrifugal/fdelta` with its test vectors and a fuzz target.

### 7.6 Subscription token refresh

The same as §6.6, scoped to the subscription's `sub_session`, plus `invalidations`. It uses `sub_refresh`. A non-temporary error on the refresh reply unsubscribes (with an `unsubscribe` command) using the server's code. So does `Unauthorized` or an empty token from the refresh `get_token`, as `unsubscribed(1, "unauthorized")`. That unsubscribe's reply is awaited like any other (§7.3). Retries after a temporary error, a `get_token` error or a failed write are jittered 10–20 s (centrifuge-js `backoff(0, 10000, 20000)`) and never scheduled past the token's known expiry. The refresh is checked against the session **after** the token fetch and before sending (pattern 42). The new token is cached for later resubscribes.

### 7.7 State invalidation

Triggered by unsubscribe push 2502 or disconnect 3014.

- Clear the compaction id mapping.
- Clear the token, only with `get_token`.
- Bump `invalidations`.
- If a position exists, set `offset = 0, epoch = "_"` (a sentinel the server never matches, so the reply reports `was_recovering: true, recovered: false`). If no position exists, leave it unset (pattern 57).
- Clear the delta base.
- 2502 then resubscribes, since it is ≥ 2500.

### 7.8 Channel compaction

When the subscribe reply carries `id > 0`, pushes for that subscription arrive with `push.id` instead of `push.channel`. The core keeps `id → subscription`. It is reset whenever the connection's session ends and re-registered on each subscribe. A push with an unknown id is dropped.

### 7.9 Tags filter

`FilterNode` is built with the `Filter` functions, mirroring `centrifuge-go`: `Filter::eq/neq/in_/nin/exists/not_exists/starts_with/ends_with/contains/gt/gte/lt/lte/and/or/not`. It's sent as `tf`.

## 8. Server-side subscriptions

- They're created from `connect.subs` and from `subscribe` pushes, and removed by `unsubscribe` pushes and by absence from a later connect reply.
- **The defensive dedupe of live publications (§7.4) applies to server-side subscriptions too:** one at or below the committed offset of the same epoch is dropped.
- **Any unsubscribe push removes a server-side subscription, whatever its code** (2500+ and 2502 included). The client can't resubscribe it (TLA+ decision).
- Registry entries hold `(offset, epoch, recoverable)`. On reconnect, recoverable entries go into `ConnectRequest.subs` with `recover: true`.
- When the client leaves `connected`, emit `subscribing(channel)` for each entry. Entries are kept.
- On `close()`, emit `unsubscribed(channel)` for each entry.
- Push routing: route a channel's push to the server-side entry when one exists, unless a client-side subscription to the channel is `subscribed` (pattern 65). Compacted pushes (`push.id > 0`) only target client-side subscriptions.
- **Each server-side subscription of a connect reply is announced in its own step** (TLA+ `Sdk_SSSubscribed`), through the pending-delivery queue (§9.2):
  - `subscribed(ch)` is emitted, and the entry's position is stored with it.
  - Then come its recovered publications, one per step, each checked against the connect attempt and the entry still existing (pattern 21), with positions committed on delivery (§7.4).
  - A handler may disconnect in between. The rest then stay unannounced and their positions unstored, so they're recovered on the next connect.
  - The next inbound item waits until these steps are done.
- Positions follow §7.4.

## 9. Architecture

### 9.1 Requirements

Most bugs found in the official SDKs (`sdk_validation/SKILL.md`) come from one cause: client and subscription state changed from several threads under locks. Examples are centrifuge-go's mutex plus goroutines, and centrifuge-csharp before its single-loop refactor (#33). Typical results were lock-order deadlocks, events out of order, stale continuations acting on a newer connection, and handlers that re-entered the SDK while it was half-way through a transition.

The SDK TLA+ model (§0) is the contract. It models the SDK as **one event loop**: every step is one task of it. A step is an application call, a frame item, a timer, or a callback settling. The application may act between any two steps, also between two events of one frame. Whether handlers run inline or from a queue is left to the SDK.

The architecture has to give:

1. **One writer of state.** No lock protects client or subscription state, because only one place ever touches it.
2. **Events in the order of state changes**, across the client, all its subscriptions and server-side subscriptions. Each state change is exactly one event (`LegalEvents`).
3. **No step runs while a handler runs.** The state a handler sees is the state its event reports. A publication counts as delivered once its handler returns, which is what `ExactlyOnce` needs.
4. **Calling the SDK from a handler or a callback never deadlocks.** Where an operation can't complete without the loop making progress, it fails at once with `Error::WouldDeadlock` instead of hanging (§9.5).
5. **Calls from other threads never wait for handlers.** They take effect in call order, right after the step in progress.
6. **Efficient:** no thread or allocation per message, and no driver round trip for publish/RPC/history/presence on a connected client.

### 9.2 Structure: a sans-IO core driven by one task

```
                         ┌──────────────────────── driver task (one per client) ───────────────────────┐
 app calls ──ops (mpsc)──►  select! (biased): ops ▸ pending ▸ inbound ▸ callbacks ▸ timers ▸ writes │
                         │        │                                                                    │
                         │        ▼                                                                    │
                         │   Core::step(input, now) ──► Outputs { events, writes, timers, callbacks,  │
                         │   (pure, sync, no IO, no locks)        transports to open/close, snapshot } │
                         │        │ events                                                             │
                         └────────┼────────────────────────────────────────────────────────────────────┘
                                  ▼  batch + wait for ack
                         ┌─────────────────┐            ┌─────────────┐  calls' replies  ┌───────────────┐
                         │ event dispatcher│──handlers─►│ application │◄─────────────────┤ reader task   │
                         │ (own OS thread) │            └─────────────┘  (call table)    │ writer task   │
                         └─────────────────┘                                             └───────────────┘
```

**`Core`** is a plain synchronous state machine with no Tokio, no sockets, no clocks and no locks:

```rust
impl Core {
    fn step(&mut self, input: Input, now: Instant, out: &mut Outputs);
    fn next_deadline(&self) -> Option<Instant>;
}
enum Input {
    App(Op),                                   // connect, disconnect, subscribe, unsubscribe, set_token, ...
    Opened { transport: TransportId },
    Item { transport: TransportId, reply: Reply },     // one item of a frame
    Closed { transport: TransportId, code: Option<u16>, reason: String },
    WriteFailed { transport: TransportId, frame: FrameId, error: WriteError },
    TokenSettled { flow: FlowId, result: Result<Option<String>, TokenError> }, // subscription tokens: Some or Err
    StateSettled { flow: FlowId, result: Result<StreamPosition, BoxError> },
    TimerFired { timer: TimerId },
    Delivered { batch: BatchId, outcome: DeliveryOutcome },             // handler ack (§9.4)
}
```

- Each `Input` variant maps to a TLA+ step (`App_*`, `Env_*`, `Cb_*`, `Sdk_*`). Each `Core` method names the step it implements, so code and model can be compared rule by rule.
- `Outputs` is data: events to deliver, frames to write, timers to arm or cancel (deadlines, not tasks), callbacks to start (with the `FlowId` they belong to), transports to open or close, and the new state snapshot.
- Being pure makes the core deterministic. Tests feed it inputs and a fake clock and assert on outputs, with no runtime and no sleeps (§11.1). The same core can later serve a wasm driver or a blocking facade.
- This is the sans-IO style of `quinn-proto`, `rustls` and `str0m`.

**The driver task** owns the `Core` and does all IO. Each loop iteration:
1. Take every input that's already waiting, up to 64, in this priority (`select!` is biased), stepping each one in turn (§9.10):
   - App ops first, so calls take effect right after the step in progress, in call order.
   - Then the core's **pending deliveries**: recovered publications of a subscribe or connect reply, one per step.
   - Then inbound items, then callback results, then expired timers, then write outcomes.

   Pending deliveries come before inbound items, so a live publication can't overtake the recovered ones before it (pattern 15). They come after app ops, so an `unsubscribe()` from a handler stops the rest (pattern 21).
2. Call `core.step(...)`.
3. Apply the outputs: open or close transports, queue frames, arm the single `sleep_until(core.next_deadline())`, spawn callbacks, publish the snapshot.
4. If the step produced events, **wake the writer first**, so commands the step produced (a pong, an unsubscribe) never wait behind a slow handler. Then hand the batch to the dispatcher and **await its ack** before taking the next input (§9.4). Ops sent meanwhile wait in the inbox. They're processed first once the ack arrives.
5. The step itself runs under `catch_unwind`. A panic in the core (a bug) publishes a terminal snapshot (`disconnected(2, "bad protocol")`), fails every waiter and call, and emits `error`, so the client never stays `connected` while deaf (pattern 19). A reader or writer task that ends with a `JoinError` (a panic) is handled the same way.

**Handlers block the read loop**, as in centrifuge-js, centrifuge-go and centrifuge-csharp. While a handler (or a stream consumer) holds an event, no step runs. That's required for exactly-once and ordering (§9.4). It also gives backpressure: an app slower than the incoming stream falls behind visibly, instead of buffering without limit.
- The reader may run ahead of the driver by **at most 256 items or 4 MiB** of undelivered data, whichever comes first (a bounded channel plus a byte budget). A single item larger than the byte budget is admitted alone when the window is empty, so messages up to `max_message_size` always get through. Then it stops reading the socket. TCP backpressure reaches the server, whose slow-client handling (disconnect 3008) decides, exactly as if the handler blocked the socket read itself. Memory stays bounded however slow the app is.
- The bounded window still lets call replies behind a short burst be resolved by the reader (§9.3), so a handler awaiting an `rpc()` works unless more than the window arrives meanwhile. In that case the call ends with its timeout and never hangs.
- A handler that holds the loop longer than the server's ping interval plus pong timeout gets the connection closed by the server ("no pong", 3012). This is the same contract as the other SDKs. After a long handler, pings already buffered are processed before the client's own no-ping timer is checked (inbound items come before timers in `select!`), so a slow handler never causes a client-side "no ping" by itself.

**Frames are split into items** by the reader. Each item is one `Input`, so app ops from a handler run between two items of the same frame (pattern 15, and TLA+ "a teardown drops the rest" of a frame).

**Generation counters** tag every continuation and timer. Each one names the counters it checks, and code review rejects a continuation without a check. In the core these are plain integers; there are no atomics.

| Counter | Scope | Bumped when | Guards |
|---|---|---|---|
| `connect_attempt` | client | every new attempt, disconnect, close | token/data results, transport opens, connect reply |
| `conn_generation` | client | the connected session ends | refresh timers and results, subscribe and unsubscribe replies and timeouts, pongs |
| `transport_id` | client | every new transport | inbound items, close notifications, write outcomes, call table entries |
| `subscribe_attempt` | subscription | `unsubscribe()` | subscribe replies, get_token/get_state results (`FlowId`) |
| `sub_session` | subscription | each entry into `subscribed` | sub refresh, recovered publication delivery |
| `invalidations` | subscription | state invalidation | token/state results, refresh |
| `unsubscribes` | subscription (in the snapshot) | each entry into `unsubscribed`, and `remove_subscription()` | subscription calls and `sub.ready()` waiting (§9.3) |
| `disconnects` | client (in the snapshot) | each entry into `disconnected` and each `disconnect()` call | waiting calls (§9.3) |

**Shared state** is deliberately small. None of it is ever held across an `.await` or a handler call:

| Shared piece | Type | Written by | Read by |
|---|---|---|---|
| Client snapshot (state, `conn_generation`, `disconnects`, current transport writer + session/node) | `watch` / `ArcSwap` | driver, at the end of a step | `state()`, call fast path, waiters |
| Subscription snapshot (state, `sub_session`, `unsubscribes`) | the same, per subscription | driver | `sub.state()`, sub call fast path, waiters |
| Op sequence: `requested_seq` (bumped by every sync op) and `applied_seq` (in the snapshot) | atomics | handles / driver | waiters, projected state (below) |
| Projected state: the state the not-yet-applied sync ops lead to | per client / subscription, small `Mutex` | sync op methods | `state()` |
| Subscription registry (`channel → Subscription`) | `Mutex<HashMap>`, leaf lock | `new_subscription(..).build()`, `remove_subscription()` on the calling thread | `get_subscription()`, `subscriptions()`, the driver |
| Last received instant | atomic | reader | liveness checks (§6.4) |
| Call table (`id → oneshot`, tagged with `transport_id`) | `Mutex<HashMap>`, leaf lock | calls insert, reader and writer resolve, driver sweeps on teardown | — |

**Sync calls take effect at once for the caller**, as in centrifuge-js, while still being applied by the driver, which keeps batching (§9.10). `connect()`, `disconnect()`, `subscribe()` and `unsubscribe()` change state in a way that depends only on the current state, so each one:
- bumps `requested_seq`
- records its effect in the projected state
- sends its op

`state()` returns the projected state until the driver has applied the op. So `client.connect()?; client.state()` is `Connecting` at once, and `sub.unsubscribe(); sub.state()` is `Unsubscribed` at once, also inside a handler. That matches centrifuge-js, where such a call runs nested, though the op's *events* still come after the handler returns (§1.1).

Waiters (`ready()`, calls waiting for the connection) first wait until `applied_seq` has caught up with the `requested_seq` they saw, then judge the state. So `client.connect()?; client.ready(None).await` waits instead of failing at once, and a `publish()` right after `disconnect()` waits for the next connect instead of going out on the old connection (centrifuge-csharp C1/D2, centrifuge-js #427). Inside a handler, where catching up would wait for the handler itself, waiters judge the projected state, with the §9.5 `WouldDeadlock` rules.

**The registry changes synchronously:**
- `build()` inserts into the registry before it returns, and checks for duplicates against it.
- `remove_subscription()` removes the entry before it returns. A new subscription to the channel is then possible at once, also from the `unsubscribed` handler (centrifuge-js #427, centrifuge-csharp A15). Its subscribe still waits for the old unsubscribe (§7.3).

### 9.3 Calls: the fast path

`publish`, `rpc`, `history`, `presence`, `presence_stats` and `send` don't change SDK state, so they don't go through the core:

1. The call future reads the snapshot. If the client is connected (and, for a subscription method, the subscription is subscribed), it inserts a `oneshot` into the call table under the current `transport_id` and queues its command on that transport's writer. The insert is refused if teardown already closed that `transport_id` in the table, so no call can be left behind after a sweep.
2. **The reader resolves call replies directly** from the call table, without passing through the driver. The table records the result kind each call expects. A reply without it (§5.6, "malformed") fails the call with `ClientDisconnected` and sends the driver a bad-protocol close. A join, leave or presence entry without `ClientInfo` decodes as empty info, as centrifuge-go does. State-changing replies (connect, subscribe, unsubscribe, refresh, sub_refresh) go to the driver as `Input::Item`. A reply nobody waits for is dropped (TLA+ `Sdk_StaleReply`).
3. A failed write fails exactly the calls in that frame (pattern 41): with `ClientDisconnected` when the transport broke, and with `Error::Transport { status }` when an emulation request was rejected on its own (e.g. HTTP 413, §5.7). A fast-path call wakes the writer itself.
4. On teardown, the driver first delivers the close events, then fails that transport's calls with `ClientDisconnected` (pattern 47).
5. The timeout runs in the call future: `select! { biased; reply, sleep(timeout) }`, so a reply already received wins (pattern 28). A sleep that completes more than 1 s late gets a 1 s grace (§9.7).
6. **When not connected** (or the subscription isn't subscribed), the call future waits on the snapshot `watch` up to `timeout`. It fails when the client enters `disconnected` or `closed` (`disconnects` changed). A subscription call or `sub.ready()` fails with `SubscriptionUnsubscribed` when `unsubscribes` changes, which catches an `unsubscribe()` + `subscribe()` too quick to observe, and `remove_subscription()` (centrifuge-go C3, centrifuge-python N2). A call made from a `disconnected` handler therefore waits for the next connect. That's the intended exception to pattern 8, and a test pins it. A call made while disconnected waits for the next connect, as in centrifuge-js and the TLA+ decision "method calls made while disconnected wait for the next connect; `disconnect()` fails them".
7. Commands are only queued on a transport whose connect reply was received, so nothing ever overtakes the connect command (§6.2).

**Call results aren't ordered with events.** A reply resolved by the reader may reach the app before earlier publications of the same frame have reached handlers. The core's "rest of the frame is dropped" rules (§5.3, §5.5) apply to events and state, not to call results already received. This is documented; no SDK orders call completions against later events.

So a call made from a handler doesn't depend on the loop. That's what makes calls from handlers safe (§9.5). It also costs no driver round trip in the common case.

### 9.4 Event delivery

- **One dispatcher per client** runs every handler of that client and its subscriptions, one at a time, in the order the core produced the events (pattern 38).
- **A step's events are delivered as one batch, and the driver waits for the ack** before the next step. So:
  - Inside a handler, `client.state()` / `sub.state()` report the state the event announces, or, after the handler itself called a sync op, the projected state that op leads to (§9.2; invariant I1).
  - What a handler asks for (`unsubscribe()`, `disconnect()`, `connect()` ...) is applied right after the batch, before any further frame item, timer or callback result. The app sees the effect's events next.
- **Exactly-once delivery** (TLA+ `ExactlyOnce`, `NoStalePublication`). These rules together keep it:
  1. **Delivered means handed to app code**: the handler returned, or a stream consumer took the item (below). The ack carries the outcome. The core commits the recovery position only on a successful ack (§7.4). A panicking handler commits nothing, so the stopped client recovers that publication on the next `connect()`.
  2. **One publication per step.** Live publications are one inbound item each. Recovered publications go through the core's pending-delivery queue, one per step (TLA+ `Sdk_Deliver`). Between two of them, app ops run, so a handler's `unsubscribe()` or `disconnect()` stops the rest, and none is delivered after `unsubscribed` (pattern 21).
  3. **Ordering:** pending deliveries are taken before inbound items (§9.2), so recovered publications always precede the live ones behind them.
  4. **Stale data never reaches the app:**
     - Items from a closed transport are dropped by `transport_id`.
     - Pending deliveries of an ended subscribed session are dropped by `sub_session`.
     - Nothing undelivered moves the position, so recovery fetches all of it again.
  5. **No queue between "delivered" and the app.** The SDK never buffers publications after counting them delivered. That's why streams use hand-off semantics instead of a buffer.
  6. **What the SDK can't promise:** work a handler hands off (a spawned task, the app's own channel) is the app's to finish. If the app loses it, recovery won't resend it. The docs say this next to the handler API.
  7. **When recovery is impossible**, the app learns it: `subscribed` with `was_recovering && !recovered`, or `get_state` runs again (error 112). Exactly-once never silently degrades into a gap.
- **Where handlers run** (`ClientBuilder::event_delivery`):
  - `EventDelivery::Thread` (default): a dedicated OS thread per client, named `centrifuge-events`. It enters the runtime context, so `tokio::spawn` works inside handlers. A handler that blocks stalls only this client's events, never the Tokio runtime. Blocking on a fast-path call works (§9.5), on any runtime flavour, including `current_thread`.
  - `EventDelivery::Inline`: handlers run on the driver task itself. No extra thread, so it suits load tools with thousands of clients. Handlers must not block, like any code in an async task. Blocking on an SDK future inside an inline handler fails with `WouldDeadlock`.
- **Handler signatures:** `FnMut(&Client, Event) + MaybeSend + 'static` and `FnMut(&Subscription, Event) + MaybeSend + 'static` (§9.11). A handler is taken out of its slot while it runs, so registering a handler of the same kind from inside it neither deadlocks nor panics. The new one takes effect for the next event (the Rust form of pattern 32). Handlers run serially, so `FnMut` is enough (no `Sync`) and handlers can keep plain mutable state. The handle is passed in, as in centrifuge-swift and centrifuge-java, so handlers don't need to capture a `Client`. That avoids the reference cycle that would keep the client alive (§9.8).
- **Handler panics** are caught on the dispatcher. As in centrifuge-js and centrifuge-csharp #34, the client stops loudly:
  - The panic is reported through the `error` event.
  - The client goes to `disconnected(2, "bad protocol")` without reconnecting, and later handlers for that event are skipped.
  - A publication whose handler panicked doesn't count as delivered, so the next recovery delivers it again.
  - The SDK's own state is complete, as if the handler had returned.
  - A panic inside the `error` handler is logged with `tracing`, never re-emitted as another `error` event, and stops the client the same way.
- **Waiters are released after the handler of their event.** `ready()` waiting for `connected` or `subscribed` resolves after that event's handler returned, and only if the state still holds (centrifuge-js; centrifuge-python F15).
- **Streams** (phase 1): `client.events()`, `sub.events()` and `sub.publications()` return `impl Stream + FusedStream`. Each kind has one active stream per object: a second call takes over, and the earlier stream ends with `None`. `next()` is cancel-safe: dropping it inside `select!` loses no item, and the item stays offered. Streams end after `remove_subscription()`'s cleanup unsubscribe completes, or on `close()` (pattern 60). They're tested against the `futures` stream contract (wakers, `None` is final; pattern 64). They use **hand-off** semantics: the dispatcher offers one item and waits until the consumer's `next()` takes it, and only then acks. So the guarantees match callbacks:
  - "delivered" means the consumer took the item
  - nothing taken counts before it's taken
  - an `unsubscribe()` made between two `next()` calls is applied before the following item
  - a dropped stream acks like an unregistered handler: it receives nothing more, and the position still moves, as with no listener

  The price is that a consumer that stops calling `next()` holds the loop, like a slow handler. Waits that need the loop (`ready()`, calls while connecting) made by that consumer between two `next()` calls can't complete, so they end with their timeout. The docs say so. The SDK can't tell the consumer task apart, so it can't fail them fast with `WouldDeadlock` as it does in handlers.

  An unbounded buffer would be simpler, but it would count publications as delivered before the app sees them. A dropped stream or an `unsubscribe()` with items still buffered would then lose them across recovery, or deliver them after `unsubscribed`. Both break exactly-once, so the buffered design is rejected.

### 9.5 Calling the SDK from handlers and callbacks

The rules, in one table (also in the crate docs):

| Where | Allowed | Fails fast with `WouldDeadlock` | Notes |
|---|---|---|---|
| Any task or thread outside handlers | everything | — | Sync methods never block. Async methods only wait for the network. |
| Event handler: sync methods (`connect`, `disconnect`, `subscribe`, `unsubscribe`, `new_subscription`, `remove_subscription`, `set_token`, getters) | yes | — | They're applied right after the handler's batch. `state()` reflects them at once (projected state, §9.2). |
| Event handler: fast-path calls (`publish`, `rpc`, `history`, `presence`, `presence_stats`, `send`) while connected / subscribed | yes, also blocked on (`Handle::block_on`, `futures::executor::block_on`) with `EventDelivery::Thread` | with `EventDelivery::Inline`, when blocked on | Replies are resolved by the reader, not by the loop (§9.3). Prefer `tokio::spawn` anyway, so later events aren't held up. |
| Event handler: anything that needs the loop to progress: `ready()`, calls while connecting or subscribing, waiting for `close()` to finish | not when blocked on inside the handler | yes | The loop waits for this handler, so such a wait could never end. `close()` starts closing and returns; it waits only outside handlers. |
| Event handler of client A calling client B | everything | — | Different loops. |
| `get_token` / `get_state` (async, own task) | everything | calls that would wait for the connection this callback is part of, e.g. `rpc()` from `get_token` during the initial connect | Detected with a Tokio `task_local!` set on the callback's task. During a refresh (connected) calls work. |
| `Drop` of the last handle | — | — | Never blocks. It starts a close in the background. |

How it's detected: the dispatcher sets a thread-local (Thread mode), or a task-local (Inline mode), with the client's id while a handler runs. Callback tasks carry a task-local with the client id and the flow. An SDK future first checks:
- whether it's being polled under that marker for its own client, and
- whether it would have to wait for the loop.

If both hold, it returns `Err(WouldDeadlock)` on its first poll. The check costs one thread-local read.

**Handlers that need async work** spawn it. Order between spawned tasks isn't guaranteed. When order matters, forward to one consumer task:

```rust
let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
sub.on_publication(move |_sub, e| { let _ = tx.send(e); });
tokio::spawn(async move { while let Some(e) = rx.recv().await { store(e).await; } });
// or simply: let mut pubs = sub.publications(); while let Some(e) = pubs.next().await { .. }
```

**Async handlers aren't offered.** The loop would have to await them before the next step, and awaiting an SDK call that needs the loop inside one would deadlock with no way to detect it. Spawning (or a stream) keeps the guarantee explicit.

**Blocking facade** (phase 2, `blocking` feature): `centrifuge::blocking::Client` owns a runtime, like `reqwest::blocking`, for non-async apps. It works because handlers run on their own thread and fast-path calls don't depend on the loop.

### 9.6 Transport abstraction

```rust
trait Transport: MaybeSend {
    fn kind(&self) -> TransportKind;               // WebSocket | HttpStream
    fn uses_emulation(&self) -> bool;              // true: connect goes in the open request
    /// Open the transport. For emulation, `initial` is the encoded connect command.
    async fn open(cfg: &TransportConfig, initial: Option<Bytes>) -> Result<Opened, TransportError>;
}
struct Opened {
    inbound: mpsc::Receiver<Inbound>,              // bounded (§9.2 backpressure): decoded items, then a final Closed
    writer: Box<dyn FrameWriter>,                  // WebSocket sink, or the in-order emulation poster
}
enum Inbound { Messages(Vec<Reply>), Closed { code: Option<u16>, reason: String } }
```

- Phase 1 has two implementations: WebSocket (tokio-tungstenite) and HTTP-streaming (hyper).
- The core never touches sockets, and the driver only through this trait. It sees opens, message batches, a single `Closed`, and write outcomes, all tagged with `transport_id`.
- For emulation, `FrameWriter` needs the session and node. The driver and the call fast path pass them with each frame (`OutgoingFrame { data, call_ids, session, node }`), so the writer stays stateless about the connection.
- A write outcome comes back to the driver (and to the call table for call frames). A failed write fails exactly the calls in that frame (pattern 41), and for emulation may close the transport (§5.7).
- The trait also lets tests use an in-memory transport, and keeps a wasm or WebTransport implementation possible later. It stays internal.

### 9.7 Timers

- Timers are deadlines in the core, each carrying the counters it was armed under. The driver sleeps until the earliest one.
- Command timeouts, the no-ping timer and the handshake timeout are checked with inbound items polled first in the driver's `select!`. Call timeouts live in the call future (§9.3). A reply already received always wins over its timeout (pattern 28).
- A timer that fires more than 1 s late (the process was suspended) gets a 1 s grace before failing (`centrifuge-js` `lateCallTimer`/`lateCallGrace`; patterns 28, 55).
- Cancelling a timer and replacing it is one operation. A late fire is recognised by its counters (pattern 80).
- **The clock is Tokio's monotonic `Instant`.** On macOS and Linux it doesn't advance while the system sleeps, so a laptop wake looks like no time passed. The driver also tracks `SystemTime`. When wall time jumped well past monotonic time since the last check, the wake is handled like a late timer:
  - it gets the grace
  - token expiry (kept as wall time, §6.6) is re-checked
  - the no-ping window restarts from the wake, so a connection that died during sleep is found within one ping interval (centrifuge-swift "uptime" finding)
  - a sleep at least as long as the no-ping window (the server's ping interval plus `max_server_ping_delay`) reconnects at once: the server has dropped a connection that missed its pongs that long, and the old socket may be stuck on a network that's gone

### 9.8 Lifetime and resource cleanup

- The driver runs until `close()` or until every `Client` and `Subscription` handle the app holds is dropped. The registry's own entries don't count: the driver owns subscription state, and a `Subscription` handle is a reference to it. Dropping a `Subscription` handle doesn't unsubscribe; the subscription stays registered and is reachable with `get_subscription()`. Holding only a `Subscription` keeps its client alive. Then it performs close: it ends the connection, cancels timers and tasks, stops the dispatcher thread, and drops handlers.
- Handlers receive the handle as an argument, so they don't need to capture one. A handler that does capture a `Client` keeps it alive (a reference cycle through the driver). The docs say to use the argument, or `WeakClient`, or call `close()` explicitly.
- After `disconnect()`: no open socket, no running reconnect, refresh or ping timer, and no `get_token` calls (invariant I5).
- `Drop` never blocks.
- `close()` never joins the dispatcher thread from that thread itself. When the last handle is dropped inside a handler, the thread is detached and ends after that handler returns (patterns 1, 43).

### 9.9 Logging

`tracing` spans per client (`client_id`, after connect) and per subscription (`channel`). Frames are traced at `TRACE` with payloads redacted unless a `trace_payloads` config flag is set. There's no `println!`.

### 9.10 Outgoing command batching

Commands are batched automatically. The app never asks for it, and nothing is held back to wait for more.

- **Per transport, one writer task** owns the outgoing queue. When it's ready to write, it takes **everything queued at that moment** and writes it as one frame: newline-joined for JSON, varint-delimited for Protobuf. Over emulation it becomes one request (§5.7). An idle client sends each command at once. A busy one forms larger frames by itself, because commands pile up while the previous write is in progress.
- **A burst of calls, one frame.** In each loop iteration, the driver takes every input that's **already waiting** (up to 64) before it flushes writes. Steps and their event deliveries still run one at a time and in order. The wake-up of the writer is deferred to the end of the iteration, or happens earlier, when a step's events are handed to the dispatcher (§9.2 step 4), so a slow handler never holds commands back. So a burst of `subscribe()` / `unsubscribe()` calls becomes one frame without any timer, and nothing that's already waiting is delayed. Fast-path calls such as `publish()` skip the driver and share frames through the writer queue, as described next.
- **One step, one frame.** The driver queues every command a core step produced before waking the writer. A connect reply that resubscribes 100 subscriptions sends them in one frame. This is centrifuge-csharp's "the commands a step queued go as one frame".
- **Fast-path calls** (§9.3) go into the same queue, so concurrent `publish()` calls from many tasks share frames too.
- **Frame cap:** 15 KB, as in centrifuge-csharp. A batch that would exceed it is split, and a single larger command goes alone. This keeps emulation requests well under the server's request-body limit, and keeps one frame from delaying a pong behind a large write.
- **Order** is the queue order, over both transports (emulation writes one request at a time, §5.7).
- **Pongs** are queued like any command. They're tiny and the queue is short, so they don't need a priority lane.
- **Why no delay timer:** centrifuge-csharp waits 1 ms before flushing (`CommandBatchDelayMs`) so commands from separate steps can join. Here the writer is a separate task, so commands queued while it's busy or not yet scheduled already join the next frame. A fixed delay would add latency to every command of an idle client and only help a narrow case. If benchmarks show a need, a `batch_delay` option (default 0) can be added without changing behaviour for anyone else.

### 9.11 Designed for future work

Later features must arrive as **additive** changes: no breaking release and no redesign. Phase 1 builds in the following for each.

| Future feature | Phase | What phase 1 already provides |
|---|---|---|
| Browser / wasm32 | 3 | The core is its own crate with no IO, runtime or clock, and CI builds it for `wasm32-unknown-unknown`. Every public `Send`/`Sync` bound is `MaybeSend`/`MaybeSync`, which are `Send`/`Sync` natively and implemented for all types on wasm32, so no signature changes. `EventDelivery::Inline` already exists (the only mode without threads). The `Transport` trait is internal, so a `web-sys` WebSocket/fetch transport and a `wasm-bindgen-futures` driver slot in. Options with no browser meaning (`proxy`, `tls`, `http_headers`) become configuration errors there, and `emulated_headers` covers headers. |
| Blocking facade | 2 | Handlers run on their own thread, and fast-path calls don't depend on the loop (§9.3, §9.5). The facade is a wrapper module behind a feature. |
| Fallback reset | 2 | The core owns the transport list and the fallback state (§5.8). The reset is a rule inside the existing `connect` and reconnect steps. |
| Map subscriptions, shared poll | 3 | The generated proto already carries their fields (`type`, `phase`, `cursor`, `state`, `track`, `untrack`). Core subscriptions have an internal `kind` (stream today). Map and shared poll get their own public handle types (`MapSubscription`, ...) from new builder methods, so `Subscription` doesn't change. Event enums are `#[non_exhaustive]`, so `update`/`sync` events are additive. |
| Client publish debouncing | 3 | The writer queue is owned per transport (§9.10). The core keeps `publish_debounce` from the subscribe reply (proto field 19), unused until then. |
| Manual batching API | 3 | Batching is already centralized in the writer (§9.10). A manual mode only holds its flush. |
| Optimistic subscriptions | 3 | Sending subscribes before the connect reply is a core-only change. The TLA+ model already allows it over any transport. |
| More transports (WebTransport, SSE) | on demand | `TransportKind` is `#[non_exhaustive]`, and the fallback list and the `Transport` trait are transport-agnostic. |
| New server fields and features | ongoing | Unknown proto fields are ignored. Public structs are `#[non_exhaustive]` or builder-only. Options are builder methods. |
| Model-based testing against TLA+ | 1 (done) | The core is pure and deterministic, so traces replay into it directly (§11.1). |

General rules that keep this true:
- No public struct with public fields that might grow, unless it's `#[non_exhaustive]`.
- No public trait users implement, unless it's sealed or deliberately an extension point.
- No dependency in `centrifuge-client-core` that does IO or needs a runtime.
- No hard `Send`/`Sync` in public bounds: use `MaybeSend`/`MaybeSync`.
- A new feature that needs a breaking change gets a spec note and an explicit decision first.

## 10. Codes and constants (public)

`centrifuge::codes` exports the client-side codes from §6.5 and the server codes the SDK interprets (`TOKEN_EXPIRED = 109`, `UNRECOVERABLE_POSITION = 112`, `UNSUBSCRIBE_STATE_INVALIDATED = 2502`, `DISCONNECT_STATE_INVALIDATED = 3014`), plus `is_reconnect_code(u32) -> bool`.

## 11. Testing and verification

### 11.1 Unit tests

- **Core (sans-IO):** table-driven tests feed `Core::step` inputs with a fake clock and assert on events, frames, timers and callbacks. They need no runtime, no sockets and no sleeps. Every bug pattern from §12 that the core can express gets a core-level test.
- **Model-based (done in phase 1):** `cargo xtask tla-traces` simulates the SDK TLA+ model (the stream-subscription `MC_*` configs) under `tests/tla/Replay.tla`, which only narrows the generated schedules to the ones this SDK implements (flows start eagerly once connected, queued deliveries first, no optimistic subscribes). `machine/replay.rs` replays every trace into the core step by step and checks states, events (with publication offsets and payloads, so a wrong delta base shows), commands, callbacks and timers. TLC's counterexample of each relevant `bug_*` config is replayed too and must diverge, at the bug's step. The traces are committed, so CI runs this without the model.
- **Dispatcher and fast path:** `WouldDeadlock` detection for every row of the §9.5 table, in both `EventDelivery` modes and on both runtime flavours (`current_thread`, `multi_thread`).

- Codecs:
  - Golden JSON and Protobuf frames shared with `centrifugal/protocol/testdata`.
  - Batch splitting, empty lines, truncated varints, size limits.
  - Strict field naming (round-trip against the server's expectations).
- Backoff bounds, the reconnect code rule, close-frame parsing (JSON reason and plain).
- Fossil delta: fdelta vectors, plus a `cargo fuzz` target.
- **Benchmarks** (`criterion`, in `benches/`): JSON and Protobuf decode of single and batched replies, encode of commands and batches, byte-stream reassembly of split frames, and fossil delta apply.
  - CI runs them on PRs and compares against `main`. A regression beyond 10% in a benchmark is reported as a PR comment, not a failure, because shared runners are noisy.
  - Results on `main` are kept as artifacts to track trends.

### 11.2 Fake-server tests

**The fake server** is a protocol-level, in-process Centrifugo stand-in, in the shape of `centrifuge-go`'s `FakeServer` (`fakeserver_test.go`) and `centrifuge-js`'s `FakeCentrifugoServer` (`src/fakeServer.ts`), extended with the controls `sdk_validation` (`SKILL.md` §4, "Fake server") needs.
- It lives in the workspace crate `crates/centrifuge-fake-server` (`publish = false`). The SDK's own tests and a Rust `sdk_validation` harness use the same code.
- It's built on `tokio` + `hyper` + `tokio-tungstenite` and speaks both codecs over both transports: WebSocket, and HTTP-streaming with the `/emulation` endpoint.
- It's generic on purpose: sensible defaults for the handshake, every received command captured for assertions, and hooks plus raw senders, so a new scenario never needs changes to the client under test or to the fake.

```rust
let srv = FakeServer::start(FakeConfig::default()).await;       // random local port
srv.ws_url(); srv.http_stream_url();                            // endpoints for Client::builder*

// Defaults, overridable
srv.set_connect_result(ConnectResult { client: "fake".into(), ping: 25, pong: true, ..Default::default() });
srv.on_subscribe(|channel, req| SubscribeResult { recoverable: true, epoch: "e1".into(), ..Default::default() });
srv.on_command(|cmd| -> Option<Reply> { /* inject errors (100 temporary, 109, 112, 101 …), or None to fall through */ });

// Server-initiated traffic
srv.publish("news", data); srv.publish_id(42, data);            // by channel, or by compaction id
srv.join(ch, info); srv.leave(ch, info); srv.message(data); srv.refresh_push(expires, ttl);
srv.unsubscribe_push(ch, code, reason); srv.subscribe_push(ch, sub);   // server-side subscriptions
srv.disconnect(code, reason);                                    // push, then close
srv.send_frame(vec![reply_a, push_b, ping]);                     // several items in ONE frame (adversarial orders)
srv.send_raw(bytes);                                             // undecodable data, HTML bodies, split frames

// Transport faults
srv.close_connection(); srv.terminate();                         // graceful close vs abrupt drop
srv.close_with(code, reason);                                     // WebSocket close frame (e.g. 3014, 3500)
srv.hold_replies(true);                                          // silent server: accept, never reply
srv.pause_reads(Duration);                                       // stop reading the client (full send buffer)
srv.refuse_upgrade(StatusCode) / srv.hang_upgrade(true);         // fallback triggers
srv.emulation_status(StatusCode) / srv.hang_emulation(true);     // emulation endpoint failures
srv.split_writes(1);                                             // write every frame in 1-byte chunks (byte-stream reassembly)
srv.stale_close_delay(Duration);                                 // close a connection that sends no connect in time

// Assertions
srv.received();                                                  // every command, in order, with its transport
srv.last_subscribe(); srv.wait_for(|cmd| …).await;
srv.connection_count();                                          // open transports (invariant I3)
srv.connects();                                                  // connect commands seen
```

Its behaviour follows the real server where tests rely on it, checked against the `centrifuge` source:
- the connect command must come first
- the unsubscribe push of a subscribe in progress goes after its reply
- the disconnect push is the last frame
- recovery offsets
- ping/pong timing

The scenario families come from `centrifuge-go`'s tests:
- connect/reconnect, refresh, subscription refresh
- server-side subscriptions, recovery, delta, compaction, filter
- `get_state`, state invalidation, queue ordering

Also add the adversarial frame orders from `SKILL.md` §3 ("Late and out-of-state frames").

Every scenario runs over both transports and both codecs. Fallback-specific cases (`SKILL.md` §6, "Transport fallback matrix"):
- WebSocket upgrade refused, hanging or timing out → falls back to HTTP-streaming with no delay in the first round
- a slow first connect reply over a WebSocket that opened → no fallback
- a WebSocket that upgrades, then sends garbage → falls back (§5.8)
- a connect proxy returning a temporary error → no zero-delay loop
- a broken emulation endpoint (404, 5xx, hang) → reconnect with growing backoff
- replies split at every byte offset, including inside varint prefixes
- 200 with an HTML body
- blank keep-alive lines
- a final disconnect push right before EOF

Time-dependent tests use `tokio::time::pause()` so they're deterministic.

### 11.3 Seeded chaos

A seeded random-operations runner against the fake server: random `connect`, `disconnect`, `subscribe`, `unsubscribe`, `remove_subscription`, calls, handler re-entrancy, server-side faults. It checks invariants I1–I8 (I1–I7 from `SKILL.md`, I8 added here) continuously and at quiescent points:

| Id | Invariant |
|---|---|
| I1 | State chain is consistent; `state()` inside a handler matches the event, unless the handler itself issued a sync op, in which case it matches that op's projected state. |
| I2 | No panics. |
| I3 | At most one open server connection per client at quiescence. |
| I4 | Liveness: after healing, `connected` within 3 s, and every non-unsubscribed subscription `subscribed` within 3 s. The chaos config lowers `min_reconnect_delay` / `max_reconnect_delay` (and the resubscribe delays) to 50 ms / 500 ms for this. |
| I5 | Cleanup after `disconnect()`: no connections, timers or `get_token` calls. |
| I6 | Every future settles within its timeout plus a margin. |
| I7 | No duplicate `connected`/`subscribed` events without a state change in between. |
| I8 | **Exactly once:** per recoverable channel, the offsets handed to the app continue without gap or duplicate across reconnects, resubscribes and `unsubscribe()`/`subscribe()`. They may restart only after `subscribed` with `recovered = false` (or a `get_state` reload). Checked with a server that publishes numbered messages, under chaos, in both handler and stream modes. |

**TLA+ properties and what keeps them.** Each property of the SDK model (`tla/sdk` README) is enforced by named rules and checked by tests:

| TLA+ property | Spec rules | Tests |
|---|---|---|
| `LegalEvents` | One event per state change, none without one (§6.1, §7.1); event order inside a step (§6.1) | I1, I7; model-based replay (§11.1) |
| `ExactlyOnce` | Commit on delivery, reply position taken at once and never moved backward, dedupe of live publications on the committed offset, other-stream publications ignored (§7.4, §9.4) | I8 |
| `NoStalePublication` | `transport_id`, `sub_session`, delivery only while subscribed (§7.4, §9.2) | I8, late-frame probes (§11.2) |
| `DeltaBase` | Base reset per subscribe, first publication full, negotiated decoding (§7.5) | delta chaos, fdelta vectors |
| `NoSubscribeOfSubscribedChannel` | Per-channel unsubscribe bookkeeping, unsubscribe deferral (§7.3) | replace/toggle probes; no 105 in real-server runs |
| `ConsistentAtQuiescence` | Teardown resets bookkeeping (§7.1); unsubscribe failures reconnect (§7.3); flows always settle (§7.2) | I3, I4 |
| `NoStuckConnecting`, `ConnectingEnds` | Every connect step ends in a timer, a callback or a transport in progress; callbacks always settle (§6.2, §7.2) | I4, I6 |
| `SubscribingEnds` | Invalidation restarts a flow; teardown clears `inflight` (§7.1, §7.2) | I4 |
| `NoWaitersWhenDisconnected` | Relaxed on purpose: calls made while disconnected wait for the next connect (TLA+ decision, §9.3) | I6, pattern 8 test |
| `ClosedIsFinal` | After `close()` nothing is sent or acted on (§7.1) | I5 |

Run it in CI with a fixed seed set (e.g. 300 seeds × 60 ops), plus a longer nightly run.

### 11.4 Real-server tests

- **`docker-compose.yml` pins Centrifugo `v6.10.2`**, configured as in `sdk_validation` §5. Configs are in `tests/centrifugo/`, checked with `centrifugo checkconfig`. Host ports are 8010 and 8012, so the stack runs alongside other SDKs' stacks on 8000/8002.
  - **`centrifugo`** (insecure, `:8010`):
    - HTTP API key `test-api-key`, HMAC secret `secret` (connection tokens with a `channels` claim give server-side subscriptions)
    - `http_stream` enabled
    - presence, join/leave (forced) and fossil delta on the default namespace
    - namespaces `recovery` (history 100), `smallhistory` (history 2, for error 112 / unrecoverable positions), `positioned`, `delta`, `tagged` (`allow_tags_filter`), `nohistory`
  - **`centrifugo_secure`** (`:8012`):
    - connection tokens required
    - subscription tokens required in `private`
    - `expired_close_delay` and `expired_sub_close_delay` of 3 s, for token expiry and refresh
  - Map and shared-poll namespaces join when those subscription types do (phase 3).
  - Conditions a stock config can't produce (an epoch change, history TTL expiry, Centrifugo PRO compaction) are covered by the fake server, or reported as not exercised.
- Integration tests run in CI against it for JSON and Protobuf.
- A chaos TCP proxy (latency, drops, half-open) for the delivery runner, ported from `sdk_validation`.
- `http_stream.enabled: true` on the test server. CI runs the integration suite over WebSocket only, over HTTP-streaming only, and with fallback where the WebSocket port is blocked by the proxy.

### 11.5 Pinning fixes

Each bug fix gets a test that fails before the fix and passes after, run both ways (`SKILL.md` principle 4).

### 11.6 CI and releases

Everything runs in GitHub Actions. Nothing is released from a laptop.

**`ci.yml`** runs on PRs, on `main`, in the merge queue, and weekly:
- `rustfmt`; `clippy --all-targets --all-features -D warnings` with `clippy::pedantic`.
- Tests on Linux, macOS and Windows (stable), plus beta, which doesn't block merges.
- MSRV `cargo check` with the `rust-version` read from `Cargo.toml`.
- `cargo hack --feature-powerset`, and `cargo minimal-versions` (the lower bounds in `Cargo.toml` must build).
- Rustdoc on nightly with `--cfg docsrs -D warnings`.
- `centrifuge-client-core` builds for `wasm32-unknown-unknown` (§9.11).
- Coverage (`cargo-llvm-cov` → Codecov via OIDC).
- `cargo-deny`: advisories, licenses, bans, sources.
- `typos`, and `zizmor` for workflow security.
- One aggregated `CI success` check for branch protection.

Integration jobs against a real Centrifugo (§11.4) and the seeded chaos runs (§11.3) join this file when the code they test exists.

**`release.yml`**: a pushed `vX.Y.Z` tag publishes.
- The version is bumped beforehand in a normal commit on `main`. The workflow checks that the tag names it, that the commit is on `main` and passed CI, and that the bump matches the API change (`cargo-semver-checks`).
- It publishes the core, then the client, to crates.io via **trusted publishing** (OIDC, no stored token) from the protected `release` environment, and creates the GitHub release with generated notes, unless the maintainer created it.
- Release notes live on GitHub releases, as in the other Centrifugal SDKs.

**`pr-title.yml`**: PR titles must be conventional commits; squash merges make them the commit messages.

**Supply chain:**
- Every action is pinned to a full commit SHA, and Dependabot bumps actions and the lock file weekly, with a 7-day cooldown.
- `Cargo.lock` is committed.
- Workflows default to read-only permissions, credentials aren't persisted, and the release job uses no build cache.

The one-time setup (first manual publish, crates.io trusted publisher, `release` environment, tag and branch protection) is in `CONTRIBUTING.md`.

## 12. Conformance checklist (bug catalogue)

The implementation must handle each pattern from `sdk_validation/SKILL.md` that applies to a WebSocket or HTTP-streaming client on Tokio. Patterns that don't apply in phase 1:
- **SSE-specific part of 12.** Its generic form (one fallback advance per failed transport) applies (§5.8).
- **Network-event-specific: 2, 3, 10, 17.** The SDK has no network listeners (§13), so they don't apply. Re-check them if network-change handling is ever added.
- **Map-subscription-specific: 20.** Re-check when map subscriptions arrive (§9.11).

Review the rest again whenever a transport or a subscription kind is added. The validation findings of each SDK (`sdk_validation/*/FINDINGS.md`) were reviewed against this spec on 2026-10-26. The rules they added name their source inline.

| Patterns | Where it's handled |
|---|---|
| 1, 43 | No reference cycles through handlers (handle passed in); `close()` never joins the dispatcher from itself; dispatcher thread detached when the last handle drops in a handler (§9.4, §9.8) |
| 4, 5 | Transport creation failure is an attempt failure, advanced once for fallback; dropping the open future aborts it (§5.1, §5.8) |
| 6, 7, 11, 35, 46, 49, 50 | Generation counters on every continuation (§9.2); a step completes before its events are delivered (§6.2, §9.4) |
| 8 | Calls fail fast only where TLA+ says so; calls from a `disconnected` handler wait for the next connect by design (§9.3) |
| 9, 42 | Refreshes scoped to `conn_generation` / `sub_session` (§6.6, §7.6) |
| 14 | Close and read errors of a transport the SDK closed are dropped by `transport_id` and never emit `error` (§6.5) |
| 15, 16, 18, 21, 56, 68, 81 | Serial inbound items, transport tags, pending deliveries before inbound, commit on delivery, position never moved backward (§7.4, §9.2, §9.4) |
| 19, 70 | Panics caught in handlers, the `error` handler, callbacks and the core step; reader/writer `JoinError` is terminal and visible (§7.2, §9.2, §9.4) |
| 22, 57, 58, 82, 36 | Token and state invalidation rules (§6.5, §6.6, §7.1, §7.2, §7.7) |
| 24, 25, 71, 72, 73 | Byte-stream reassembly with growable buffer and caps, JSON line cap, blank lines skipped, decode errors before the first message are transport failures, synchronous teardown (§5.3, §5.7) |
| 28, 55, 80 | Inbound before timers, last-received instant, late-timer grace, wake detection, counters on timers (§6.4, §9.7) |
| 29 | Pong only on the current transport (§6.4) |
| 30 | The token step runs on every subscribe path (§7.2) |
| 31, 48, 61 | Copies for iteration; the core owns its collections; registry is a leaf-locked map (§4.3, §9.2) |
| 32 | Handler slot taken while running, so registering from inside a handler is safe (§9.4) |
| 33, 37, 44, 79 | Subscribe attempt ids; per-channel unsubscribe bookkeeping; write → emit → await; unsubscribe failures reconnect only within their session (§7.3) |
| 34, 51, 52, 53 | Handshake timeout and abort; close never waits for the peer; only SDK-owned resources are closed (§5.1) |
| 38 | One dispatcher for all events (§9.4) |
| 39, 40, 45 | No locks in the core; callbacks in tasks; handlers on the dispatcher; self-waits fail with `WouldDeadlock` (§9.2–§9.5) |
| 41 | A call completes exactly once: a `oneshot` removed from the call table (§9.3, §9.6) |
| 47 | Close is handled before pending calls fail (§6.5, §9.3) |
| 54 | App callback errors are their own types (`TokenError`), never confused with SDK timeouts (§4.2) |
| 59, 60 | Remove checks identity; the registry entry goes before `remove_subscription()` returns; streams end after the cleanup unsubscribe (§4.3, §9.2, §9.4) |
| 62, 75, 76 | Waiters catch up with `applied_seq`, are released after their event's handler, and are removed on timeout or drop (§4.3, §9.2, §9.4) |
| 63, 74 | Configuration validated up front; zero durations rejected (§4.1, §4.2) |
| 64 | `Stream` adapters follow the `futures` contract: fused, cancel-safe `next()`, one active stream per kind (§9.4) |
| 65 | Server-side routing rule (§8) |
| 66, 13 | Open bounded by `handshake_timeout`; fallback proof rules; zero-delay round only for transport failures (§5.8) |
| 67 | Backoff resets only after a ping or command reply, on every transport (§6.2, §6.3) |
| 69 | Decode by the negotiated delta, not the requested one (§7.5) |
| 77, 78 | Position rules (§7.4) |
| 23, 26, 27, 83 | Emulation requests: in-order writer with per-request deadline, aborted on close, 404/5xx/401/403/3xx close the transport (§5.7) |

## 13. Desktop apps (Tauri)

The target setup: the Rust backend owns the connection for the life of the process, and the webview UI is just one consumer of it.

- **Lifetime.** The app creates the `Client` at startup (e.g. in Tauri `setup`, stored in managed state) and closes it on app exit, never on window close. Nothing in the SDK depends on a window, an event loop other than Tokio, or the main thread. Tauri's async runtime is Tokio, so `Client::builder(url).build()` works inside it (pass `.runtime(tauri::async_runtime::handle())` from setup hooks).
- **Bridging to the UI.** Event handlers forward events to the webview with `app_handle.emit(...)`. When no window exists they act on the data directly, e.g. with tray notifications. The UI calls the SDK through Tauri commands (`publish`, `rpc`, `history`). An example `examples/tauri` shows the pattern. It forwards publications to the webview as `e.json::<Box<serde_json::value::RawValue>>()`, which passes the JSON through without parsing it into Rust types (one copy into the box, no re-serialization). Emitting the `Bytes` directly would arrive in JavaScript as an array of numbers. It uses the handle passed to handlers rather than capturing one (§9.4), and it re-sends the current state to a newly opened window (`client.state()`, `subscriptions()`), because the window missed earlier events.
- **Sleep/wake and network changes.** Laptops suspend and switch networks. After a resume, the no-ping timer must not tear down a connection whose pings are already buffered (§9.7). A dead connection is found by the no-ping timer, as in centrifuge-go.
  - **No network-change API, on purpose.** After a silent network loss (no FIN or RST reaches the socket), any native client recovers only through timers. That's the no-ping timeout (about 35 s with Centrifugo's 25 s ping and the 10 s `max_server_ping_delay`), then the backoff. Neither std nor Tokio offers network-change events, the way browsers hand centrifuge-js `online`.
  - An app with its own signal (OS notifications, a "Retry" button) calls `disconnect(); connect()` to reconnect at once. It sees `disconnected` and `connecting` events, and calls waiting for a connection fail with `ClientDisconnected`. In phase 2 this also returns the client to WebSocket after a fallback (§5.8).
  - Apps that want faster detection lower `max_server_ping_delay`, or the server's ping interval.
  - A dedicated method (a reconnect without the `disconnected` event) or SDK-side OS network watching can be added later as additive changes, if users ask.
- **Auth headers.** If the webview setup used centrifuge-js `headers` (emulated) with a connect proxy, set the same map as `emulated_headers` and the server config stays unchanged. Switching to real `http_headers` needs the names moved from `emulated_headers` to `http_headers` in the Centrifugo config. In exchange, a gateway in front of Centrifugo can check them. A webview that still connects with centrifuge-js keeps the browser limitation and must keep using emulation.
- **Corporate networks.** Proxies and TLS trust are covered by §5.2. The default `ProxyConfig::System` reads env vars and then the macOS/Windows system proxy settings. That matters because GUI apps on macOS usually don't get the shell's env vars. Apps with their own proxy settings UI pass an explicit `ProxyConfig`. Some corporate proxies and firewalls block WebSocket upgrades but allow plain HTTPS. Configuring `[websocket, http_stream]` fallback (§5.8) covers them without app code.
- **Moving from centrifuge-js.** The JS features not in this SDK's phases 1–2 are map subscriptions, shared poll, the manual batching API (automatic batching is built in), client publish debouncing and optimistic subscriptions, plus SSE, SockJS and WebTransport (HTTP-streaming fallback is supported). An app that uses them should keep those channels in the webview, or wait for phase 3. Everything else in the feature matrix is covered. JSON payloads are byte-identical between the two SDKs, so the UI can keep its message types.

## 14. Documentation and examples

- A README with quick start, feature matrix row and links to centrifugal.dev.
- `examples/`: `quickstart`, `desktop_app` (the §13 patterns, without a GUI dependency), `chat` (typed JSON, subscriptions, publish), `protobuf` (binary payloads), `token_refresh` (`get_token` against a backend), `server_side_subscriptions`, `streams`, `recovery` (`since` across restarts), `get_state` (app-owned state, reloaded when recovery fails), `delta` (Fossil delta, bytes saved).
- Rustdoc on every public item, with the state diagrams and code tables from §6–§7.
- After release, add a Rust column to the feature matrix in `centrifugal.dev/docs/transports/client_sdk.md`, and Rust tabs to `client_api.md`.

## 15. Decisions

Settled with the maintainers, 2026-10-26:

| # | Decision | Where |
|---|---|---|
| 1 | Behaviour parity with centrifuge-js comes first. Rust idiom only where it changes no behaviour; intentional differences are listed. | §1, §1.1 |
| 2 | Crate `centrifuge-client`, lib name `centrifuge`, repo `centrifuge-rust` | §3.1 |
| 3 | Tokio only. async-std is discontinued; wasm later gets its own driver over the same core. | §3.2 |
| 4 | Protobuf is the default protocol; JSON is supported. Payloads are `Bytes` in both, with serde helpers. | §4.2, §4.6 |
| 5 | Non-waiting `connect`/`subscribe` + `ready(Option<Duration>)`, as centrifuge-js, for batching | §4.3 |
| 6 | Sans-IO core, one driver per client, handlers on a dedicated thread by default (`Inline` opt-in) | §9 |
| 7 | Calls made inside a handler take effect when it returns | §1.1, §9.4 |
| 8 | Handlers block the read loop; read-ahead is bounded (256 items / 4 MiB) | §9.2 |
| 9 | Handler panics stop the client (bad protocol, no reconnect) | §9.4 |
| 10 | Streams use hand-off delivery to keep exactly-once | §9.4 |
| 11 | Automatic batching, no delay timer | §9.10 |
| 12 | `http_headers` and `emulated_headers`; no option called `headers` | §5.1 |
| 13 | `set_data()`; the deprecated `getData` isn't offered | §4.3 |
| 14 | HTTP-streaming fallback in phase 1; fallback reset in phase 2 | §5.8 |
| 15 | Proxy auth: Basic and SOCKS5 user/pass only; NTLM/Kerberos on demand | §5.2 |
| 16 | `Option<String>` connection tokens, builder-time handlers, `test-util`, history options builder; JS handler names kept | §4 |
| 17 | Reconnect backoff resets after the first ping or command reply on every transport | §6.2, §1.1 |
| 18 | `refresh` push adjusts only an existing client-side refresh schedule | §6.6 |
| 19 | No `network_changed()` / `force_reconnect()`: apps call `disconnect(); connect()`, which in phase 2 also resets the fallback | §5.8, §13 |
| 20 | Built for future work: `centrifuge-client-core` crate (wasm-checked), `MaybeSend`/`MaybeSync` bounds, the provisions listed in §9.11 | §3.1, §9.11 |

Settled during implementation, 2026-10-27 (reviewed one by one):

| # | Decision | Where |
|---|---|---|
| 21 | The unsubscribe deferral applies over emulation only; over WebSocket the unsubscribe goes at once and a later subscribe doesn't wait (TLA+ `Ordered`, centrifuge-js #443) | §7.3 |
| 22 | `unsubscribe()` while the flow waits for `get_token`/`get_state` sends an unsubscribe command (centrifuge-js `_inflight`, TLA+ `NeedsUnsubCmd`) | §7.3 |
| 23 | A disconnect push is honoured while connecting, and proves the transport (HTTP-streaming reports a refused connect this way); catalogue pattern 84 | §6.5 |
| 24 | A 200 HTML response to an HTTP-stream request is a transport failure (captive portal), retried | §5.3 |
| 25 | HTTP-streaming over HTTP/1.1 only | §5.7 |
| 26 | TLS through the OS trust store only (`rustls-platform-verifier`), extra roots via `add_root_pem`; no bundled roots | §5.2 |
| 27 | Error events carry `Arc<dyn Error + Send + Sync>`, so all events are `Clone` | §4.7 |
| 28 | A panicking handler stops the client visibly; the batch's other events are delivered; its publication isn't committed | §9.4 |
| 29 | Ops after `close()` are dropped at the handle; `close().await` in a handler returns at once | §4.3 |
| 30 | `Client::builder(url)` is infallible; `build()` validates | §4.1 |
| 31 | Emulation POSTs: 404/401/403/3xx/5xx close the transport; other non-2xx fail that request's calls with `Error::Transport` | §5.7 |
| 32 | A `Subscription` keeps its `Client` alive (strong reference; no cycle through handlers) | §9.8 |
| 33 | The TLA+ replay narrows the model's schedules to the implemented ones (`tests/tla/Replay.tla`); the narrowed spec is model-checked exhaustively (`cargo xtask tla-check`; `MC_large` and `MC_large_emulation` only when named, as their state space outgrows 100 GB of disk) | §11.1 |
| 34 | A system sleep longer than the no-ping window reconnects at once | §9.7 |
| 35 | SOCKS5 with local DNS tries every resolved address | §5.2 |
| 36 | Benchmark comparisons go to the PR job summary, not a comment | §11.1 |
| 37 | Centrifugo PRO (channel compaction) tests run locally only, not in CI | §11.4 |
| 38 | The first real release is 0.1.0 | §2 |

Still open:
- Whether the Tauri users' proxies need NTLM or Kerberos (#15).

## 16. References

Technology research, 2026-10-26:

- crates.io API (versions, MSRV and features of `tokio`, `hyper`, `hyper-util`, `tokio-tungstenite`, `tokio-websockets`, `rustls`, `rustls-platform-verifier`, `prost`, `turmoil`; name availability)
- [Actors with Tokio (Alice Ryhl)](https://ryhl.io/blog/actors-with-tokio/)
- [Rust API Guidelines checklist](https://rust-lang.github.io/api-guidelines/checklist.html)
- [tokio-tungstenite (lib.rs)](https://lib.rs/crates/tokio-tungstenite)
- [Rust WebSocket guide (websocket.org)](https://websocket.org/guides/languages/rust/)
- [rustls-native-certs README (recommends the platform verifier)](https://github.com/rustls/rustls-native-certs)
- [rustls-platform-verifier docs](https://docs.rs/rustls-platform-verifier/=0.7.1/)
- [hyper-util proxy Matcher](https://docs.rs/hyper-util/latest/hyper_util/client/proxy/matcher/struct.Matcher.html)
- [hyper-util SOCKSv5 connector](https://doc.servo.org/src/hyper_util/client/legacy/connect/proxy/socks/v5/mod.rs.html)
- [hyper-util CHANGELOG (proxy connectors since 0.1.12)](https://forgejo.ellis.link/continuwuation/hyper-util/src/branch/master/CHANGELOG.md)
- [rustls crypto providers](https://docs.rs/rustls/) and [aws-lc-rs build requirements](https://docs.rs/aws-lc-rs)
- [turmoil](https://docs.rs/turmoil/0.7.2)
- [tokio-centrifuge](https://github.com/IntrepidAI/tokio-centrifuge), inspected at its latest commit
- [Tauri: calling Rust from the frontend](https://v2.tauri.app/develop/calling-rust/)
