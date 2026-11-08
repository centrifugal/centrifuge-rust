# Changelog

Release notes are on the [releases page](https://github.com/centrifugal/centrifuge-rust/releases).

## [Unreleased]

## [0.1.2]

### Added

- Browsers (`wasm32-unknown-unknown`): the client runs in a page or a Dedicated Worker over the browser's `WebSocket` and `fetch` (HTTP-streaming), with the same API (handlers needn't be `Send` there). `EventDelivery::Inline` is the default on wasm32; an explicit `proxy()`, `tls()`, `http_headers()` or `EventDelivery::Thread`, or a missing browser API, fails `build()`. As in centrifuge-js, `online` / `offline` and the back/forward cache are acted on. New `ClientBuilder::credentials` (`Credentials`) sets `fetch`'s credentials mode; no effect on other targets. Tested in Chrome and Firefox in CI. See the README.
- `centrifuge_client_core::time::Instant`, the clock type the state machine takes (`std::time::Instant` on native targets).

## [0.1.1]

### Added

- `centrifuge::android` (Android only): `init_with_env`, `init_with_refs` and `init_with_runtime` set up the TLS platform verifier through the client's own copy of rustls-platform-verifier, so the app can't initialise a different version by mistake. Without the setup, `build()` fails with `Error::Configuration` when an endpoint uses TLS, instead of the verifier panicking on the first connection. Exempt from semver: the signatures use `jni` types, re-exported as `centrifuge::android::jni` (docs/spec.md §5.2). An app that sets up rustls-platform-verifier directly, as the earlier docs on `main` said, or through another library, also calls the wrapper once; it's a no-op then, and without it `build()` refuses TLS endpoints.

### Fixed

- `ready()` and `sub.ready()` awaited right after `connect()` or `subscribe()`, and calls made from event handlers, could deadlock the calling thread: they read a state snapshot twice under one held borrow, and a snapshot update arriving between the two reads blocked both sides. Rare (about one in a few hundred test runs), and no timeout could end it, since the thread was blocked rather than waiting.
- `build()` fails with `Error::Configuration` when the `EventDelivery::Thread` event thread can't be started. Before, the client ran but silently delivered no events at all. If the event thread stops later (an SDK bug), the client now stops loudly as on a handler panic, with an `error` event and `disconnected`, and the events it didn't deliver aren't committed, so recovery delivers them again. Before, it kept running, dropped every event and advanced positions past them. Later events are delivered on the client's task (docs/spec.md §9.4).
- HTTP-streaming: closing a session now stops an emulation request already in flight. Before, `send()` and other calls could wait until that request ended or hit its timeout after `disconnect()`, and the request kept its connection open. Now they fail with `ClientDisconnected` right away.
- `centrifuge-client` builds for Android. The platform verifier there takes no extra roots, so a `TlsConfig` with PEM CAs fails `build()` with `Error::Configuration` on Android instead of dropping them (docs/spec.md §5.2). Android and iOS are now built in CI.

## [0.1.0]

The first usable release: phase 1 of [`docs/spec.md`](docs/spec.md).

### Added

- `Client` with connect, disconnect, close, states and events (handlers or `Stream`s); automatic reconnect with jittered backoff; ping/pong liveness; system sleep detection.
- Transports: WebSocket and HTTP-streaming with bidirectional emulation, with fallback between them; captive-portal detection.
- Protocols: Protobuf (default) and JSON.
- Connection and subscription tokens with `get_token` and refresh; connect and subscribe `data` with `set_data()`; real HTTP headers and emulated headers.
- Client-side subscriptions: positioned and recoverable streams with exactly-once delivery across reconnects, `since` and `get_state`, join/leave, fossil delta compression, server-side tags filters, channel compaction (Centrifugo PRO).
- Server-side subscriptions; async server messages.
- Calls: publish, RPC, send, history, presence, presence stats (waiting for the connection while connecting); typed JSON helpers (feature `serde`).
- Proxies: HTTP(S) CONNECT and SOCKS5 with auth, environment variables, OS settings (macOS, Windows), per-endpoint decisions; TLS with the OS trust store and extra roots.
- `statistics()`, `WeakClient`, `EventDelivery::{Thread, Inline}`, `WouldDeadlock` detection for calls from handlers.
- Examples, benchmarks, a TLA+ model trace replay of the core.
