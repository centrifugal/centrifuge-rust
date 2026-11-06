# Changelog

Release notes are on the [releases page](https://github.com/centrifugal/centrifuge-rust/releases).

## [Unreleased]

### Added

- `centrifuge::android` (Android only): `init_with_env`, `init_with_refs` and `init_with_runtime` set up the TLS platform verifier through the client's own copy of rustls-platform-verifier, so the app can't initialise a different version by mistake. Without the setup, `build()` fails with `Error::Configuration` when an endpoint uses TLS, instead of the verifier panicking on the first connection. Exempt from semver: the signatures use `jni` types, re-exported as `centrifuge::android::jni` (docs/spec.md §5.2).

### Fixed

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
