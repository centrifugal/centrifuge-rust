# centrifuge-rust

[![crates.io](https://img.shields.io/crates/v/centrifuge-client.svg)](https://crates.io/crates/centrifuge-client)
[![docs.rs](https://img.shields.io/docsrs/centrifuge-client)](https://docs.rs/centrifuge-client)
[![CI](https://github.com/centrifugal/centrifuge-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/centrifugal/centrifuge-rust/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/crates/msrv/centrifuge-client)](Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

The official Rust real-time client SDK for [Centrifugo](https://centrifugal.dev) and the [Centrifuge](https://github.com/centrifugal/centrifuge) library. Built for desktop apps (Tauri and others), backend services, bots and tools that need a live connection.

- **Transports:** WebSocket, and HTTP-streaming fallback.
- **Protocols:** Protobuf (the default) and JSON.
- **Subscriptions:** client-side and server-side ones, positioned and recoverable streams, history, presence, join/leave, delta compression (fossil), and server-side tags filters.
- **Exactly-once delivery across reconnects:** missed publications are recovered, and losses are reported (`recovered: false`). Positions can be saved and restored across restarts.
- **Tokens:** a `get_token` callback for connection and subscription tokens, refreshed before they expire.
- **Real networks:** reconnects with jittered backoff, ping/pong liveness, detection of system sleep, captive portals, HTTP CONNECT and SOCKS5 proxies (with the OS proxy settings by default), and TLS with the OS trust store.
- **Correct under concurrency:** a sans-IO state machine owned by one task, replayed against the SDK's TLA+ model, and validated by seeded chaos against a fake server and a real Centrifugo.

```toml
[dependencies]
centrifuge-client = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

The package is `centrifuge-client`; the library is imported as `centrifuge`.

## Quick start

```rust
use centrifuge::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder("wss://example.com/connection/websocket")
        // Called before the first connect and before the token expires.
        .get_token(|_ctx| async move {
            let token = my_backend::connection_token().await?;
            Ok(Some(token))
        })
        .on_connected(|_client, e| println!("connected, client id {}", e.client_id))
        .on_disconnected(|_client, e| println!("disconnected: {} ({})", e.reason, e.code))
        .build()?;

    let sub = client
        .new_subscription("chat:42")
        // A token for this channel, also refreshed before it expires.
        .get_token(|ctx| async move { Ok(my_backend::subscription_token(&ctx.channel).await?) })
        .recoverable(true)
        .on_subscribed(|sub, _| println!("subscribed to {}", sub.channel()))
        .on_publication(|_sub, e| println!("{}", String::from_utf8_lossy(&e.data)))
        .build()?;

    sub.subscribe();     // never waits: sent right after the connect reply
    client.connect()?;   // never waits either
    sub.ready(None).await?;

    sub.publish(r#"{"text":"hello"}"#).await?;

    tokio::signal::ctrl_c().await?;
    client.close().await;
    Ok(())
}
```

`my_backend` stands for your app server, which issues the JWTs ([connection](https://centrifugal.dev/docs/server/authentication) and [channel](https://centrifugal.dev/docs/server/channel_token_auth) tokens). With a static token, use `.token(…)` instead of `.get_token(…)`.

More in [`crates/centrifuge-client/examples`](crates/centrifuge-client/examples): a terminal chat with typed JSON, token refresh, streams with `select!`, recovery across restarts, app-owned state with `get_state`, delta compression, binary payloads over Protobuf, server-side subscriptions, and the patterns for a desktop app.

```sh
docker compose up -d                      # Centrifugo on localhost:8010 (and 8012, secure)
cargo run --example quickstart            # against the secure instance, with tokens
cargo run --example chat --features serde -- alice
```

## How it works

### State changes don't wait; calls do

`connect()`, `disconnect()`, `subscribe()` and `unsubscribe()` only change the desired state and return at once, and `state()` reflects them at once. The SDK then works out the commands. That's what lets it batch: subscribe to ten channels, call `connect()`, and the ten subscribes go out in one frame right after the connect reply. Await `ready()` once if you need to know when you're connected:

```rust
for ch in ["news", "chat:42", "alerts"] {
    client.new_subscription(ch).build()?.subscribe();
}
client.connect()?;
client.ready(Some(Duration::from_secs(5))).await?;
```

Calls (`publish`, `rpc`, `history`, `presence`, `presence_stats`, `send`) made while the client is connecting, or before `connect()`, wait for the connection up to the `timeout` option, as in centrifuge-js. A `disconnect()` fails them with `Error::ClientDisconnected`. Subscription calls also wait while the subscription is subscribing.

### Events: handlers or streams

One handler per event kind, on the builder or later. Handlers get the handle as an argument, so they never need to capture it:

```rust
sub.on_publication(|sub, e| {
    let msg: Message = e.json().expect("valid JSON");     // feature "serde"
    println!("{} in {}", msg.text, sub.channel());
});
```

Or as streams, with the same order and guarantees, since an item counts as delivered only when `next()` takes it:

```rust
let mut publications = sub.publications();
while let Some(p) = publications.next().await {
    handle(p.data).await;
}
```

Handlers run one at a time on a thread of the client's own by default (`EventDelivery::Thread`). A slow handler never blocks the async runtime or another client. `EventDelivery::Inline` runs them on the client's task instead, for tools running thousands of clients.

**While a handler runs, the client processes nothing else.** That makes the event order a guarantee, so a handler can unsubscribe or disconnect and see no further event. It also means a handler can't wait for the client to make progress: `ready()` or a call that would have to wait for the connection fails with `Error::WouldDeadlock` instead of hanging. Calls on a connected client work, and with the default thread delivery a handler may even block on one (`Handle::block_on`), though this client's events wait for as long as it blocks. Spawn anything that needs to wait.

Handlers get their `Client` or `Subscription` as an argument: use that rather than capturing a handle. A handler that captures one keeps the client alive through its own handler (a reference cycle); capture a `WeakClient` (`client.downgrade()`) if it must hold on to the client.

### Recovery and exactly-once delivery

With `recoverable(true)`, a subscription remembers the position of the last publication delivered to you. After a reconnect it asks the server for everything after it. The recovered publications come first, then live ones, each exactly once. If history no longer has them, `on_subscribed` says `was_recovering: true, recovered: false`, so you know to reload your state. Positions survive restarts with `since`:

```rust
let sub = client.new_subscription("orders").recoverable(true).since(saved_position).build()?;
```

Or load the position from your own state on every subscribe that can't recover by itself, with `get_state(|ctx| async move { … })`. A position is committed only after its handler returns. A panicking handler stops the client, and the publication it got is delivered again after the next `connect()`.

### Tokens

```rust
let client = Client::builder(url)
    .get_token(|_ctx| async move {
        let token = my_backend::connection_token().await?;   // `?`: any error retries later
        Ok(Some(token))                                      // Err(TokenError::Unauthorized) stops
    })
    .build()?;
```

The getter is called before the first connect when no token is set, before the token expires (the server's `ttl`), and after the server reports it expired. Subscriptions take the same callback. It runs as a task on the runtime, never on a handler thread, and once at a time per connection attempt. The result of an attempt that `disconnect()` ended is ignored.

### Transports, proxies and TLS

```rust
use centrifuge::{Client, ProxyConfig, TlsConfig, TransportEndpoint};

let client = Client::builder_with_transports([
    TransportEndpoint::websocket("wss://example.com/connection/websocket"),
    TransportEndpoint::http_stream("https://example.com/connection/http_stream"),
])
.proxy(ProxyConfig::System)                          // the default: env vars, then OS settings
.tls(TlsConfig::new().add_root_pem(corporate_ca)?)   // the OS trust store, plus a CA
.build()?;
```

The first round tries each transport at once. After that, the transport that worked is kept. HTTP-streaming uses Centrifugo's bidirectional emulation: commands go to `/emulation` on the same origin. Proxies are HTTP(S) CONNECT (Basic auth) or SOCKS5 (username/password, local or remote DNS). They can come from the environment (`HTTPS_PROXY`, `NO_PROXY`, …), from the OS (macOS, Windows), or be decided per endpoint with `ProxyConfig::custom`. `client.set_proxy()` changes them for the next connect. Plain `ws://` and `http://` endpoints also go through an HTTP proxy with CONNECT, to port 80, which some proxies refuse by default (Squid allows CONNECT only to TLS ports): use `wss://`/`https://` behind such a proxy.

### Desktop apps and Tauri

The client keeps running with no window open, and notices on its own when the machine wakes from sleep. In Tauri (2.x), build it in `setup` with an explicit runtime, keep it in managed state, and forward events to the webview:

```rust
use tauri::{Emitter as _, Manager as _};

.setup(|app| {
    let handle = app.handle().clone();
    let client = centrifuge::Client::builder(url)
        .runtime(tauri::async_runtime::handle().inner().clone())
        .on_connected(move |_, _| { let _ = handle.emit("centrifugo-status", "online"); })
        .build()?;
    client.connect()?;
    app.manage(client);
    Ok(())
})
```

[`examples/desktop_app.rs`](crates/centrifuge-client/examples/desktop_app.rs) shows the same patterns without a GUI dependency.

## Cargo features

| Feature | Default | What it does |
|---|---|---|
| `json` | yes | JSON protocol (Protobuf, the default protocol, is always built in) |
| `http-stream` | yes | HTTP-streaming transport and fallback |
| `system-proxy` | yes | OS proxy settings (macOS, Windows) |
| `aws-lc-rs` | yes | rustls crypto provider |
| `ring` | no | the alternative rustls crypto provider |
| `delta` | yes | fossil delta compression |
| `serde` | yes | `publish_json`, `rpc_json`, `.json::<T>()` on events and results |
| `rustls-config` | no | a raw `rustls::ClientConfig` (exempt from semver: rustls is pre-1.0) |
| `test-util` | no | public constructors for events and results, to unit-test your handlers |

## Compatibility

- **Protocol:** the Centrifugo client protocol v2 (Centrifugo v4 and newer). HTTP-streaming needs Centrifugo v5 or newer with `http_stream` enabled. Tested against Centrifugo v6.10.
- **Runtime:** Tokio (multi-thread or current-thread).
- **Platforms:** Linux, macOS, Windows. A browser (wasm) build is planned: the core already builds for `wasm32-unknown-unknown`.
- **MSRV:** Rust 1.85. Raising it is a minor-version change, noted in the [changelog](CHANGELOG.md).

## Differences from centrifuge-js

The behaviour follows centrifuge-js: the same states, events, codes and recovery semantics. The few differences are deliberate and listed in [spec §1.1](docs/spec.md):

- Event handlers run after the operation that triggered them returns, not inside it.
- Handler panics stop the client visibly (`error`, then `disconnected`); it works again after `connect()`.
- There are no `offline`/`online` hooks. Call `disconnect(); connect()` on a network change.

## How it's tested

- **TLA+ model.** The core replays 663 traces generated from the SDK's TLA+ model, step by step: states, events, publications (offset and payload), commands, callbacks, timers. The counterexamples of the model's known-bug variants must diverge, and they do, at the bug's step.
- **Unit and integration tests.** These run against an in-process fake server over every transport and codec. Integration tests also run against a real Centrifugo in CI (`docker compose up`).
- **Validation harness.** Seeded chaos (600 runs, invariants I1–I8), probes of more than 20 known bug patterns from other SDKs, real-server chaos through a fault proxy (kills, blackholes, 1-byte re-chunking, latency, short-lived tokens), and process suspension.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
