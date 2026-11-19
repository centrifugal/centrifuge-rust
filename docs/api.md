# centrifuge-client: planned public API

Status: implemented (phase 1), 2026-10-27. Derived from [`spec.md`](spec.md) §4–§5, §7, §9, §10.

This is the whole public surface of `centrifuge-client` (imported as `centrifuge`) on one page, for comparing the implementation against. Behaviour lives in the spec. When the two disagree, the spec wins, and this page gets fixed. Signatures are the design target and may still change when the code reads better (`impl Into<Bytes>` vs `Bytes`, etc.).

Builder options are methods, not public struct fields (C-STRUCT-PRIVATE). Every public type that may grow is `#[non_exhaustive]`.

## Client

```rust
impl Client {                               // Clone + Send + Sync; clones share one connection
    pub fn builder(endpoint: impl Into<String>) -> ClientBuilder;       // ws(s):// → WebSocket, http(s):// → HTTP-streaming; checked by build()
    pub fn builder_with_transports(list: impl IntoIterator<Item = TransportEndpoint>) -> ClientBuilder; // fallback, in order

    // State changes: never wait. state() reflects them at once; the SDK applies them right after.
    pub fn connect(&self) -> Result<(), Error>;          // Err(ClientClosed) only after close()
    pub fn disconnect(&self);
    pub fn close(&self) -> impl Future<Output = ()> + Send + 'static; // starts closing when called; await = wait for completion
    pub fn state(&self) -> State;                        // Disconnected | Connecting | Connected | Closed
    pub async fn ready(&self, timeout: Option<Duration>) -> Result<(), Error>; // fails at once when disconnected

    pub fn set_token(&self, token: impl Into<String>);
    pub fn set_data(&self, data: impl Into<Bytes>) -> Result<(), Error>;      // JSON protocol: must be valid JSON
    pub fn set_http_headers(&self, headers: http::HeaderMap) -> Result<(), Error>;
    pub fn set_emulated_headers(&self, headers: HashMap<String, String>) -> Result<(), Error>;
    pub fn set_proxy(&self, proxy: ProxyConfig) -> Result<(), Error>;

    // Calls: on any channel the server allows, no client-side subscription needed
    // (as centrifuge-js client.publish etc.). They wait for the connection while
    // connecting or disconnected, up to `timeout`.
    pub async fn rpc(&self, method: &str, data: impl Into<Bytes>) -> Result<RpcResult, Error>;
    pub async fn send(&self, data: impl Into<Bytes>) -> Result<(), Error>;   // resolves once written
    pub async fn publish(&self, channel: &str, data: impl Into<Bytes>) -> Result<(), Error>;
    pub async fn history(&self, channel: &str, opts: HistoryOptions) -> Result<HistoryResult, Error>;
    pub async fn presence(&self, channel: &str) -> Result<PresenceResult, Error>;
    pub async fn presence_stats(&self, channel: &str) -> Result<PresenceStatsResult, Error>;
    // feature "serde"
    pub async fn rpc_json<Req: Serialize, Resp: DeserializeOwned>(&self, method: &str, req: &Req) -> Result<Resp, Error>;
    pub async fn publish_json<T: Serialize>(&self, channel: &str, value: &T) -> Result<(), Error>;

    // Subscription registry (changes synchronously)
    pub fn new_subscription(&self, channel: impl Into<String>) -> SubscriptionBuilder;
    pub fn get_subscription(&self, channel: &str) -> Option<Subscription>;
    pub fn remove_subscription(&self, sub: &Subscription) -> Result<(), Error>; // Ok when not registered
    pub fn subscriptions(&self) -> HashMap<String, Subscription>;            // a copy

    pub fn statistics(&self) -> Statistics;
    pub fn downgrade(&self) -> WeakClient;                                   // WeakClient::upgrade() -> Option<Client>

    // Handlers (also on ClientBuilder). One per kind; registering again replaces it.
    pub fn on_connecting  (&self, f: impl FnMut(&Client, ConnectingEvent)   + MaybeSend + 'static);
    pub fn on_connected   (&self, f: impl FnMut(&Client, ConnectedEvent)    + MaybeSend + 'static);
    pub fn on_disconnected(&self, f: impl FnMut(&Client, DisconnectedEvent) + MaybeSend + 'static);
    pub fn on_error       (&self, f: impl FnMut(&Client, ErrorEvent)        + MaybeSend + 'static);
    pub fn on_message     (&self, f: impl FnMut(&Client, MessageEvent)      + MaybeSend + 'static);
    // Client-level subscription handlers are for SERVER-SIDE subscriptions (names as in centrifuge-js).
    pub fn on_subscribed  (&self, f: impl FnMut(&Client, ServerSubscribedEvent)   + MaybeSend + 'static);
    pub fn on_subscribing (&self, f: impl FnMut(&Client, ServerSubscribingEvent)  + MaybeSend + 'static);
    pub fn on_unsubscribed(&self, f: impl FnMut(&Client, ServerUnsubscribedEvent) + MaybeSend + 'static);
    pub fn on_publication (&self, f: impl FnMut(&Client, ServerPublicationEvent)  + MaybeSend + 'static);
    pub fn on_join        (&self, f: impl FnMut(&Client, ServerJoinEvent)         + MaybeSend + 'static);
    pub fn on_leave       (&self, f: impl FnMut(&Client, ServerLeaveEvent)        + MaybeSend + 'static);

    // Streams (hand-off delivery: an item counts as delivered when next() takes it)
    pub fn events(&self) -> EventStream<ClientEvent>;     // Stream + FusedStream + Unpin
}

impl ClientBuilder {
    pub fn build(self) -> Result<Client, Error>;          // validates everything; Error::Configuration otherwise
    pub fn runtime(self, h: tokio::runtime::Handle) -> Self;           // default: Handle::try_current()
    pub fn protocol(self, p: Protocol) -> Self;                        // Protocol::Protobuf (default) | Protocol::Json
    pub fn token(self, t: impl Into<String>) -> Self;
    pub fn get_token<F, Fut>(self, f: F) -> Self
        where F: Fn(ConnectionTokenContext) -> Fut + MaybeSend + MaybeSync + 'static,
              Fut: Future<Output = Result<Option<String>, TokenError>> + MaybeSend + 'static;
    pub fn data(self, d: impl Into<Bytes>) -> Self;
    pub fn name(self, n: impl Into<String>) -> Self;                   // default "rust", ≤ 16 chars
    pub fn version(self, v: impl Into<String>) -> Self;                // ≤ 64 chars
    pub fn http_headers(self, h: http::HeaderMap) -> Self;             // managed headers (Host, Upgrade, …) rejected
    pub fn emulated_headers(self, h: HashMap<String, String>) -> Self;
    pub fn websocket_subprotocols<I, S>(self, subprotocols: I) -> Self;  // extra Sec-WebSocket-Protocol values; default: none
    pub fn emulation_endpoint(self, url: impl Into<String>) -> Self;   // default: /emulation on the stream's origin
    pub fn timeout(self, d: Duration) -> Self;                         // 5 s
    pub fn handshake_timeout(self, d: Duration) -> Self;               // 5 s
    pub fn min_reconnect_delay(self, d: Duration) -> Self;             // 500 ms
    pub fn max_reconnect_delay(self, d: Duration) -> Self;             // 20 s
    pub fn max_server_ping_delay(self, d: Duration) -> Self;           // 10 s
    pub fn max_message_size(self, bytes: usize) -> Self;               // 64 MiB
    pub fn tls(self, t: TlsConfig) -> Self;
    pub fn proxy(self, p: ProxyConfig) -> Self;                        // ProxyConfig::System (default)
    pub fn event_delivery(self, m: EventDelivery) -> Self;             // Thread (default) | Inline
    // + every on_* handler from Client
}
```

Token getters: `Ok(Some(t))` uses `t`. `Ok(None)` connects anonymously (on refresh: unauthorized). `Err(TokenError::Unauthorized)` stops. Any other error retries later, and `?` works because `TokenError: From<E: std::error::Error + Send + Sync + 'static>`.

## Subscription

```rust
impl Subscription {                         // Clone + Send + Sync
    pub fn channel(&self) -> &str;
    pub fn state(&self) -> SubState;        // Unsubscribed | Subscribing | Subscribed
    pub fn subscribe(&self);                // never waits; works before connect()
    pub fn unsubscribe(&self);              // keeps the recovery position
    pub async fn ready(&self, timeout: Option<Duration>) -> Result<(), Error>;
    pub fn set_data(&self, data: impl Into<Bytes>) -> Result<(), Error>;
    pub fn set_tags_filter(&self, f: Option<FilterNode>) -> Result<(), Error>;  // only while unsubscribed, not with delta

    // Calls (wait while subscribing, up to `timeout`)
    pub async fn publish(&self, data: impl Into<Bytes>) -> Result<(), Error>;
    pub async fn history(&self, opts: HistoryOptions) -> Result<HistoryResult, Error>;
    pub async fn presence(&self) -> Result<PresenceResult, Error>;
    pub async fn presence_stats(&self) -> Result<PresenceStatsResult, Error>;
    pub async fn publish_json<T: Serialize>(&self, value: &T) -> Result<(), Error>;   // feature "serde"

    pub fn on_subscribing (&self, f: impl FnMut(&Subscription, SubscribingEvent)       + MaybeSend + 'static);
    pub fn on_subscribed  (&self, f: impl FnMut(&Subscription, SubscribedEvent)        + MaybeSend + 'static);
    pub fn on_unsubscribed(&self, f: impl FnMut(&Subscription, UnsubscribedEvent)      + MaybeSend + 'static);
    pub fn on_error       (&self, f: impl FnMut(&Subscription, SubscriptionErrorEvent) + MaybeSend + 'static);
    pub fn on_publication (&self, f: impl FnMut(&Subscription, PublicationEvent)       + MaybeSend + 'static);
    pub fn on_join        (&self, f: impl FnMut(&Subscription, JoinEvent)              + MaybeSend + 'static);
    pub fn on_leave       (&self, f: impl FnMut(&Subscription, LeaveEvent)             + MaybeSend + 'static);

    pub fn client(&self) -> &Client;
    pub fn events(&self) -> EventStream<SubscriptionEvent>;
    pub fn publications(&self) -> EventStream<PublicationEvent>;
}

impl SubscriptionBuilder {
    pub fn build(self) -> Result<Subscription, Error>;    // Err(DuplicateSubscription) | Err(Configuration) | Err(ClientClosed)
    pub fn token(self, t: impl Into<String>) -> Self;
    pub fn get_token<F, Fut>(self, f: F) -> Self          // empty string = unauthorized
        where F: Fn(SubscriptionTokenContext) -> Fut + MaybeSend + MaybeSync + 'static,
              Fut: Future<Output = Result<String, TokenError>> + MaybeSend + 'static;
    pub fn data(self, d: impl Into<Bytes>) -> Self;
    pub fn positioned(self, on: bool) -> Self;
    pub fn recoverable(self, on: bool) -> Self;
    pub fn join_leave(self, on: bool) -> Self;
    pub fn since(self, pos: StreamPosition) -> Self;
    pub fn delta(self, d: DeltaType) -> Self;             // DeltaType::Fossil
    pub fn tags_filter(self, f: FilterNode) -> Self;
    pub fn get_state<F, Fut>(self, f: F) -> Self
        where F: Fn(SubscriptionGetStateContext) -> Fut + MaybeSend + MaybeSync + 'static,
              Fut: Future<Output = Result<StreamPosition, BoxError>> + MaybeSend + 'static;
    pub fn min_resubscribe_delay(self, d: Duration) -> Self;   // 500 ms
    pub fn max_resubscribe_delay(self, d: Duration) -> Self;   // 20 s
    // + every on_* handler from Subscription
}
```

Callback contexts: `ConnectionTokenContext {}`, `SubscriptionTokenContext { channel }`, `SubscriptionGetStateContext { channel }`.

## Data types

Payloads are `Bytes` in both protocols, re-exported as `centrifuge::Bytes`.

```rust
pub struct Publication { pub data: Bytes, pub offset: u64, pub tags: HashMap<String, String>,
                         pub info: Option<ClientInfo>, pub channel: Option<String>, pub time: Option<i64> }
impl Publication { pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error>; }   // feature "serde"
pub struct ClientInfo { pub user: String, pub client: String, pub conn_info: Option<Bytes>, pub chan_info: Option<Bytes> }
pub struct StreamPosition { pub offset: u64, pub epoch: String }
impl StreamPosition { pub fn new(offset: u64, epoch: impl Into<String>) -> Self; }

pub struct HistoryOptions { /* private */ }
impl HistoryOptions {
    pub fn new() -> Self;                 // position only, as centrifuge-js history() with no arguments
    pub fn limit(self, n: u32) -> Self;   // up to n publications
    pub fn all(self) -> Self;             // every publication the server keeps
    pub fn position_only(self) -> Self;
    pub fn since(self, pos: StreamPosition) -> Self;
    pub fn reverse(self, on: bool) -> Self;
}
pub struct HistoryResult { pub publications: Vec<Publication>, pub offset: u64, pub epoch: String }
pub struct PresenceResult { pub clients: HashMap<String, ClientInfo> }
pub struct PresenceStatsResult { pub num_clients: u32, pub num_users: u32 }
pub struct RpcResult { pub data: Bytes }          // .json::<T>() with "serde"

pub struct Statistics {
    pub state: State, pub transport: Option<TransportKind>, pub connected_since: Option<SystemTime>,
    pub connects: u64, pub reconnect_attempts: u64, pub bytes_in: u64, pub bytes_out: u64,
    pub messages_in: u64, pub messages_out: u64, pub publications_delivered: u64, pub subscriptions: usize,
}
```

## Events

Each event is a struct named after it.

| Event | Fields |
|---|---|
| `ConnectingEvent`, `DisconnectedEvent` | `code: u32`, `reason: String` |
| `ConnectedEvent` | `client_id`, `version`, `data: Option<Bytes>`, `transport: TransportKind` |
| `ErrorEvent` | `error: ClientError` |
| `MessageEvent` | `data: Bytes` (`.json::<T>()`) |
| `SubscribingEvent`, `UnsubscribedEvent` | `code`, `reason` |
| `SubscribedEvent` | `was_recovering`, `recovered`, `recoverable`, `positioned`, `stream_position: Option<StreamPosition>`, `data: Option<Bytes>` |
| `SubscriptionErrorEvent` | `error: SubscriptionError` |
| `PublicationEvent` | `Deref<Target = Publication>` |
| `JoinEvent`, `LeaveEvent` | `Deref<Target = ClientInfo>` |
| `ServerSubscribedEvent`, `ServerSubscribingEvent`, `ServerUnsubscribedEvent`, `ServerPublicationEvent`, `ServerJoinEvent`, `ServerLeaveEvent` | the same fields as their client-side counterparts, plus `channel` |
| `ClientEvent`, `SubscriptionEvent` | enums over the above, for the streams |

## Errors

```rust
pub enum Error {
    Timeout, ClientDisconnected, ClientClosed, SubscriptionUnsubscribed, DuplicateSubscription,
    Server(ServerError),                                  // code, message, temporary
    InvalidData(String),
    Transport { status: Option<u16>, message: String },  // #[non_exhaustive]
    Configuration(String),
    WouldDeadlock,                                        // spec §9.5
}
pub enum TokenError { Unauthorized, Other(BoxError) }    // not std::error::Error, so that `?` works in getters

pub struct ClientError { pub kind: ClientErrorKind, pub message: String, pub source: Option<ArcError> }   // Clone
pub enum ClientErrorKind {
    Transport { transport: TransportKind, status: Option<u16> }, ConnectToken, Connect, Refresh,
    Configuration, Protocol, Delta { channel: String, offset: u64 }, HandlerPanicked,
}
pub struct SubscriptionError { pub kind: SubscriptionErrorKind, pub message: String, pub source: Option<ArcError> }
pub enum SubscriptionErrorKind { Subscribe, Token, GetState, Refresh, Configuration }
```

## Transport, proxy, TLS

```rust
pub enum TransportKind { WebSocket, HttpStream }
impl TransportEndpoint { pub fn websocket(url: impl Into<String>) -> Self; pub fn http_stream(url: impl Into<String>) -> Self; }
pub enum EventDelivery { Thread, Inline }
pub enum Protocol { Protobuf, Json }
pub enum DeltaType { Fossil }

pub enum ProxyConfig {                                       // #[non_exhaustive]
    None, FromEnv, System,                                   // System: env vars, then OS settings (default)
    Http { url, auth },                                      // #[non_exhaustive]: build with ProxyConfig::http(url, auth)
    Socks5 { url, auth, remote_dns },                        // #[non_exhaustive]: ProxyConfig::socks5(url, auth, remote_dns)
    Custom(CustomProxy),                                     // opaque: ProxyConfig::custom(|url| -> ProxyTarget)
}
impl ProxyConfig {
    pub fn http(url: Url, auth: Option<ProxyAuth>) -> Self;
    pub fn socks5(url: Url, auth: Option<ProxyAuth>, remote_dns: bool) -> Self;
    pub fn custom(f: impl Fn(&Url) -> ProxyTarget + MaybeSend + MaybeSync + 'static) -> Self;
}
pub struct ProxyAuth { pub username: String, pub password: String }   // Debug redacts the password
pub enum ProxyTarget { Direct, Http { .. }, Socks5 { .. } }          // ProxyTarget::http(url, auth), ::socks5(url, auth, remote_dns)

impl TlsConfig {
    pub fn new() -> Self;                                    // OS trust store (rustls-platform-verifier)
    pub fn add_root_pem(self, pem: &[u8]) -> Result<Self, Error>;
    pub fn danger_accept_invalid_certs(self) -> Self;        // development only; warns on every connect
    pub fn rustls_client_config(cfg: rustls::ClientConfig) -> Self;  // feature "rustls-config", semver-exempt
}
```

## Misc

```rust
pub mod codes {
    // client-side connecting / disconnected / subscribing / unsubscribed codes (spec §6.5), plus:
    pub const TOKEN_EXPIRED: u32 = 109;
    pub const UNRECOVERABLE_POSITION: u32 = 112;
    pub const UNSUBSCRIBE_STATE_INVALIDATED: u32 = 2502;
    pub const DISCONNECT_STATE_INVALIDATED: u32 = 3014;
    pub fn is_reconnect_code(code: u32) -> bool;
}
pub struct Filter;   // Filter::eq/neq/in_/nin/exists/not_exists/starts_with/ends_with/contains/gt/gte/lt/lte/and/or/not → FilterNode
pub trait MaybeSend; pub trait MaybeSync;   // Send/Sync on native targets, implemented for every type on wasm32
pub use bytes::Bytes;
```

## Cargo features

| Feature | Default | Effect |
|---|---|---|
| `json` | yes | JSON codec |
| `http-stream` | yes | HTTP-streaming transport and fallback |
| `system-proxy` | yes | OS proxy settings |
| `aws-lc-rs` | yes | rustls crypto provider |
| `ring` | no | Alternative crypto provider |
| `delta` | yes | Fossil delta decoding |
| `serde` | yes | `*_json` helpers |
| `rustls-config` | no | Raw `rustls::ClientConfig` (semver-exempt) |
| `test-util` | no | Constructors for events and results, for unit-testing handlers |

## Notes for the API docs

These go into the rustdoc of the items concerned:

- **`history()` with default options returns no publications.** `HistoryOptions::new()` asks for the stream position only, as centrifuge-js `history()` does without arguments. Examples must show `.limit(n)` or `.all()` to get publications:
  ```rust
  let last_ten = sub.history(HistoryOptions::new().limit(10)).await?;
  let top = sub.history(HistoryOptions::new()).await?;   // offset + epoch only
  ```
- **Client-level `on_subscribed` and friends are for server-side subscriptions.** The first line of their rustdoc says so. Client-side subscriptions use the same handler names on `Subscription`.
- **Token getters:** show `|ctx| async move { ... }`, not `async |ctx|`, which stable Rust can't yet require to be `Send`. Also show cloning a captured HTTP client into the closure and again into the `async` block.

## Deliberately not in the API

| Not offered | Use instead |
|---|---|
| `connect_and_wait()` | `connect()` then `ready()`, so batching isn't serialized away |
| `flush()` | Calls already wait for replies |
| `server_info()` | Not needed for now |
| `force_reconnect()` / `network_changed()` | `disconnect(); connect()` |
| `get_data` callbacks | `set_data()` |

Phase 3, not in phase 1: map and shared-poll subscriptions, manual batching, publish debouncing, optimistic subscriptions.
