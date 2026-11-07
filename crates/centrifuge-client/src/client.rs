//! The client handle and its builder (docs/spec.md §4.1–§4.4).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use centrifuge_client_core::codec::{self, Protocol};
use centrifuge_client_core::machine::{Core, CoreConfig, Op, State, TransportKind};
use centrifuge_client_core::proto;
use centrifuge_client_core::types::{
    HistoryOptions, HistoryResult, PresenceResult, PresenceStatsResult, RpcResult,
};
use tokio::sync::{mpsc, oneshot};
use url::Url;

use crate::calls;
use crate::config::{
    EventDelivery, ProxyConfig, TlsConfig, TransportEndpoint, check_http_headers, defaults,
    duration,
};
use crate::dispatch::{ClientHandlers, Delivery, Dispatcher, in_handler};
use crate::driver::{ConnTokenGetter, Driver, DriverConfig, TokenFuture};
use crate::error::{Error, TokenError};
use crate::events::{
    ClientEvent, ConnectedEvent, ConnectingEvent, DisconnectedEvent, ErrorEvent, MessageEvent,
    ServerJoinEvent, ServerLeaveEvent, ServerPublicationEvent, ServerSubscribedEvent,
    ServerSubscribingEvent, ServerUnsubscribedEvent,
};
use crate::maybe_send::{MaybeSend, MaybeSync};
use crate::shared::{CallConfig, Command, Shared, lock};
use crate::stats::Statistics;
use crate::stream::EventStream;
use crate::subscription::{Subscription, SubscriptionBuilder};
use crate::transport::net::{Net, uses_tls};

/// Passed to the connection token getter. Empty for now; it may carry more
/// context later.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ConnectionTokenContext {}

impl ConnectionTokenContext {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

/// Owns the driver: dropping the last one stops it.
pub(crate) struct ClientInner {
    pub(crate) shared: Arc<Shared>,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        // An unpolled stream must not hold the driver (and its thread and
        // socket) alive once nobody can reach the client.
        self.shared.stop.send_replace(true);
        let _ = self.shared.commands.send(Command::Dropped);
    }
}

/// A connection to a Centrifugo or Centrifuge server.
///
/// Cheap to clone; clones share one connection. The client runs until
/// [`close()`](Self::close) or until every `Client` and [`Subscription`]
/// handle is dropped.
///
/// Handlers get the client as an argument, so they don't need to capture it.
/// A handler that captures a `Client`, or a [`Subscription`] (which keeps its
/// client alive), creates a reference cycle and the client is never dropped;
/// capture a [`WeakClient`] instead.
///
/// ```no_run
/// # async fn run() -> Result<(), centrifuge::Error> {
/// let client = centrifuge::Client::builder("wss://example.com/connection/websocket")
///     .token("<JWT>")
///     .on_connected(|_client, e| println!("connected over {:?}", e.transport))
///     .build()?;
/// client.connect()?;
/// client.ready(None).await?;
/// client.publish("news", r#"{"text":"hi"}"#).await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("id", &self.inner.shared.id)
            .field("state", &self.state())
            .finish()
    }
}

/// A [`Client`] handle that doesn't keep the client alive.
#[derive(Clone, Debug)]
pub struct WeakClient {
    inner: Weak<ClientInner>,
}

impl WeakClient {
    /// The client, if it's still alive.
    #[must_use]
    pub fn upgrade(&self) -> Option<Client> {
        self.inner.upgrade().map(|inner| Client { inner })
    }
}

impl Client {
    /// A builder for one endpoint: `ws://`/`wss://` for WebSocket,
    /// `http://`/`https://` for HTTP-streaming.
    pub fn builder(endpoint: impl Into<String>) -> ClientBuilder {
        let url: String = endpoint.into();
        let kind = if url.starts_with("http://") || url.starts_with("https://") {
            TransportKind::HttpStream
        } else {
            TransportKind::WebSocket
        };
        ClientBuilder::new(vec![TransportEndpoint { kind, url }])
    }

    /// A builder with transport fallback: tried in order until one works
    /// (docs/spec.md §5.8).
    pub fn builder_with_transports(
        list: impl IntoIterator<Item = TransportEndpoint>,
    ) -> ClientBuilder {
        ClientBuilder::new(list.into_iter().collect())
    }

    pub(crate) fn from_inner(inner: Arc<ClientInner>) -> Self {
        Self { inner }
    }

    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.inner.shared
    }

    /// Starts connecting. Never waits: [`state()`](Self::state) shows
    /// `Connecting` at once, and the SDK connects in the background,
    /// reconnecting until [`disconnect()`](Self::disconnect) or
    /// [`close()`](Self::close). A no-op when connecting or connected.
    ///
    /// Subscriptions subscribed before connecting go out right after the
    /// connect reply, in one frame. Await [`ready()`](Self::ready) to know when
    /// the client is connected.
    ///
    /// # Errors
    ///
    /// [`Error::ClientClosed`] after `close()`.
    pub fn connect(&self) -> Result<(), Error> {
        let st = self.inner.shared.send_client_op(Op::Connect, |s| match s {
            State::Disconnected => State::Connecting,
            s => s,
        });
        if st == State::Closed {
            Err(Error::ClientClosed)
        } else {
            Ok(())
        }
    }

    /// Disconnects (code 0, "disconnect called"). Subscriptions go to
    /// `Subscribing` and keep their positions for the next `connect()`.
    pub fn disconnect(&self) {
        self.inner
            .shared
            .send_client_op(Op::Disconnect, |s| match s {
                State::Closed => State::Closed,
                _ => State::Disconnected,
            });
    }

    /// Closes the client for good: subscriptions are unsubscribed (code 2),
    /// the connection ends and the driver stops.
    ///
    /// Closing starts when this is **called**. The returned future only waits
    /// until the close is complete and its events delivered, so dropping it
    /// still closes. Awaited from inside an event handler, it returns at once
    /// (the close can't complete while the handler runs).
    pub fn close(&self) -> impl Future<Output = ()> + Send + 'static {
        let shared = &self.inner.shared;
        let rx = lock(&shared.close_waiters).as_mut().map(|w| {
            let (tx, rx) = oneshot::channel();
            w.push(tx);
            rx
        });
        // Closing never waits for stream consumers: an unpolled stream can't
        // hold close() (its last item stays buffered, then it ends).
        shared.stop.send_replace(true);
        shared.send_client_op(Op::Close, |_| State::Closed);
        let rx = if in_handler(shared.id) { None } else { rx };
        async move {
            if let Some(rx) = rx {
                let _ = rx.await;
            }
        }
    }

    /// The client state, including the effect of operations called just
    /// before: `connect()` then `state()` gives `Connecting`, or a later state
    /// if the client has already moved on (`Connected`).
    #[must_use]
    pub fn state(&self) -> State {
        self.inner.shared.state()
    }

    /// Waits until the client is connected and its `connected` handler has
    /// returned.
    ///
    /// # Errors
    ///
    /// Fails at once with [`Error::ClientDisconnected`] while disconnected and
    /// [`Error::ClientClosed`] after `close()`; with [`Error::Timeout`] when
    /// `timeout` passes, and with [`Error::WouldDeadlock`] from an event
    /// handler while not connected.
    pub async fn ready(&self, timeout: Option<Duration>) -> Result<(), Error> {
        calls::client_ready(&self.inner.shared, timeout).await
    }

    /// Replaces the connection token used from the next connect attempt.
    pub fn set_token(&self, token: impl Into<String>) {
        self.inner.shared.send_op(Op::SetToken(token.into()));
    }

    /// Replaces the connect data sent with the next connect attempt.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidData`] when the JSON protocol is used and `data` isn't
    /// valid JSON.
    pub fn set_data(&self, data: impl Into<Bytes>) -> Result<(), Error> {
        let data = data.into();
        self.validate("data", &data)?;
        self.inner.shared.send_op(Op::SetData(data));
        Ok(())
    }

    /// Replaces the HTTP headers sent from the next connect attempt (the
    /// WebSocket upgrade, the HTTP-stream request and emulation requests).
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for headers the transport manages (`Host`,
    /// `Upgrade`, `Sec-WebSocket-*`, …).
    pub fn set_http_headers(&self, headers: http::HeaderMap) -> Result<(), Error> {
        check_http_headers(&headers)?;
        *lock(&self.inner.shared.http_headers) = headers;
        Ok(())
    }

    /// Replaces the headers sent inside the connect command from the next
    /// connect attempt (Centrifugo `emulated_headers`; for environments that
    /// can't set real headers).
    ///
    /// # Errors
    ///
    /// Never for now; the `Result` leaves room for validation.
    pub fn set_emulated_headers(&self, headers: HashMap<String, String>) -> Result<(), Error> {
        self.inner.shared.send_op(Op::SetEmulatedHeaders(headers));
        Ok(())
    }

    /// Replaces the proxy configuration from the next connect attempt.
    ///
    /// # Errors
    ///
    /// Never for now; the `Result` leaves room for validation.
    pub fn set_proxy(&self, proxy: ProxyConfig) -> Result<(), Error> {
        *lock(&self.inner.shared.net.proxy) = proxy;
        Ok(())
    }

    fn validate(&self, field: &'static str, data: &[u8]) -> Result<(), Error> {
        codec::validate_payload(self.inner.shared.cfg.protocol, field, data)
            .map_err(|e| Error::InvalidData(e.to_string()))
    }

    /// Calls an RPC method on the server.
    ///
    /// Waits for the connection while connecting or disconnected, up to the
    /// configured `timeout`.
    ///
    /// # Errors
    ///
    /// [`Error::Server`] when the server answers with an error; see [`Error`]
    /// for the rest.
    pub async fn rpc(&self, method: &str, data: impl Into<Bytes>) -> Result<RpcResult, Error> {
        let data = data.into();
        self.validate("data", &data)?;
        let cmd = proto::Command {
            rpc: Some(proto::RpcRequest {
                data,
                method: method.to_owned(),
            }),
            ..Default::default()
        };
        let reply = calls::call(&self.inner.shared, None, cmd).await?;
        Ok(reply.rpc.map(Into::into).unwrap_or_default())
    }

    /// Sends an async message to the server (fire and forget). Resolves once
    /// written to the connection.
    ///
    /// # Errors
    ///
    /// As [`rpc()`](Self::rpc), without server errors.
    pub async fn send(&self, data: impl Into<Bytes>) -> Result<(), Error> {
        let data = data.into();
        self.validate("data", &data)?;
        let cmd = proto::Command {
            send: Some(proto::SendRequest { data }),
            ..Default::default()
        };
        calls::send(&self.inner.shared, cmd).await
    }

    /// Publishes into any channel the server allows; no subscription needed.
    ///
    /// A publish whose future is dropped before it resolves may still have
    /// been delivered.
    ///
    /// # Errors
    ///
    /// As [`rpc()`](Self::rpc).
    pub async fn publish(&self, channel: &str, data: impl Into<Bytes>) -> Result<(), Error> {
        let data = data.into();
        self.validate("data", &data)?;
        calls::call(&self.inner.shared, None, publish_command(channel, data))
            .await
            .map(drop)
    }

    /// Reads a channel's history; no subscription needed.
    ///
    /// **`HistoryOptions::new()` returns no publications**, only the stream
    /// position (as centrifuge-js `history()` without arguments). Ask for
    /// them with `.limit(n)` or `.all()`:
    ///
    /// ```no_run
    /// # async fn run(client: centrifuge::Client) -> Result<(), centrifuge::Error> {
    /// use centrifuge::HistoryOptions;
    /// let last_ten = client.history("news", HistoryOptions::new().limit(10)).await?;
    /// let top = client.history("news", HistoryOptions::new()).await?; // offset + epoch only
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// As [`rpc()`](Self::rpc).
    pub async fn history(
        &self,
        channel: &str,
        opts: HistoryOptions,
    ) -> Result<HistoryResult, Error> {
        calls::call(&self.inner.shared, None, history_command(channel, &opts))
            .await
            .map(history_result)
    }

    /// Who is in a channel; no subscription needed.
    ///
    /// # Errors
    ///
    /// As [`rpc()`](Self::rpc).
    pub async fn presence(&self, channel: &str) -> Result<PresenceResult, Error> {
        calls::call(&self.inner.shared, None, presence_command(channel))
            .await
            .map(presence_result)
    }

    /// How many clients and users are in a channel; no subscription needed.
    ///
    /// # Errors
    ///
    /// As [`rpc()`](Self::rpc).
    pub async fn presence_stats(&self, channel: &str) -> Result<PresenceStatsResult, Error> {
        calls::call(&self.inner.shared, None, presence_stats_command(channel))
            .await
            .map(presence_stats_result)
    }

    /// [`rpc()`](Self::rpc) with JSON request and response.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidData`] when serializing or deserializing fails; else as
    /// `rpc()`.
    #[cfg(feature = "serde")]
    pub async fn rpc_json<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        req: &Req,
    ) -> Result<Resp, Error> {
        let data = serde_json::to_vec(req).map_err(|e| Error::InvalidData(e.to_string()))?;
        let res = self.rpc(method, data).await?;
        serde_json::from_slice(&res.data).map_err(|e| Error::InvalidData(e.to_string()))
    }

    /// [`publish()`](Self::publish) of a value serialized as JSON.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidData`] when serializing fails; else as `publish()`.
    #[cfg(feature = "serde")]
    pub async fn publish_json<T: serde::Serialize + ?Sized>(
        &self,
        channel: &str,
        value: &T,
    ) -> Result<(), Error> {
        let data = serde_json::to_vec(value).map_err(|e| Error::InvalidData(e.to_string()))?;
        self.publish(channel, data).await
    }

    /// A builder for a client-side subscription to `channel`. Nothing is
    /// registered until [`SubscriptionBuilder::build`].
    pub fn new_subscription(&self, channel: impl Into<String>) -> SubscriptionBuilder {
        SubscriptionBuilder::new(self.clone(), channel.into())
    }

    /// The registered subscription to `channel`.
    #[must_use]
    pub fn get_subscription(&self, channel: &str) -> Option<Subscription> {
        let inner = lock(&self.inner.shared.registry).get(channel).cloned()?;
        Some(Subscription::from_parts(inner, self.clone()))
    }

    /// Unsubscribes and removes a subscription from the registry. The removed
    /// handle is unusable at once; its streams end after the unsubscribe.
    ///
    /// A no-op when it isn't registered, or when a different subscription is
    /// registered for its channel.
    ///
    /// # Errors
    ///
    /// [`Error::ClientClosed`] after `close()`.
    pub fn remove_subscription(&self, sub: &Subscription) -> Result<(), Error> {
        let shared = &self.inner.shared;
        if shared.state() == State::Closed {
            return Err(Error::ClientClosed);
        }
        let inner = sub.inner();
        {
            let mut reg = lock(&shared.registry);
            match reg.get(&inner.channel) {
                Some(r) if Arc::ptr_eq(r, inner) => {
                    reg.remove(&inner.channel);
                }
                _ => return Ok(()),
            }
        }
        inner.removed.store(true, Ordering::SeqCst);
        shared.send_sub_op(inner, Op::RemoveSubscription(inner.key), |_| {
            centrifuge_client_core::machine::SubState::Unsubscribed
        });
        Ok(())
    }

    /// A copy of the registry, by channel.
    #[must_use]
    pub fn subscriptions(&self) -> HashMap<String, Subscription> {
        let reg: Vec<_> = lock(&self.inner.shared.registry)
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        reg.into_iter()
            .map(|(k, v)| (k, Subscription::from_parts(v, self.clone())))
            .collect()
    }

    /// Counters for status displays, logs and support tickets.
    #[must_use]
    pub fn statistics(&self) -> Statistics {
        let shared = &self.inner.shared;
        let subscriptions = lock(&shared.registry).len();
        let connected_since = *lock(&shared.connected_since);
        let snap = shared.snapshot.borrow();
        let s = snap.core_stats;
        Statistics {
            state: snap.state,
            transport: snap.session.as_ref().map(|s| s.kind),
            connected_since,
            connects: s.connects,
            reconnect_attempts: s.reconnect_attempts,
            bytes_in: shared.bytes_in.load(Ordering::Relaxed),
            bytes_out: shared.bytes_out.load(Ordering::Relaxed),
            messages_in: s.messages_in,
            messages_out: s.messages_out,
            publications_delivered: s.publications_delivered,
            subscriptions,
        }
    }

    /// A handle that doesn't keep the client alive.
    #[must_use]
    pub fn downgrade(&self) -> WeakClient {
        WeakClient {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Every client event as a stream, with hand-off delivery: the SDK waits
    /// until `next()` takes each item. One stream at a time: a new call ends
    /// the previous stream. It ends on `close()`.
    pub fn events(&self) -> EventStream<ClientEvent> {
        let shared = &self.inner.shared;
        let mut slot = lock(&shared.events_stream);
        let (rx, tail) = slot.open();
        if lock(&shared.close_waiters).is_none() {
            slot.close();
        }
        EventStream::new(rx, tail)
    }
}

pub(crate) fn publish_command(channel: &str, data: Bytes) -> proto::Command {
    proto::Command {
        publish: Some(proto::PublishRequest {
            channel: channel.to_owned(),
            data,
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub(crate) fn history_command(channel: &str, opts: &HistoryOptions) -> proto::Command {
    proto::Command {
        history: Some(opts.to_proto(channel)),
        ..Default::default()
    }
}

pub(crate) fn presence_command(channel: &str) -> proto::Command {
    proto::Command {
        presence: Some(proto::PresenceRequest {
            channel: channel.to_owned(),
        }),
        ..Default::default()
    }
}

pub(crate) fn presence_stats_command(channel: &str) -> proto::Command {
    proto::Command {
        presence_stats: Some(proto::PresenceStatsRequest {
            channel: channel.to_owned(),
        }),
        ..Default::default()
    }
}

pub(crate) fn history_result(r: proto::Reply) -> HistoryResult {
    r.history.map(Into::into).unwrap_or_default()
}

pub(crate) fn presence_result(r: proto::Reply) -> PresenceResult {
    r.presence.map(Into::into).unwrap_or_default()
}

#[allow(clippy::needless_pass_by_value)] // used as `.map(presence_stats_result)`
pub(crate) fn presence_stats_result(r: proto::Reply) -> PresenceStatsResult {
    r.presence_stats.map(Into::into).unwrap_or_default()
}

macro_rules! client_handlers {
    ($($(#[$doc:meta])* $name:ident => $field:ident: $ev:ty;)*) => {
        impl Client {
            $(
                $(#[$doc])*
                ///
                /// One handler per kind: registering again replaces it.
                pub fn $name(&self, f: impl FnMut(&Client, $ev) + MaybeSend + 'static) {
                    let mut h = lock(&self.inner.shared.handlers);
                    h.$field = Some(Box::new(f));
                    h.generation += 1;
                }
            )*
        }

        impl ClientBuilder {
            $(
                $(#[$doc])*
                pub fn $name(mut self, f: impl FnMut(&Client, $ev) + MaybeSend + 'static) -> Self {
                    self.handlers.$field = Some(Box::new(f));
                    self
                }
            )*
        }
    };
}

client_handlers! {
    /// The client started connecting or reconnecting.
    on_connecting => connecting: ConnectingEvent;
    /// The client connected.
    on_connected => connected: ConnectedEvent;
    /// The client is disconnected (and won't reconnect by itself).
    on_disconnected => disconnected: DisconnectedEvent;
    /// A background error: transport failures, token getter failures, …
    on_error => error: ErrorEvent;
    /// An async message from the server.
    on_message => message: MessageEvent;
    /// **Server-side subscriptions:** a server-side subscription is subscribed.
    /// Client-side subscriptions have their own `on_subscribed`.
    on_subscribed => server_subscribed: ServerSubscribedEvent;
    /// **Server-side subscriptions:** a server-side subscription waits for a
    /// reconnect.
    on_subscribing => server_subscribing: ServerSubscribingEvent;
    /// **Server-side subscriptions:** a server-side subscription ended.
    on_unsubscribed => server_unsubscribed: ServerUnsubscribedEvent;
    /// **Server-side subscriptions:** a publication in a server-side
    /// subscription.
    on_publication => server_publication: ServerPublicationEvent;
    /// **Server-side subscriptions:** a join in a server-side subscription.
    on_join => server_join: ServerJoinEvent;
    /// **Server-side subscriptions:** a leave in a server-side subscription.
    on_leave => server_leave: ServerLeaveEvent;
}

/// Builds a [`Client`]. Everything is validated in [`build()`](Self::build).
#[must_use = "a builder does nothing until build() is called"]
pub struct ClientBuilder {
    endpoints: Vec<TransportEndpoint>,
    runtime: Option<tokio::runtime::Handle>,
    protocol: Protocol,
    token: String,
    get_token: Option<ConnTokenGetter>,
    data: Bytes,
    name: String,
    version: String,
    http_headers: http::HeaderMap,
    emulated_headers: HashMap<String, String>,
    emulation_endpoint: Option<String>,
    timeout: Duration,
    handshake_timeout: Duration,
    min_reconnect_delay: Duration,
    max_reconnect_delay: Duration,
    max_server_ping_delay: Duration,
    max_message_size: usize,
    tls: TlsConfig,
    proxy: ProxyConfig,
    event_delivery: EventDelivery,
    handlers: ClientHandlers,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("endpoints", &self.endpoints)
            .field("protocol", &self.protocol)
            .field("name", &self.name)
            .field("timeout", &self.timeout)
            .field("proxy", &self.proxy)
            .field("event_delivery", &self.event_delivery)
            .finish_non_exhaustive()
    }
}

impl ClientBuilder {
    fn new(endpoints: Vec<TransportEndpoint>) -> Self {
        Self {
            endpoints,
            runtime: None,
            protocol: Protocol::default(),
            token: String::new(),
            get_token: None,
            data: Bytes::new(),
            name: "rust".into(),
            version: String::new(),
            http_headers: http::HeaderMap::new(),
            emulated_headers: HashMap::new(),
            emulation_endpoint: None,
            timeout: defaults::TIMEOUT,
            handshake_timeout: defaults::HANDSHAKE_TIMEOUT,
            min_reconnect_delay: defaults::MIN_RECONNECT_DELAY,
            max_reconnect_delay: defaults::MAX_RECONNECT_DELAY,
            max_server_ping_delay: defaults::MAX_SERVER_PING_DELAY,
            max_message_size: defaults::MAX_MESSAGE_SIZE,
            tls: TlsConfig::default(),
            proxy: ProxyConfig::default(),
            event_delivery: EventDelivery::default(),
            handlers: ClientHandlers::default(),
        }
    }

    /// The Tokio runtime to run on. Default: the current one at `build()`.
    /// Needed where `build()` runs outside a runtime (Tauri setup hooks:
    /// `tauri::async_runtime::handle()`).
    pub fn runtime(mut self, handle: tokio::runtime::Handle) -> Self {
        self.runtime = Some(handle);
        self
    }

    /// The wire protocol: [`Protocol::Protobuf`] (default) or `Protocol::Json`.
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// The connection token (JWT).
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = token.into();
        self
    }

    /// Fetches the connection token: before the first connect when no token is
    /// set, when the token expires, and after the server rejects it.
    ///
    /// `Ok(Some(t))` uses `t`; `Ok(None)` connects anonymously (on refresh:
    /// unauthorized); `Err(TokenError::Unauthorized)` stops; any other error
    /// is retried with backoff, and `?` turns errors into that.
    ///
    /// ```no_run
    /// # async fn fetch(_: &str) -> Result<String, std::io::Error> { Ok(String::new()) }
    /// let builder = centrifuge::Client::builder("wss://example.com/connection/websocket")
    ///     .get_token(|_ctx| async move {
    ///         let token = fetch("https://example.com/centrifugo/token").await?;
    ///         Ok(Some(token))
    ///     });
    /// ```
    pub fn get_token<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(ConnectionTokenContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<Option<String>, TokenError>> + MaybeSend + 'static,
    {
        self.get_token = Some(Arc::new(move |ctx| -> TokenFuture { Box::pin(f(ctx)) }));
        self
    }

    /// Connect data, sent with every connect.
    pub fn data(mut self, data: impl Into<Bytes>) -> Self {
        self.data = data.into();
        self
    }

    /// Client name (default `"rust"`, at most 16 characters).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// App version (at most 64 characters).
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Real HTTP headers: on the WebSocket upgrade, the HTTP-stream request
    /// and every emulation request.
    pub fn http_headers(mut self, headers: http::HeaderMap) -> Self {
        self.http_headers = headers;
        self
    }

    /// Headers sent inside the connect command (Centrifugo
    /// `emulated_headers`).
    pub fn emulated_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.emulated_headers = headers;
        self
    }

    /// The emulation endpoint for HTTP-streaming. Default: `/emulation` on the
    /// stream endpoint's origin.
    pub fn emulation_endpoint(mut self, url: impl Into<String>) -> Self {
        self.emulation_endpoint = Some(url.into());
        self
    }

    /// Command timeout (default 5 s).
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    /// Transport open timeout: DNS, TCP, proxy, TLS and upgrade (default 5 s).
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = d;
        self
    }

    /// Reconnect backoff minimum (default 500 ms).
    pub fn min_reconnect_delay(mut self, d: Duration) -> Self {
        self.min_reconnect_delay = d;
        self
    }

    /// Reconnect backoff maximum (default 20 s).
    pub fn max_reconnect_delay(mut self, d: Duration) -> Self {
        self.max_reconnect_delay = d;
        self
    }

    /// Slack on top of the server's ping interval before the connection is
    /// considered dead (default 10 s).
    pub fn max_server_ping_delay(mut self, d: Duration) -> Self {
        self.max_server_ping_delay = d;
        self
    }

    /// Incoming message size limit (default 64 MiB).
    pub fn max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = bytes;
        self
    }

    /// TLS configuration. Default: the OS trust store.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Proxy configuration. Default: [`ProxyConfig::System`].
    pub fn proxy(mut self, proxy: ProxyConfig) -> Self {
        self.proxy = proxy;
        self
    }

    /// Where event handlers run. Default: [`EventDelivery::Thread`].
    pub fn event_delivery(mut self, mode: EventDelivery) -> Self {
        self.event_delivery = mode;
        self
    }

    fn endpoints(&self) -> Result<Vec<(TransportKind, Url)>, Error> {
        if self.endpoints.is_empty() {
            return Err(Error::Configuration("no transport endpoint".into()));
        }
        self.endpoints
            .iter()
            .map(|e| {
                let url = Url::parse(&e.url).map_err(|err| {
                    Error::Configuration(format!("invalid endpoint {}: {err}", e.url))
                })?;
                let ok = if e.kind == TransportKind::HttpStream {
                    matches!(url.scheme(), "http" | "https")
                } else {
                    matches!(url.scheme(), "ws" | "wss")
                };
                if !ok {
                    return Err(Error::Configuration(format!(
                        "{} doesn't suit transport {:?}",
                        e.url, e.kind
                    )));
                }
                if url.host_str().is_none_or(str::is_empty) {
                    return Err(Error::Configuration(format!(
                        "endpoint {} has no host",
                        e.url
                    )));
                }
                #[cfg(not(feature = "http-stream"))]
                if e.kind == TransportKind::HttpStream {
                    return Err(Error::Configuration(
                        "HTTP-streaming needs the `http-stream` feature".into(),
                    ));
                }
                Ok((e.kind, url))
            })
            .collect()
    }

    /// Validates the configuration and starts the client's task (no
    /// connection until [`Client::connect`]).
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for invalid options (durations must be
    /// longer than zero and at most 24 hours), when there's no Tokio runtime
    /// (neither [`runtime()`](Self::runtime) nor a current one), or when the
    /// event thread of [`EventDelivery::Thread`] can't be started.
    #[allow(clippy::too_many_lines)] // validation, then wiring; clearer in one place
    pub fn build(self) -> Result<Client, Error> {
        let endpoints = self.endpoints()?;
        let runtime = match self.runtime.clone() {
            Some(h) => h,
            None => tokio::runtime::Handle::try_current().map_err(|_| {
                Error::Configuration(
                    "no Tokio runtime: call build() inside one, or pass .runtime(handle)".into(),
                )
            })?,
        };
        if self.name.chars().count() > 16 {
            return Err(Error::Configuration(
                "name is longer than 16 characters".into(),
            ));
        }
        if self.version.chars().count() > 64 {
            return Err(Error::Configuration(
                "version is longer than 64 characters".into(),
            ));
        }
        duration("timeout", self.timeout)?;
        duration("handshake_timeout", self.handshake_timeout)?;
        duration("min_reconnect_delay", self.min_reconnect_delay)?;
        duration("max_reconnect_delay", self.max_reconnect_delay)?;
        duration("max_server_ping_delay", self.max_server_ping_delay)?;
        if self.min_reconnect_delay > self.max_reconnect_delay {
            return Err(Error::Configuration(
                "min_reconnect_delay is above max_reconnect_delay".into(),
            ));
        }
        if self.max_message_size == 0 {
            return Err(Error::Configuration(
                "max_message_size must not be zero".into(),
            ));
        }
        check_http_headers(&self.http_headers)?;
        codec::validate_payload(self.protocol, "data", &self.data)
            .map_err(|e| Error::InvalidData(e.to_string()))?;
        let emulation_endpoint = match &self.emulation_endpoint {
            Some(u) => {
                let url = Url::parse(u).map_err(|e| {
                    Error::Configuration(format!("invalid emulation endpoint {u}: {e}"))
                })?;
                if !matches!(url.scheme(), "http" | "https") {
                    return Err(Error::Configuration(format!(
                        "emulation endpoint {u} isn't http(s)"
                    )));
                }
                Some(url)
            }
            None => None,
        };
        // A TLS endpoint or proxy can't work without the verifier's setup, so
        // say so here instead of retrying forever (docs/spec.md §5.2).
        #[cfg(target_os = "android")]
        let needs_tls = configured_tls(&endpoints, emulation_endpoint.as_ref(), &self.proxy);
        let net = Arc::new(Net::new(self.proxy, &self.tls, self.handshake_timeout)?);
        #[cfg(target_os = "android")]
        if needs_tls {
            net.android_tls_ready().map_err(Error::Configuration)?;
        }

        let core = Core::new(CoreConfig {
            protocol: self.protocol,
            transports: endpoints.iter().map(|(k, _)| *k).collect(),
            token: self.token,
            has_get_token: self.get_token.is_some(),
            data: self.data,
            name: self.name,
            version: self.version,
            emulated_headers: self.emulated_headers,
            timeout: self.timeout,
            handshake_timeout: self.handshake_timeout,
            min_reconnect_delay: self.min_reconnect_delay,
            max_reconnect_delay: self.max_reconnect_delay,
            max_server_ping_delay: self.max_server_ping_delay,
            max_message_size: self.max_message_size,
            seed: random_seed(),
        });

        let (tx, rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared::new(
            tx,
            CallConfig {
                protocol: self.protocol,
                timeout: self.timeout,
            },
            self.http_headers,
            net,
        ));
        *lock(&shared.handlers) = self.handlers;
        let inner = Arc::new(ClientInner {
            shared: shared.clone(),
        });
        let delivery = Delivery {
            client: Arc::downgrade(&inner),
            shared: shared.clone(),
        };
        let dispatcher = Dispatcher::new(self.event_delivery, delivery, runtime.clone())?;
        let driver = Driver::new(
            core,
            shared,
            DriverConfig {
                endpoints,
                emulation_endpoint,
                max_message_size: self.max_message_size,
                get_token: self.get_token,
            },
            dispatcher,
            rx,
        );
        runtime.spawn(driver.run());
        Ok(Client { inner })
    }
}

/// Seeds backoff jitter differently per client and process, so many clients
/// restarted together don't reconnect in lockstep.
fn random_seed() -> u64 {
    use std::hash::{BuildHasher as _, Hasher as _};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    h.finish()
}

/// Whether a connection known at `build()` uses TLS: a `wss`/`https`
/// endpoint, an `https` emulation endpoint (only HTTP-streaming uses it), or an
/// explicit `https` proxy. Proxies from the environment, the OS, a custom
/// function or `set_proxy` are only known per connection attempt, and are
/// checked there. Android refuses these at `build()` until the verifier's setup
/// (docs/spec.md §5.2).
#[cfg_attr(not(target_os = "android"), allow(dead_code))]
fn configured_tls(
    endpoints: &[(TransportKind, Url)],
    emulation: Option<&Url>,
    proxy: &ProxyConfig,
) -> bool {
    let streaming = endpoints
        .iter()
        .any(|(k, _)| *k == TransportKind::HttpStream);
    endpoints.iter().any(|(_, u)| uses_tls(u))
        || (streaming && emulation.is_some_and(uses_tls))
        || matches!(proxy, ProxyConfig::Http { url, .. } if uses_tls(url))
}

#[allow(dead_code)]
fn _assert_traits() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<Client>();
    send_sync::<WeakClient>();
    send_sync::<ConnectedEvent>();
    send_sync::<ErrorEvent>();
    send_sync::<DisconnectedEvent>();
}

#[cfg(test)]
mod tests {
    use super::{ProxyConfig, TransportKind, Url, configured_tls};

    fn ep(kind: TransportKind, url: &str) -> (TransportKind, Url) {
        (kind, Url::parse(url).unwrap())
    }

    #[test]
    fn configured_tls_covers_endpoints_emulation_and_proxy() {
        let ws = ep(TransportKind::WebSocket, "ws://h/connection/websocket");
        let wss = ep(TransportKind::WebSocket, "wss://h/connection/websocket");
        let http = ep(TransportKind::HttpStream, "http://h/connection/http-stream");
        let https = ep(
            TransportKind::HttpStream,
            "https://h/connection/http-stream",
        );
        let tls_emu = Url::parse("https://h/emulation").unwrap();
        let plain_emu = Url::parse("http://h/emulation").unwrap();
        let none = ProxyConfig::None;
        let tls_proxy = ProxyConfig::http(Url::parse("https://proxy:3128").unwrap(), None);
        let plain_proxy = ProxyConfig::http(Url::parse("http://proxy:3128").unwrap(), None);

        assert!(!configured_tls(std::slice::from_ref(&ws), None, &none));
        assert!(configured_tls(&[wss], None, &none));
        assert!(configured_tls(&[https], None, &none));
        assert!(!configured_tls(
            std::slice::from_ref(&http),
            Some(&plain_emu),
            &none
        ));
        // A plain stream endpoint with an HTTPS emulation endpoint still uses TLS.
        assert!(configured_tls(
            std::slice::from_ref(&http),
            Some(&tls_emu),
            &none
        ));
        assert!(configured_tls(&[ws.clone(), http], Some(&tls_emu), &none));
        // Without HTTP-streaming, the emulation endpoint is never used.
        assert!(!configured_tls(
            std::slice::from_ref(&ws),
            Some(&tls_emu),
            &none
        ));
        // An explicit HTTPS proxy is TLS even for ws:// endpoints.
        assert!(configured_tls(std::slice::from_ref(&ws), None, &tls_proxy));
        assert!(!configured_tls(
            std::slice::from_ref(&ws),
            None,
            &plain_proxy
        ));
        // Proxies only known per attempt are checked at connect time instead.
        assert!(!configured_tls(&[ws], None, &ProxyConfig::System));
    }
}
