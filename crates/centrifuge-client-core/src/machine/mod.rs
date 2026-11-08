//! The sans-IO state machine (docs/spec.md §9.2).
//!
//! [`Core`] owns all client and subscription state. The driver feeds it one
//! [`Input`] at a time with the current time and acts on the [`Outputs`] it
//! returns: events to deliver, commands to write, transports to open or close,
//! callbacks to start. It never does IO, never reads a clock and holds no locks,
//! so every behaviour is testable step by step.
//!
//! Each `Input` corresponds to a step of the SDK TLA+ model (`tla/sdk`):
//! `App_*` → [`Input::App`], `Env_*` → transport inputs, `Cb_*` → callback
//! results, `Sdk_*` → replies, pushes, timers and [`Input::DeliverNext`].

// Proto messages are handed over by value on purpose (they're consumed piecewise),
// and step handlers mirror the TLA+ steps one function each.
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
mod conn;
#[allow(clippy::needless_pass_by_value)]
mod server;
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
mod sub;
mod timers;

#[cfg(all(test, feature = "json"))]
#[allow(
    clippy::too_many_lines,
    clippy::struct_excessive_bools,
    clippy::unused_self,
    clippy::print_stdout,
    clippy::panic
)]
mod replay;
#[cfg(test)]
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
mod tests;

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use crate::time::Instant;

use bytes::Bytes;

use crate::codec::{DecodeError, Protocol};
use crate::filter::FilterNode;
use crate::proto;
use crate::types::{ClientInfo, Publication, StreamPosition};

pub use timers::TimerKey;
use timers::{Rng, Timers};

/// Identifies a subscription object (not a channel: a removed subscription and
/// a new one to the same channel have different keys).
pub type SubKey = u64;
/// Identifies one app callback invocation.
pub type FlowId = u64;
/// Identifies one transport (connection attempt).
pub type TransportId = u64;

/// A shareable error from app code or the transport.
pub type ArcError = Arc<dyn std::error::Error + Send + Sync + 'static>;

/// The transports the SDK speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TransportKind {
    /// WebSocket.
    WebSocket,
    /// HTTP-streaming with bidirectional emulation.
    HttpStream,
}

impl TransportKind {
    /// Whether commands go through the emulation endpoint.
    #[must_use]
    pub fn uses_emulation(self) -> bool {
        matches!(self, Self::HttpStream)
    }
}

/// Client state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum State {
    /// Not connected and not trying to.
    Disconnected,
    /// Trying to connect (or reconnect).
    Connecting,
    /// Connected.
    Connected,
    /// Closed for good.
    Closed,
}

/// Subscription state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SubState {
    /// Not subscribed and not trying to.
    Unsubscribed,
    /// Trying to subscribe (or resubscribe).
    Subscribing,
    /// Subscribed.
    Subscribed,
}

/// Kinds of background client errors (docs/spec.md §4.7).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientErrorKind {
    /// A transport failed to open, read or write.
    #[non_exhaustive]
    Transport {
        /// Which transport.
        transport: TransportKind,
        /// HTTP status, when there was one.
        status: Option<u16>,
    },
    /// The connection token getter failed before a connect.
    ConnectToken,
    /// The server answered the connect command with an error.
    Connect,
    /// A connection token refresh failed.
    Refresh,
    /// Configuration that no retry can fix.
    Configuration,
    /// Undecodable data after the connection was proven, or a message over
    /// the size limit.
    Protocol,
    /// A publication's delta couldn't be applied.
    #[non_exhaustive]
    Delta {
        /// The channel.
        channel: String,
        /// The publication's offset.
        offset: u64,
    },
    /// An event handler panicked.
    HandlerPanicked,
}

/// Kinds of background subscription errors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionErrorKind {
    /// The server answered the subscribe command with a temporary error.
    Subscribe,
    /// The subscription token getter failed.
    Token,
    /// The `get_state` callback failed.
    GetState,
    /// A subscription token refresh failed.
    Refresh,
    /// Configuration that no retry can fix.
    Configuration,
}

/// What a `subscribed` event reports.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
#[allow(clippy::struct_excessive_bools)] // independent protocol flags
pub struct SubscribedInfo {
    /// A recovery was attempted.
    pub was_recovering: bool,
    /// The recovery got every missed publication.
    pub recovered: bool,
    /// The subscription is recoverable.
    pub recoverable: bool,
    /// The subscription is positioned.
    pub positioned: bool,
    /// The stream position, for positioned or recoverable subscriptions.
    pub stream_position: Option<StreamPosition>,
    /// Data from the server's subscribe reply.
    pub data: Bytes,
}

/// An event for the app, in the order of the state changes.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Event {
    /// Client → connecting.
    Connecting {
        /// Code (docs/spec.md §6.5).
        code: u32,
        /// Reason.
        reason: String,
    },
    /// Client → connected.
    Connected {
        /// Client ID the server assigned.
        client_id: String,
        /// Server version.
        version: String,
        /// Data from the connect reply.
        data: Bytes,
        /// The transport in use.
        transport: TransportKind,
    },
    /// Client → disconnected.
    Disconnected {
        /// Code.
        code: u32,
        /// Reason.
        reason: String,
    },
    /// A background client error.
    Error {
        /// What failed.
        kind: ClientErrorKind,
        /// A description.
        message: String,
        /// The underlying error, if any.
        source: Option<ArcError>,
    },
    /// An async message from the server.
    Message {
        /// The payload.
        data: Bytes,
    },
    /// A server-side subscription was subscribed.
    ServerSubscribed {
        /// The channel.
        channel: String,
        /// Details.
        info: SubscribedInfo,
    },
    /// A server-side subscription is waiting for a reconnect.
    ServerSubscribing {
        /// The channel.
        channel: String,
    },
    /// A server-side subscription ended.
    ServerUnsubscribed {
        /// The channel.
        channel: String,
    },
    /// A publication in a server-side subscription.
    ServerPublication {
        /// The channel.
        channel: String,
        /// The publication.
        publication: Publication,
    },
    /// A join in a server-side subscription.
    ServerJoin {
        /// The channel.
        channel: String,
        /// Who joined.
        info: ClientInfo,
    },
    /// A leave in a server-side subscription.
    ServerLeave {
        /// The channel.
        channel: String,
        /// Who left.
        info: ClientInfo,
    },
    /// Subscription → subscribing.
    Subscribing {
        /// The subscription.
        sub: SubKey,
        /// Code.
        code: u32,
        /// Reason.
        reason: String,
    },
    /// Subscription → subscribed.
    Subscribed {
        /// The subscription.
        sub: SubKey,
        /// Details.
        info: SubscribedInfo,
    },
    /// Subscription → unsubscribed.
    Unsubscribed {
        /// The subscription.
        sub: SubKey,
        /// Code.
        code: u32,
        /// Reason.
        reason: String,
    },
    /// A background subscription error.
    SubscriptionError {
        /// The subscription.
        sub: SubKey,
        /// What failed.
        kind: SubscriptionErrorKind,
        /// A description.
        message: String,
        /// The underlying error, if any.
        source: Option<ArcError>,
    },
    /// A publication in a client-side subscription.
    Publication {
        /// The subscription.
        sub: SubKey,
        /// The publication.
        publication: Publication,
    },
    /// A join in a client-side subscription.
    Join {
        /// The subscription.
        sub: SubKey,
        /// Who joined.
        info: ClientInfo,
    },
    /// A leave in a client-side subscription.
    Leave {
        /// The subscription.
        sub: SubKey,
        /// Who left.
        info: ClientInfo,
    },
}

/// Client configuration the core needs.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // independent protocol flags
pub struct CoreConfig {
    /// Wire protocol.
    pub protocol: Protocol,
    /// Transports in fallback order (at least one).
    pub transports: Vec<TransportKind>,
    /// Initial connection token.
    pub token: String,
    /// Whether a connection token getter is set.
    pub has_get_token: bool,
    /// Connect data.
    pub data: Bytes,
    /// Client name.
    pub name: String,
    /// Client version.
    pub version: String,
    /// Emulated headers, sent in the connect command.
    pub emulated_headers: HashMap<String, String>,
    /// Command timeout.
    pub timeout: Duration,
    /// Transport open timeout.
    pub handshake_timeout: Duration,
    /// Reconnect backoff minimum.
    pub min_reconnect_delay: Duration,
    /// Reconnect backoff maximum.
    pub max_reconnect_delay: Duration,
    /// Slack added to the server's ping interval.
    pub max_server_ping_delay: Duration,
    /// Incoming message size limit (also caps delta outputs).
    pub max_message_size: usize,
    /// Seed for backoff jitter.
    pub seed: u64,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            protocol: Protocol::default(),
            transports: vec![TransportKind::WebSocket],
            token: String::new(),
            has_get_token: false,
            data: Bytes::new(),
            name: "rust".into(),
            version: String::new(),
            emulated_headers: HashMap::new(),
            timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            min_reconnect_delay: Duration::from_millis(500),
            max_reconnect_delay: Duration::from_secs(20),
            max_server_ping_delay: Duration::from_secs(10),
            max_message_size: 64 << 20,
            seed: 0x5EED,
        }
    }
}

/// Subscription configuration.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // independent protocol flags
pub struct SubConfig {
    /// The channel.
    pub channel: String,
    /// Initial subscription token.
    pub token: String,
    /// Whether a subscription token getter is set.
    pub has_get_token: bool,
    /// Subscribe data.
    pub data: Bytes,
    /// Ask for a positioned subscription.
    pub positioned: bool,
    /// Ask for a recoverable subscription.
    pub recoverable: bool,
    /// Ask for join/leave pushes.
    pub join_leave: bool,
    /// Recover from this position on the first subscribe.
    pub since: Option<StreamPosition>,
    /// Ask for fossil delta compression.
    pub delta: bool,
    /// Server-side tags filter.
    pub tags_filter: Option<FilterNode>,
    /// Whether a `get_state` callback is set.
    pub has_get_state: bool,
    /// Resubscribe backoff minimum.
    pub min_resubscribe_delay: Duration,
    /// Resubscribe backoff maximum.
    pub max_resubscribe_delay: Duration,
}

impl SubConfig {
    /// Defaults for `channel`.
    #[must_use]
    pub fn new(channel: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            token: String::new(),
            has_get_token: false,
            data: Bytes::new(),
            positioned: false,
            recoverable: false,
            join_leave: false,
            since: None,
            delta: false,
            tags_filter: None,
            has_get_state: false,
            min_resubscribe_delay: Duration::from_millis(500),
            max_resubscribe_delay: Duration::from_secs(20),
        }
    }
}

/// App operations (TLA+ `App_*`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Op {
    /// `connect()`.
    Connect,
    /// `disconnect()`.
    Disconnect,
    /// `close()`.
    Close,
    /// `set_token()`.
    SetToken(String),
    /// `set_data()`.
    SetData(Bytes),
    /// `set_emulated_headers()`.
    SetEmulatedHeaders(HashMap<String, String>),
    /// A subscription was created (registered by the client layer).
    NewSubscription {
        /// Its key.
        key: SubKey,
        /// Its configuration.
        config: Box<SubConfig>,
    },
    /// `sub.subscribe()`.
    Subscribe(SubKey),
    /// `sub.unsubscribe()`.
    Unsubscribe(SubKey),
    /// `remove_subscription()`.
    RemoveSubscription(SubKey),
    /// `sub.set_data()`.
    SetSubscriptionData(SubKey, Bytes),
    /// `sub.set_tags_filter()`.
    SetTagsFilter(SubKey, Option<FilterNode>),
}

/// Why a transport ended.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CloseReason {
    /// A WebSocket close frame.
    Frame {
        /// Close code.
        code: u16,
        /// Close reason text.
        reason: String,
    },
    /// The stream or socket ended without a close frame.
    Eof,
    /// The transport failed (open, read or write), maybe with an HTTP status.
    Failed {
        /// HTTP status, if the failure was an HTTP response.
        status: Option<u16>,
        /// A description.
        message: String,
        /// The underlying error.
        source: Option<ArcError>,
    },
    /// Data that doesn't decode as the protocol.
    Undecodable(DecodeError),
    /// An incoming message over the size limit.
    TooLarge,
}

/// The result of an app callback.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CallbackError {
    /// The app said: stop, not allowed.
    Unauthorized,
    /// Any other failure: retried.
    Other(ArcError),
}

/// A step input.
#[derive(Debug)]
#[non_exhaustive]
pub enum Input {
    /// An app operation.
    App(Op),
    /// A transport opened (WebSocket upgraded, HTTP-stream response headers).
    Opened {
        /// Which transport.
        transport: TransportId,
    },
    /// One item of a received frame.
    Item {
        /// Which transport.
        transport: TransportId,
        /// The reply or push.
        reply: Box<proto::Reply>,
    },
    /// A transport ended.
    Closed {
        /// Which transport.
        transport: TransportId,
        /// Why.
        reason: CloseReason,
    },
    /// Commands that couldn't be written without the transport closing (an
    /// emulation request rejected on its own, e.g. HTTP 413).
    WriteFailed {
        /// Which transport.
        transport: TransportId,
        /// Ids of the commands in that frame.
        command_ids: Vec<u32>,
        /// Whether the frame held a pong.
        had_pong: bool,
    },
    /// A token getter settled.
    TokenSettled {
        /// Which invocation.
        flow: FlowId,
        /// The token, `None` for "no token".
        result: Result<Option<String>, CallbackError>,
    },
    /// A `get_state` callback settled.
    StateSettled {
        /// Which invocation.
        flow: FlowId,
        /// The position.
        result: Result<StreamPosition, CallbackError>,
    },
    /// A deadline passed.
    TimerFired {
        /// Which.
        key: TimerKey,
        /// The driver ran it more than 1 s late (the process was suspended).
        late: bool,
        /// When the current transport last delivered a decoded message.
        last_received: Option<Instant>,
    },
    /// Deliver the next queued delivery (recovered publications, server-side
    /// subscriptions of a connect reply). The driver sends this while
    /// [`Core::has_pending_delivery`] is true, before inbound items.
    DeliverNext,
    /// The previous step's events were delivered.
    Delivered {
        /// `false` when a handler panicked.
        ok: bool,
        /// The panic message, if any.
        panic: Option<String>,
    },
    /// The wall clock jumped ahead of the monotonic one by `slept` (system
    /// sleep; docs/spec.md §9.7).
    Resumed {
        /// How long the system slept.
        slept: Duration,
    },
    /// A browser lifecycle event; native targets never send it.
    Lifecycle(Lifecycle),
}

/// Browser lifecycle events, acted on as centrifuge-js does
/// (docs/wasm-plan.md, step 7a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lifecycle {
    /// `offline`: the transport is closed, and the client is connecting.
    Offline,
    /// `online`: reconnect now instead of waiting out the backoff; after an
    /// `Offline`, a transport opened meanwhile is replaced (mobile Safari may
    /// never report it closed).
    Online,
    /// `pagehide` with `persisted`: the page enters the back/forward cache,
    /// frozen; its transport is closed, so the server sees the client go.
    PageHidden,
    /// `pageshow` with `persisted`: the page is restored; reconnect now.
    PageShown,
}

/// A transport to open.
#[derive(Debug, Clone)]
pub struct OpenRequest {
    /// Its id.
    pub transport: TransportId,
    /// Which kind.
    pub kind: TransportKind,
    /// Index into the configured transport list.
    pub index: usize,
    /// For emulation: the connect command, sent as the open request's body.
    pub initial: Option<proto::Command>,
}

/// An app callback to start.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Callback {
    /// Call the connection token getter.
    ConnectionToken {
        /// Report the result with this flow.
        flow: FlowId,
    },
    /// Call the subscription token getter.
    SubscriptionToken {
        /// Report the result with this flow.
        flow: FlowId,
        /// For which subscription.
        sub: SubKey,
        /// Its channel.
        channel: String,
    },
    /// Call `get_state`.
    GetState {
        /// Report the result with this flow.
        flow: FlowId,
        /// For which subscription.
        sub: SubKey,
        /// Its channel.
        channel: String,
    },
}

/// What a step asks the driver to do.
#[derive(Debug, Default)]
pub struct Outputs {
    /// Events to deliver, in order.
    pub events: Vec<Event>,
    /// Commands to write.
    pub commands: Vec<(TransportId, proto::Command)>,
    /// A transport to open.
    pub open: Option<OpenRequest>,
    /// Transports to close (gracefully, in the background).
    pub close: Vec<TransportId>,
    /// Callbacks to start.
    pub callbacks: Vec<Callback>,
    /// Fail the fast-path calls of these transports, after the events.
    pub fail_calls: Vec<TransportId>,
}

impl Outputs {
    fn event(&mut self, e: Event) {
        self.events.push(e);
    }
}

/// The connected session, for the call fast path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// The transport.
    pub transport: TransportId,
    /// Its kind.
    pub kind: TransportKind,
    /// Emulation session.
    pub session: String,
    /// Emulation node.
    pub node: String,
}

/// A queued delivery (docs/spec.md §9.4).
#[derive(Debug)]
enum Delivery {
    /// A recovered publication of a client-side subscription.
    SubPublication {
        sub: SubKey,
        session: u64,
        publication: proto::Publication,
        data: Bytes,
    },
    /// A server-side subscription of a connect reply.
    ServerSubscribed {
        attempt: u64,
        channel: String,
        result: Box<proto::SubscribeResult>,
    },
    /// A recovered publication of a server-side subscription.
    ServerPublication {
        attempt: u64,
        channel: String,
        publication: proto::Publication,
    },
}

/// A position move to apply once the step's events are delivered.
#[derive(Debug)]
enum Commit {
    Sub {
        sub: SubKey,
        session: u64,
        offset: u64,
        epoch: Option<String>,
    },
    Server {
        channel: String,
        offset: u64,
        epoch: Option<String>,
    },
}

/// What a core-issued command is waiting for.
#[derive(Debug, Clone)]
enum Pending {
    Connect {
        attempt: u64,
    },
    Subscribe {
        sub: SubKey,
        channel: String,
        attempt: u64,
        generation: u64,
    },
    Unsubscribe {
        channel: String,
        generation: u64,
    },
    Refresh {
        generation: u64,
    },
    SubRefresh {
        sub: SubKey,
        session: u64,
        invalidations: u64,
    },
}

/// What an app callback invocation belongs to.
#[derive(Debug, Clone)]
enum Flow {
    ConnectToken {
        attempt: u64,
    },
    RefreshToken {
        generation: u64,
    },
    SubToken {
        sub: SubKey,
        attempt: u64,
        invalidations: u64,
    },
    SubRefreshToken {
        sub: SubKey,
        session: u64,
        invalidations: u64,
    },
    SubState {
        sub: SubKey,
        attempt: u64,
        invalidations: u64,
    },
}

/// The state machine. See the module docs.
#[derive(Debug)]
pub struct Core {
    cfg: CoreConfig,
    rng: Rng,
    timers: Timers,
    state: State,
    conn: conn::Conn,
    subs: HashMap<SubKey, sub::Sub>,
    /// Subscriptions whose flow may be able to start.
    ready: BTreeSet<SubKey>,
    /// Per channel: unsubscribe replies awaited, and a subscribe command whose
    /// unsubscribe is deferred (docs/spec.md §7.3).
    channels: HashMap<String, sub::ChannelBook>,
    by_push_id: HashMap<i64, SubKey>,
    server_subs: HashMap<String, server::ServerSub>,
    deliveries: VecDeque<Delivery>,
    commits: Vec<Commit>,
    pending: HashMap<u32, Pending>,
    flows: HashMap<FlowId, Flow>,
    next_flow: FlowId,
    next_cmd_id: u32,
    /// Bumped on each entry into `disconnected` and each `disconnect()`.
    disconnects: u64,
    /// Bumped whenever a connected session ends.
    generation: u64,
    graced: BTreeSet<TimerKey>,
    stats: CoreStats,
}

/// Counters for `Client::statistics()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CoreStats {
    /// Successful connects, reconnects included.
    pub connects: u64,
    /// Connect attempts that didn't end in a connect.
    pub reconnect_attempts: u64,
    /// Decoded replies and pushes.
    pub messages_in: u64,
    /// Commands the core wrote.
    pub messages_out: u64,
    /// Publications whose delivery was committed.
    pub publications_delivered: u64,
}

/// Command ids the core uses; the fast path uses the upper half.
const CORE_ID_LIMIT: u32 = 1 << 31;

impl Core {
    /// A disconnected client.
    #[must_use]
    pub fn new(cfg: CoreConfig) -> Self {
        let rng = Rng::new(cfg.seed);
        let conn = conn::Conn::new(&cfg);
        Self {
            cfg,
            rng,
            timers: Timers::default(),
            state: State::Disconnected,
            conn,
            subs: HashMap::new(),
            ready: BTreeSet::new(),
            channels: HashMap::new(),
            by_push_id: HashMap::new(),
            server_subs: HashMap::new(),
            deliveries: VecDeque::new(),
            commits: Vec::new(),
            pending: HashMap::new(),
            flows: HashMap::new(),
            next_flow: 0,
            next_cmd_id: 0,
            disconnects: 0,
            generation: 0,
            graced: BTreeSet::new(),
            stats: CoreStats::default(),
        }
    }

    /// Runs one step.
    pub fn step(&mut self, input: Input, now: Instant) -> Outputs {
        let mut out = Outputs::default();
        match input {
            Input::App(op) => self.on_op(op, now, &mut out),
            Input::Opened { transport } => self.on_opened(transport, now, &mut out),
            Input::Item { transport, reply } => self.on_item(transport, *reply, now, &mut out),
            Input::Closed { transport, reason } => self.on_closed(transport, reason, now, &mut out),
            Input::WriteFailed {
                transport,
                command_ids,
                had_pong,
            } => {
                self.on_write_failed(transport, &command_ids, had_pong, now, &mut out);
            }
            Input::TokenSettled { flow, result } => self.on_token(flow, result, now, &mut out),
            Input::StateSettled { flow, result } => self.on_state(flow, result, now, &mut out),
            Input::TimerFired {
                key,
                late,
                last_received,
            } => self.on_timer(key, late, last_received, now, &mut out),
            Input::DeliverNext => self.deliver_next(now, &mut out),
            Input::Delivered { ok, panic } => self.on_delivered(ok, panic, now, &mut out),
            Input::Resumed { slept } => self.on_resumed(slept, now, &mut out),
            Input::Lifecycle(e) => self.on_lifecycle(e, now, &mut out),
        }
        self.pump(now, &mut out);
        self.stats.messages_out += out.commands.len() as u64;
        out
    }

    /// The earliest deadline, for the driver's single sleep.
    #[must_use]
    pub fn next_deadline(&self) -> Option<(Instant, TimerKey)> {
        self.timers.next()
    }

    /// Whether [`Input::DeliverNext`] has something to deliver.
    #[must_use]
    pub fn has_pending_delivery(&self) -> bool {
        !self.deliveries.is_empty()
    }

    /// The client state.
    #[must_use]
    pub fn state(&self) -> State {
        self.state
    }

    /// Bumped on each entry into `disconnected` and each `disconnect()`.
    #[must_use]
    pub fn disconnects(&self) -> u64 {
        self.disconnects
    }

    /// Bumped whenever a connected session ends.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The connected session, while connected.
    #[must_use]
    pub fn session(&self) -> Option<Session> {
        if self.state != State::Connected {
            return None;
        }
        self.conn.session()
    }

    /// The current transport, while one is open or opening.
    #[must_use]
    pub fn current_transport(&self) -> Option<(TransportId, TransportKind)> {
        self.conn.current.as_ref().map(|t| (t.id, t.kind))
    }

    /// A subscription's state, session counter and unsubscribe counter.
    #[must_use]
    pub fn subscription(&self, key: SubKey) -> Option<(SubState, u64, u64)> {
        self.subs
            .get(&key)
            .map(|s| (s.state, s.session, s.unsubscribes))
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> CoreStats {
        self.stats
    }

    /// The configured protocol.
    #[must_use]
    pub fn protocol(&self) -> Protocol {
        self.cfg.protocol
    }

    fn next_id(&mut self) -> u32 {
        loop {
            self.next_cmd_id = if self.next_cmd_id + 1 >= CORE_ID_LIMIT {
                1
            } else {
                self.next_cmd_id + 1
            };
            if !self.pending.contains_key(&self.next_cmd_id) {
                return self.next_cmd_id;
            }
        }
    }

    fn new_flow(&mut self, flow: Flow) -> FlowId {
        self.next_flow += 1;
        self.flows.insert(self.next_flow, flow);
        self.next_flow
    }

    /// Writes a core command with a reply deadline.
    fn send(
        &mut self,
        transport: TransportId,
        mut cmd: proto::Command,
        pending: Pending,
        now: Instant,
        out: &mut Outputs,
    ) -> u32 {
        let id = self.next_id();
        cmd.id = id;
        self.pending.insert(id, pending);
        self.timers
            .arm_after(TimerKey::Command(id), now, self.cfg.timeout);
        out.commands.push((transport, cmd));
        id
    }

    fn on_op(&mut self, op: Op, now: Instant, out: &mut Outputs) {
        if self.state == State::Closed {
            return;
        }
        match op {
            Op::Connect => self.op_connect(now, out),
            Op::Disconnect => self.op_disconnect(now, out),
            Op::Close => self.op_close(now, out),
            Op::SetToken(t) => {
                self.conn.token = t;
                if !self.cfg.has_get_token {
                    self.conn.refresh_required = false;
                }
            }
            Op::SetData(d) => self.cfg.data = d,
            Op::SetEmulatedHeaders(h) => self.cfg.emulated_headers = h,
            Op::NewSubscription { key, config } => {
                self.subs.insert(key, sub::Sub::new(key, *config));
            }
            Op::Subscribe(key) => self.op_subscribe(key, out),
            Op::Unsubscribe(key) => self.op_unsubscribe(key, now, out),
            Op::RemoveSubscription(key) => {
                self.op_unsubscribe(key, now, out);
                self.subs.remove(&key);
                self.ready.remove(&key);
                self.timers.cancel(TimerKey::Resubscribe(key));
                self.timers.cancel(TimerKey::SubRefresh(key));
            }
            Op::SetSubscriptionData(key, d) => {
                if let Some(s) = self.subs.get_mut(&key) {
                    s.cfg.data = d;
                }
            }
            Op::SetTagsFilter(key, f) => {
                if let Some(s) = self.subs.get_mut(&key) {
                    if s.state == SubState::Unsubscribed {
                        s.cfg.tags_filter = f;
                    }
                }
            }
        }
    }

    fn on_timer(
        &mut self,
        key: TimerKey,
        late: bool,
        last_received: Option<Instant>,
        now: Instant,
        out: &mut Outputs,
    ) {
        if !self.timers.take_due(key, now) {
            return;
        }
        let grace = Duration::from_secs(1);
        match key {
            // A deadline that ran far too late (suspended process) gets one grace
            // period, so data that arrived meanwhile is processed first
            // (patterns 28, 55).
            TimerKey::Command(_) | TimerKey::ConnectReply | TimerKey::NoPing | TimerKey::Open
                if late && self.graced.insert(key) =>
            {
                self.timers.arm_after(key, now, grace);
                return;
            }
            _ => {}
        }
        // A command keeps its mark until it ends (reply, timeout, teardown):
        // the busy-connection grace below counts too, so it's granted once
        // (sdk_validation chaos-fake seed 195: re-granted on every fire, an
        // unanswered subscribe stayed pending forever while pings arrived).
        if !matches!(key, TimerKey::Command(_)) {
            self.graced.remove(&key);
        }
        match key {
            TimerKey::Reconnect => self.start_attempt(now, out),
            TimerKey::Open => self.on_open_timeout(now, out),
            TimerKey::ConnectReply => self.on_connect_timeout(now, out),
            TimerKey::NoPing => self.on_no_ping(last_received, now, out),
            TimerKey::Refresh => self.on_refresh_timer(now, out),
            TimerKey::Command(id) => self.on_command_timeout(id, last_received, now, out),
            TimerKey::Resubscribe(k) => {
                self.ready.insert(k);
            }
            TimerKey::SubRefresh(k) => self.on_sub_refresh_timer(k, out),
            TimerKey::BackoffReset => self.conn.reconnect_attempts = 0,
        }
    }

    fn on_command_timeout(
        &mut self,
        id: u32,
        last_received: Option<Instant>,
        now: Instant,
        out: &mut Outputs,
    ) {
        // A reply may wait behind a slow handler in a busy read-ahead window:
        // while data keeps arriving, give it one more second, once (§6.4).
        if let Some(at) = last_received {
            if now.duration_since(at) < Duration::from_secs(1)
                && self.graced.insert(TimerKey::Command(id))
            {
                self.timers
                    .arm_after(TimerKey::Command(id), now, Duration::from_secs(1));
                return;
            }
        }
        self.graced.remove(&TimerKey::Command(id));
        let Some(p) = self.pending.remove(&id) else {
            return;
        };
        match p {
            Pending::Connect { .. } => {}
            Pending::Subscribe {
                sub,
                channel,
                attempt,
                generation,
            } => {
                self.on_subscribe_timeout(sub, &channel, attempt, generation, id, now, out);
            }
            Pending::Unsubscribe {
                channel,
                generation,
            } => self.on_unsubscribe_failed(&channel, generation, now, out),
            Pending::Refresh { generation } => self.on_refresh_failed(generation, None, now, out),
            Pending::SubRefresh {
                sub,
                session,
                invalidations,
            } => {
                self.on_sub_refresh_failed(sub, session, invalidations, None, now, out);
            }
        }
    }

    fn on_write_failed(
        &mut self,
        transport: TransportId,
        ids: &[u32],
        had_pong: bool,
        now: Instant,
        out: &mut Outputs,
    ) {
        if !self.conn.is_current(transport) {
            return;
        }
        if had_pong {
            // A missing pong would make the server disconnect us anyway.
            self.transport_lost(
                transport,
                crate::codes::connecting::TRANSPORT_CLOSED,
                "transport closed",
                now,
                out,
            );
            return;
        }
        for id in ids {
            self.timers.cancel(TimerKey::Command(*id));
            let Some(p) = self.pending.remove(id) else {
                continue;
            };
            match p {
                Pending::Connect { .. } => {
                    self.transport_lost(
                        transport,
                        crate::codes::connecting::TRANSPORT_CLOSED,
                        "transport closed",
                        now,
                        out,
                    );
                    return;
                }
                Pending::Subscribe {
                    sub,
                    channel,
                    attempt,
                    generation,
                } => {
                    // Never reached the server: a deferred unsubscribe of it
                    // has nothing to undo (TLA+ `Env_WriteError`).
                    if generation == self.generation {
                        self.settle_outdated_subscribe(&channel, *id, false, now, out);
                    }
                    self.on_subscribe_write_failed(sub, attempt, generation, now, out);
                }
                Pending::Unsubscribe {
                    channel,
                    generation,
                } => {
                    self.on_unsubscribe_failed(&channel, generation, now, out);
                    return;
                }
                Pending::Refresh { generation } => {
                    self.on_refresh_failed(generation, None, now, out);
                }
                Pending::SubRefresh {
                    sub,
                    session,
                    invalidations,
                } => {
                    self.on_sub_refresh_failed(sub, session, invalidations, None, now, out);
                }
            }
        }
    }

    fn deliver_next(&mut self, _now: Instant, out: &mut Outputs) {
        while let Some(d) = self.deliveries.pop_front() {
            if self.deliver(d, out) {
                return;
            }
        }
    }

    /// Delivers one queued item; returns whether it produced an event.
    fn deliver(&mut self, d: Delivery, out: &mut Outputs) -> bool {
        match d {
            Delivery::SubPublication {
                sub,
                session,
                publication,
                data,
            } => {
                let Some(s) = self.subs.get(&sub) else {
                    return false;
                };
                if s.state != SubState::Subscribed || s.session != session {
                    return false;
                }
                let channel_is_other =
                    !publication.channel.is_empty() && publication.channel != s.cfg.channel;
                if publication.offset > 0 && !channel_is_other {
                    let epoch = (!publication.epoch.is_empty()).then(|| publication.epoch.clone());
                    self.commits.push(Commit::Sub {
                        sub,
                        session,
                        offset: publication.offset,
                        epoch,
                    });
                }
                out.event(Event::Publication {
                    sub,
                    publication: Publication::from_proto(publication, data),
                });
                true
            }
            Delivery::ServerSubscribed {
                attempt,
                channel,
                result,
            } => {
                if !self.conn.is_connected_attempt(attempt, self.state) {
                    self.deliveries.clear();
                    return false;
                }
                self.server_subscribed_from_reply(attempt, &channel, *result, out);
                true
            }
            Delivery::ServerPublication {
                attempt,
                channel,
                publication,
            } => {
                if !self.conn.is_connected_attempt(attempt, self.state)
                    || !self.server_subs.contains_key(&channel)
                {
                    return false;
                }
                if publication.offset > 0 {
                    let epoch = (!publication.epoch.is_empty()).then(|| publication.epoch.clone());
                    self.commits.push(Commit::Server {
                        channel: channel.clone(),
                        offset: publication.offset,
                        epoch,
                    });
                }
                let data = publication.data.clone();
                out.event(Event::ServerPublication {
                    channel,
                    publication: Publication::from_proto(publication, data),
                });
                true
            }
        }
    }

    fn on_delivered(&mut self, ok: bool, panic: Option<String>, now: Instant, out: &mut Outputs) {
        let commits = std::mem::take(&mut self.commits);
        if !ok {
            // A panicking handler: nothing of the batch counts as delivered, and
            // the client stops loudly (docs/spec.md §9.4).
            out.event(Event::Error {
                kind: ClientErrorKind::HandlerPanicked,
                message: panic.unwrap_or_else(|| "event handler panicked".into()),
                source: None,
            });
            self.terminate(
                crate::codes::disconnected::BAD_PROTOCOL,
                "handler panicked",
                now,
                out,
            );
            return;
        }
        for c in commits {
            match c {
                Commit::Sub {
                    sub,
                    session,
                    offset,
                    epoch,
                } => {
                    if let Some(s) = self.subs.get_mut(&sub) {
                        if s.session == session {
                            s.commit(offset, epoch);
                            self.stats.publications_delivered += 1;
                        }
                    }
                }
                Commit::Server {
                    channel,
                    offset,
                    epoch,
                } => {
                    if let Some(ss) = self.server_subs.get_mut(&channel) {
                        ss.commit(offset, epoch);
                        self.stats.publications_delivered += 1;
                    }
                }
            }
        }
    }

    /// Starts subscribe flows that became possible during the step.
    fn pump(&mut self, now: Instant, out: &mut Outputs) {
        if self.state != State::Connected {
            return;
        }
        let ready = std::mem::take(&mut self.ready);
        for key in ready {
            self.maybe_start_flow(key, now, out);
        }
    }
}
