//! Connection behaviour (docs/spec.md §5.8, §6).

use std::time::Duration;

use crate::time::Instant;

use super::timers::backoff;
use super::{
    CallbackError, ClientErrorKind, CloseReason, Core, CoreConfig, Event, Flow, OpenRequest,
    Outputs, Pending, Session, State, SubState, TimerKey, TransportId, TransportKind,
};
use crate::codes::{connecting, disconnected};
use crate::{codes, proto};

/// The transport of the current attempt.
#[derive(Debug, Clone)]
pub(super) struct Transport {
    pub(super) id: TransportId,
    pub(super) kind: TransportKind,
    /// The open succeeded (upgrade done / response headers).
    pub(super) opened: bool,
    /// A message was decoded from it.
    pub(super) got_message: bool,
    /// The connect command's id, while awaiting its reply.
    pub(super) connect_id: Option<u32>,
}

/// Connection-level state of [`Core`].
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)] // independent protocol flags
pub(super) struct Conn {
    pub(super) token: String,
    pub(super) refresh_required: bool,
    /// The token the current connect command carries (pattern 58).
    pub(super) connect_token: String,
    pub(super) attempt: u64,
    pub(super) next_transport: TransportId,
    pub(super) current: Option<Transport>,
    pub(super) session: String,
    pub(super) node: String,
    /// Client-side refresh mode: the connect or last refresh reply had `expires`.
    pub(super) client_refresh: bool,
    pub(super) token_expires_at: Option<Instant>,
    pub(super) ping_window: Option<Duration>,
    pub(super) pong: bool,
    pub(super) reconnect_attempts: u32,
    pub(super) reset_pending: bool,
    /// Fallback (docs/spec.md §5.8).
    pub(super) index: usize,
    pub(super) transport_was_open: bool,
    pub(super) tried_all: bool,
    /// The last attempt failed on an unproven transport: retry at once.
    pub(super) immediate: bool,
    /// Browser lifecycle: `Offline` closed the transport (until `Online`).
    pub(super) went_offline: bool,
    /// Browser lifecycle: `PageHidden` closed it (until `PageShown`).
    pub(super) page_cached: bool,
}

impl Conn {
    pub(super) fn new(cfg: &CoreConfig) -> Self {
        Self {
            token: cfg.token.clone(),
            refresh_required: false,
            connect_token: String::new(),
            attempt: 0,
            next_transport: 0,
            current: None,
            session: String::new(),
            node: String::new(),
            client_refresh: false,
            token_expires_at: None,
            ping_window: None,
            pong: false,
            reconnect_attempts: 0,
            reset_pending: false,
            index: 0,
            transport_was_open: false,
            tried_all: false,
            immediate: false,
            went_offline: false,
            page_cached: false,
        }
    }

    pub(super) fn is_current(&self, t: TransportId) -> bool {
        self.current.as_ref().is_some_and(|c| c.id == t)
    }

    pub(super) fn is_connected_attempt(&self, attempt: u64, state: State) -> bool {
        state == State::Connected && self.attempt == attempt
    }

    pub(super) fn session(&self) -> Option<Session> {
        self.current.as_ref().map(|t| Session {
            transport: t.id,
            kind: t.kind,
            session: self.session.clone(),
            node: self.node.clone(),
        })
    }
}

/// Parses a legacy JSON close reason `{"code","reason","reconnect"}`.
fn legacy_close(reason: &str) -> Option<(u32, String, bool)> {
    #[cfg(feature = "json")]
    {
        #[derive(serde::Deserialize)]
        struct D {
            code: u32,
            #[serde(default)]
            reason: String,
            #[serde(default)]
            reconnect: bool,
        }
        if reason.starts_with('{') {
            if let Ok(d) = serde_json::from_str::<D>(reason) {
                return Some((d.code, d.reason, d.reconnect));
            }
        }
    }
    let _ = reason;
    None
}

impl Core {
    // ───────────────────────── app operations ─────────────────────────

    pub(super) fn op_connect(&mut self, now: Instant, out: &mut Outputs) {
        if self.state != State::Disconnected {
            return;
        }
        self.state = State::Connecting;
        self.conn.reconnect_attempts = 0;
        out.event(Event::Connecting {
            code: connecting::CONNECT_CALLED,
            reason: "connect called".into(),
        });
        self.start_attempt(now, out);
    }

    pub(super) fn op_disconnect(&mut self, now: Instant, out: &mut Outputs) {
        self.disconnects += 1;
        // The app's own disconnect ends what a lifecycle event started.
        self.conn.went_offline = false;
        self.conn.page_cached = false;
        if matches!(self.state, State::Connecting | State::Connected) {
            self.teardown(
                disconnected::DISCONNECT_CALLED,
                "disconnect called",
                false,
                now,
                out,
            );
        }
    }

    pub(super) fn op_close(&mut self, now: Instant, out: &mut Outputs) {
        let was = self.state;
        self.disconnects += 1;
        self.end_transport(out);
        self.clear_connection_timers();
        self.generation += u64::from(was == State::Connected);
        // Every subscription ends (unsubscribed 2) before the client's event.
        let mut keys: Vec<_> = self.subs.keys().copied().collect();
        keys.sort_unstable();
        for key in keys {
            self.unsubscribe_local(
                key,
                codes::unsubscribed::CLIENT_CLOSED,
                "client closed",
                out,
            );
        }
        let mut channels: Vec<_> = self.server_subs.keys().cloned().collect();
        channels.sort();
        for channel in channels {
            self.server_subs.remove(&channel);
            out.event(Event::ServerUnsubscribed { channel });
        }
        self.deliveries.clear();
        self.pending.clear();
        self.flows.clear();
        self.ready.clear();
        self.timers = super::Timers::default();
        self.state = State::Closed;
        if was != State::Disconnected {
            out.event(Event::Disconnected {
                code: disconnected::DISCONNECT_CALLED,
                reason: "disconnect called".into(),
            });
        }
        let _ = now;
    }

    // ───────────────────────── connect attempts ─────────────────────────

    /// Starts a connect attempt: token first, then the transport (§6.2).
    pub(super) fn start_attempt(&mut self, now: Instant, out: &mut Outputs) {
        if self.state != State::Connecting {
            return;
        }
        self.conn.attempt += 1;
        self.end_transport(out);
        let need_token =
            self.conn.refresh_required || (self.conn.token.is_empty() && self.cfg.has_get_token);
        if need_token {
            if self.cfg.has_get_token {
                let flow = self.new_flow(Flow::ConnectToken {
                    attempt: self.conn.attempt,
                });
                out.callbacks
                    .push(super::Callback::ConnectionToken { flow });
                return;
            }
            out.event(Event::Error {
                kind: ClientErrorKind::Configuration,
                message: "connection token expired and no token getter is set".into(),
                source: None,
            });
            self.teardown(disconnected::UNAUTHORIZED, "unauthorized", false, now, out);
            return;
        }
        self.open_transport(now, out);
    }

    fn open_transport(&mut self, now: Instant, out: &mut Outputs) {
        if self.conn.index >= self.cfg.transports.len() {
            self.conn.index = 0;
        }
        let kind = self.cfg.transports[self.conn.index];
        self.conn.next_transport += 1;
        let id = self.conn.next_transport;
        let mut t = super::conn::Transport {
            id,
            kind,
            opened: false,
            got_message: false,
            connect_id: None,
        };
        let initial = if kind.uses_emulation() {
            let (cmd_id, cmd) = self.connect_command();
            t.connect_id = Some(cmd_id);
            Some(cmd)
        } else {
            None
        };
        self.conn.current = Some(t);
        self.timers
            .arm_after(TimerKey::Open, now, self.cfg.handshake_timeout);
        out.open = Some(OpenRequest {
            transport: id,
            kind,
            index: self.conn.index,
            initial,
        });
    }

    /// Builds the connect command (§6.2 step 3) and registers it.
    fn connect_command(&mut self) -> (u32, proto::Command) {
        let id = self.next_id();
        self.pending.insert(
            id,
            Pending::Connect {
                attempt: self.conn.attempt,
            },
        );
        self.conn.connect_token = self.conn.token.clone();
        let subs = self
            .server_subs
            .iter()
            .filter(|(_, s)| s.recoverable)
            .map(|(ch, s)| {
                (
                    ch.clone(),
                    proto::SubscribeRequest {
                        recover: true,
                        epoch: s.epoch.clone(),
                        offset: s.offset,
                        ..Default::default()
                    },
                )
            })
            .collect();
        let cmd = proto::Command {
            id,
            connect: Some(proto::ConnectRequest {
                token: self.conn.token.clone(),
                data: self.cfg.data.clone(),
                subs,
                name: self.cfg.name.clone(),
                version: self.cfg.version.clone(),
                headers: self.cfg.emulated_headers.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        (id, cmd)
    }

    pub(super) fn on_opened(&mut self, transport: TransportId, now: Instant, out: &mut Outputs) {
        if !self.conn.is_current(transport) || self.state != State::Connecting {
            return;
        }
        self.timers.cancel(TimerKey::Open);
        let Some(t) = self.conn.current.as_mut() else {
            return;
        };
        t.opened = true;
        let kind = t.kind;
        if kind.uses_emulation() {
            // The connect command went with the open request.
        } else {
            // A socket that upgraded keeps its transport: a slow first connect
            // reply must not move the client to the fallback for good (§5.8).
            self.conn.transport_was_open = true;
            let (cmd_id, cmd) = self.connect_command();
            if let Some(t) = self.conn.current.as_mut() {
                t.connect_id = Some(cmd_id);
            }
            out.commands.push((transport, cmd));
        }
        self.timers
            .arm_after(TimerKey::ConnectReply, now, self.cfg.timeout);
    }

    pub(super) fn on_open_timeout(&mut self, now: Instant, out: &mut Outputs) {
        if self.state != State::Connecting {
            return;
        }
        let kind = self
            .conn
            .current
            .as_ref()
            .map_or(TransportKind::WebSocket, |t| t.kind);
        out.event(Event::Error {
            kind: ClientErrorKind::Transport {
                transport: kind,
                status: None,
            },
            message: "transport open timed out".into(),
            source: None,
        });
        self.attempt_failed(true, now, out);
    }

    pub(super) fn on_connect_timeout(&mut self, now: Instant, out: &mut Outputs) {
        if self.state != State::Connecting {
            return;
        }
        let kind = self
            .conn
            .current
            .as_ref()
            .map_or(TransportKind::WebSocket, |t| t.kind);
        out.event(Event::Error {
            kind: ClientErrorKind::Transport {
                transport: kind,
                status: None,
            },
            message: "connect reply timed out".into(),
            source: None,
        });
        let unproven = self.conn.current.as_ref().is_some_and(|t| !t.got_message)
            && !self.conn.transport_was_open;
        self.attempt_failed(unproven, now, out);
    }

    /// A connect attempt failed while connecting: retry, advancing the fallback
    /// when the transport never proved to reach the server.
    fn attempt_failed(&mut self, unproven: bool, now: Instant, out: &mut Outputs) {
        self.end_transport(out);
        self.timers.cancel(TimerKey::ConnectReply);
        self.timers.cancel(TimerKey::Open);
        self.conn.immediate = false;
        if unproven {
            self.advance_fallback();
        }
        self.stats.reconnect_attempts += 1;
        self.schedule_reconnect(now);
    }

    fn advance_fallback(&mut self) {
        if self.cfg.transports.len() < 2 || self.conn.transport_was_open {
            return;
        }
        self.conn.index += 1;
        if self.conn.index >= self.cfg.transports.len() {
            self.conn.index = 0;
            self.conn.tried_all = true;
        } else if !self.conn.tried_all {
            // The first round over the transports is immediate (§5.8).
            self.conn.immediate = true;
        }
    }

    pub(super) fn schedule_reconnect(&mut self, now: Instant) {
        if self.state != State::Connecting {
            return;
        }
        let delay = if self.conn.immediate {
            self.conn.immediate = false;
            Duration::ZERO
        } else {
            let d = backoff(
                &mut self.rng,
                self.conn.reconnect_attempts,
                self.cfg.min_reconnect_delay,
                self.cfg.max_reconnect_delay,
            );
            self.conn.reconnect_attempts = self.conn.reconnect_attempts.saturating_add(1);
            d
        };
        self.timers.arm_after(TimerKey::Reconnect, now, delay);
    }

    fn end_transport(&mut self, out: &mut Outputs) {
        if let Some(t) = self.conn.current.take() {
            out.close.push(t.id);
            out.fail_calls.push(t.id);
            if let Some(id) = t.connect_id {
                self.pending.remove(&id);
            }
        }
    }

    fn clear_connection_timers(&mut self) {
        for k in [
            TimerKey::Open,
            TimerKey::ConnectReply,
            TimerKey::NoPing,
            TimerKey::Refresh,
            TimerKey::BackoffReset,
            TimerKey::Reconnect,
        ] {
            self.timers.cancel(k);
        }
        let ids: Vec<u32> = self.pending.keys().copied().collect();
        for id in ids {
            self.timers.cancel(TimerKey::Command(id));
        }
        self.pending.clear();
        self.graced.clear();
    }

    // ───────────────────────── connect replies ─────────────────────────

    pub(super) fn on_connect_reply(
        &mut self,
        transport: TransportId,
        attempt: u64,
        reply: proto::Reply,
        now: Instant,
        out: &mut Outputs,
    ) {
        if self.state != State::Connecting
            || attempt != self.conn.attempt
            || !self.conn.is_current(transport)
        {
            return;
        }
        self.timers.cancel(TimerKey::ConnectReply);
        if let Some(t) = self.conn.current.as_mut() {
            t.connect_id = None;
        }
        if let Some(err) = reply.error {
            self.on_connect_error(err, now, out);
            return;
        }
        let Some(res) = reply.connect else {
            self.terminate(disconnected::BAD_PROTOCOL, "malformed reply", now, out);
            return;
        };
        self.on_connected(res, now, out);
    }

    fn on_connect_error(&mut self, err: proto::Error, now: Instant, out: &mut Outputs) {
        // Any error reply proves the transport reaches the server (§5.8).
        self.conn.transport_was_open = true;
        out.event(Event::Error {
            kind: ClientErrorKind::Connect,
            message: format!("{} ({})", err.message, err.code),
            source: None,
        });
        if err.code == codes::TOKEN_EXPIRED {
            if self.cfg.has_get_token || self.conn.token == self.conn.connect_token {
                self.conn.refresh_required = true;
            }
            self.attempt_failed(false, now, out);
        } else if err.temporary || err.code < 100 {
            self.attempt_failed(false, now, out);
        } else {
            self.teardown(err.code, &err.message, false, now, out);
        }
    }

    fn on_connected(&mut self, res: proto::ConnectResult, now: Instant, out: &mut Outputs) {
        let Some(t) = self.conn.current.as_mut() else {
            return;
        };
        t.got_message = true;
        let kind = t.kind;
        let transport = t.id;
        self.conn.transport_was_open = true;
        self.conn.refresh_required = false;
        self.state = State::Connected;
        self.conn.session.clone_from(&res.session);
        self.conn.node.clone_from(&res.node);
        self.stats.connects += 1;
        // The backoff resets on the first ping or command reply (§6.2 step 6),
        // or after staying connected for max_reconnect_delay.
        self.conn.reset_pending = true;
        self.timers
            .arm_after(TimerKey::BackoffReset, now, self.cfg.max_reconnect_delay);
        self.conn.client_refresh = res.expires;
        if res.expires {
            let ttl = Duration::from_secs(u64::from(res.ttl.max(1)));
            self.conn.token_expires_at = Some(now + ttl);
            self.timers.arm_after(TimerKey::Refresh, now, ttl);
        } else {
            self.conn.token_expires_at = None;
        }
        if res.ping > 0 {
            let window = Duration::from_secs(u64::from(res.ping)) + self.cfg.max_server_ping_delay;
            self.conn.ping_window = Some(window);
            self.conn.pong = res.pong;
            self.timers.arm_after(TimerKey::NoPing, now, window);
        } else {
            self.conn.ping_window = None;
        }
        out.event(Event::Connected {
            client_id: res.client.clone(),
            version: res.version.clone(),
            data: res.data.clone(),
            transport: kind,
        });
        self.process_server_subs_of_reply(res.subs, out);
        // Resubscribe what is subscribing, unless a flow or a retry delay is
        // pending (TLA+ decision "the resubscribe timer survives it").
        let keys: Vec<_> = self.subs.keys().copied().collect();
        for key in keys {
            if self
                .subs
                .get(&key)
                .is_some_and(|s| s.state == SubState::Subscribing)
            {
                self.ready.insert(key);
            }
        }
        let _ = transport;
    }

    // ───────────────────────── inbound items ─────────────────────────

    pub(super) fn on_item(
        &mut self,
        transport: TransportId,
        reply: proto::Reply,
        now: Instant,
        out: &mut Outputs,
    ) {
        if !self.conn.is_current(transport) {
            return;
        }
        self.stats.messages_in += 1;
        if let Some(t) = self.conn.current.as_mut() {
            t.got_message = true;
        }
        if let Some(window) = self.conn.ping_window {
            if self.state == State::Connected {
                self.timers.arm_after(TimerKey::NoPing, now, window);
            }
        }
        if reply.id > 0 {
            self.backoff_proven();
            let Some(p) = self.pending.remove(&reply.id) else {
                return;
            };
            self.timers.cancel(TimerKey::Command(reply.id));
            self.graced.remove(&TimerKey::Command(reply.id));
            match p {
                Pending::Connect { attempt } => {
                    self.on_connect_reply(transport, attempt, reply, now, out);
                }
                Pending::Subscribe {
                    sub,
                    channel,
                    attempt,
                    generation,
                } => {
                    self.on_subscribe_reply(
                        sub, &channel, attempt, generation, reply.id, reply, now, out,
                    );
                }
                Pending::Unsubscribe {
                    channel,
                    generation,
                } => self.on_unsubscribe_reply(&channel, generation, reply, now, out),
                Pending::Refresh { generation } => {
                    self.on_refresh_reply(generation, reply, now, out);
                }
                Pending::SubRefresh {
                    sub,
                    session,
                    invalidations,
                } => {
                    self.on_sub_refresh_reply(sub, session, invalidations, reply, now, out);
                }
            }
            return;
        }
        let Some(push) = reply.push else {
            // A server ping: answer on this transport only (pattern 29).
            self.backoff_proven();
            if self.state == State::Connected && self.conn.pong {
                out.commands.push((transport, proto::Command::default()));
            }
            return;
        };
        if self.state != State::Connected {
            // A disconnect push is how HTTP-streaming reports a refused
            // connect (e.g. an invalid token): honored while connecting too,
            // and, like a close code, it proves the transport works.
            if self.state == State::Connecting && push.disconnect.is_some() {
                self.conn.transport_was_open = true;
                self.on_push(transport, push, now, out);
            }
            return;
        }
        self.on_push(transport, push, now, out);
    }

    fn backoff_proven(&mut self) {
        if self.conn.reset_pending && self.state == State::Connected {
            self.conn.reset_pending = false;
            self.conn.reconnect_attempts = 0;
            self.timers.cancel(TimerKey::BackoffReset);
        }
    }

    fn on_push(
        &mut self,
        transport: TransportId,
        push: proto::Push,
        now: Instant,
        out: &mut Outputs,
    ) {
        if let Some(d) = push.disconnect {
            let reconnect = codes::is_reconnect_code(d.code);
            self.server_disconnect(transport, d.code, &d.reason, reconnect, now, out);
            return;
        }
        if let Some(m) = push.message {
            out.event(Event::Message { data: m.data });
            return;
        }
        if let Some(r) = push.refresh {
            self.on_refresh_push(r.expires, r.ttl, now);
            return;
        }
        if push.connect.is_some() {
            return; // Unidirectional transports only.
        }
        self.on_channel_push(push, now, out);
    }

    // ───────────────────────── closes and teardown ─────────────────────────

    pub(super) fn on_closed(
        &mut self,
        transport: TransportId,
        reason: CloseReason,
        now: Instant,
        out: &mut Outputs,
    ) {
        if !self.conn.is_current(transport) {
            return;
        }
        let kind = self
            .conn
            .current
            .as_ref()
            .map_or(TransportKind::WebSocket, |t| t.kind);
        let proven_now = self.conn.current.as_ref().is_some_and(|t| t.got_message);
        match reason {
            CloseReason::Frame { code, reason } => {
                let (code, reason, reconnect) = if let Some(l) = legacy_close(&reason) {
                    l
                } else {
                    let c = u32::from(code);
                    if c < 3000 {
                        if c == 1009 {
                            (
                                disconnected::MESSAGE_SIZE_LIMIT,
                                "message size limit exceeded".to_string(),
                                false,
                            )
                        } else {
                            if !proven_now {
                                self.advance_fallback_on_failure();
                            }
                            (
                                connecting::TRANSPORT_CLOSED,
                                "transport closed".to_string(),
                                true,
                            )
                        }
                    } else {
                        // A code from the server proves the transport.
                        self.conn.transport_was_open = true;
                        (c, reason, codes::is_reconnect_code(c))
                    }
                };
                self.server_disconnect(transport, code, &reason, reconnect, now, out);
            }
            CloseReason::Eof => {
                if self.state == State::Connecting && !proven_now {
                    self.advance_fallback_on_failure();
                }
                self.transport_lost(
                    transport,
                    connecting::TRANSPORT_CLOSED,
                    "transport closed",
                    now,
                    out,
                );
            }
            CloseReason::Failed {
                status,
                message,
                source,
            } => {
                if self.state == State::Connecting && !proven_now {
                    out.event(Event::Error {
                        kind: ClientErrorKind::Transport {
                            transport: kind,
                            status,
                        },
                        message,
                        source,
                    });
                    self.advance_fallback_on_failure();
                }
                self.transport_lost(
                    transport,
                    connecting::TRANSPORT_CLOSED,
                    "transport closed",
                    now,
                    out,
                );
            }
            CloseReason::Undecodable(e) => {
                if proven_now {
                    out.event(Event::Error {
                        kind: ClientErrorKind::Protocol,
                        message: e.to_string(),
                        source: None,
                    });
                    self.terminate(disconnected::BAD_PROTOCOL, "bad protocol", now, out);
                } else {
                    // A captive portal, a misrouted proxy, a TLS-inspecting box:
                    // a transport failure, not a stop (docs/spec.md §5.3).
                    out.event(Event::Error {
                        kind: ClientErrorKind::Transport {
                            transport: kind,
                            status: None,
                        },
                        message: e.to_string(),
                        source: None,
                    });
                    self.conn.transport_was_open = false;
                    self.advance_fallback_on_failure();
                    self.transport_lost(
                        transport,
                        connecting::TRANSPORT_CLOSED,
                        "transport closed",
                        now,
                        out,
                    );
                }
            }
            CloseReason::TooLarge => {
                if proven_now {
                    out.event(Event::Error {
                        kind: ClientErrorKind::Protocol,
                        message: "message size limit exceeded".into(),
                        source: None,
                    });
                    self.terminate(
                        disconnected::MESSAGE_SIZE_LIMIT,
                        "message size limit exceeded",
                        now,
                        out,
                    );
                } else {
                    self.conn.transport_was_open = false;
                    self.advance_fallback_on_failure();
                    self.transport_lost(
                        transport,
                        connecting::TRANSPORT_CLOSED,
                        "transport closed",
                        now,
                        out,
                    );
                }
            }
        }
    }

    fn advance_fallback_on_failure(&mut self) {
        self.conn.immediate = false;
        self.advance_fallback();
    }

    /// The transport went away unexpectedly: reconnect.
    pub(super) fn transport_lost(
        &mut self,
        transport: TransportId,
        code: u32,
        reason: &str,
        now: Instant,
        out: &mut Outputs,
    ) {
        if !self.conn.is_current(transport) {
            return;
        }
        match self.state {
            State::Connecting => {
                let immediate = self.conn.immediate;
                self.end_transport(out);
                self.timers.cancel(TimerKey::ConnectReply);
                self.timers.cancel(TimerKey::Open);
                self.stats.reconnect_attempts += 1;
                self.conn.immediate = immediate;
                self.schedule_reconnect(now);
            }
            State::Connected => self.teardown(code, reason, true, now, out),
            _ => {}
        }
    }

    /// A disconnect advice from the server (push or close frame).
    fn server_disconnect(
        &mut self,
        transport: TransportId,
        code: u32,
        reason: &str,
        reconnect: bool,
        now: Instant,
        out: &mut Outputs,
    ) {
        if !self.conn.is_current(transport) {
            return;
        }
        if code == codes::DISCONNECT_STATE_INVALIDATED {
            self.invalidate_connection();
        }
        if code == codes::DISCONNECT_EXPIRED && self.cfg.has_get_token {
            self.conn.refresh_required = true;
        }
        if reconnect {
            if self.state == State::Connecting {
                self.conn.transport_was_open = true;
                self.transport_lost(transport, code, reason, now, out);
            } else {
                self.teardown(code, reason, true, now, out);
            }
        } else {
            self.teardown(code, reason, false, now, out);
        }
    }

    /// A terminal stop decided by the client itself.
    pub(super) fn terminate(&mut self, code: u32, reason: &str, now: Instant, out: &mut Outputs) {
        if matches!(self.state, State::Connecting | State::Connected) {
            self.teardown(code, reason, false, now, out);
        }
    }

    /// Ends the connection (§6.1): subscriptions go subscribing (events first),
    /// then the client goes connecting (reconnect) or disconnected.
    pub(super) fn teardown(
        &mut self,
        code: u32,
        reason: &str,
        reconnect: bool,
        now: Instant,
        out: &mut Outputs,
    ) {
        let was = self.state;
        self.end_transport(out);
        self.clear_connection_timers();
        self.flows
            .retain(|_, f| matches!(f, Flow::SubState { .. } | Flow::SubToken { .. }));
        if was == State::Connected {
            self.generation += 1;
        }
        self.conn.reset_pending = false;
        self.by_push_id.clear();
        self.deliveries.clear();
        self.channels.clear();
        let mut keys: Vec<_> = self.subs.keys().copied().collect();
        keys.sort_unstable();
        for key in keys {
            self.sub_on_teardown(key, out);
        }
        if was == State::Connected {
            let mut channels: Vec<_> = self.server_subs.keys().cloned().collect();
            channels.sort();
            for channel in channels {
                out.event(Event::ServerSubscribing { channel });
            }
        }
        if reconnect {
            self.state = State::Connecting;
            if was == State::Connected {
                out.event(Event::Connecting {
                    code,
                    reason: reason.into(),
                });
            }
            self.schedule_reconnect(now);
        } else {
            self.state = State::Disconnected;
            self.disconnects += 1;
            self.timers.cancel(TimerKey::Reconnect);
            out.event(Event::Disconnected {
                code,
                reason: reason.into(),
            });
        }
    }

    /// 3014 (§6.5): drop the token when a getter can replace it, invalidate
    /// every subscription and server-side position.
    fn invalidate_connection(&mut self) {
        if self.cfg.has_get_token {
            self.conn.token.clear();
            self.conn.refresh_required = true;
        }
        let keys: Vec<_> = self.subs.keys().copied().collect();
        for key in keys {
            self.invalidate_sub(key);
        }
        for s in self.server_subs.values_mut() {
            if s.has_position() {
                s.offset = 0;
                s.epoch = sub::INVALIDATED_EPOCH.into();
            }
        }
    }

    // ───────────────────────── no-ping, resume ─────────────────────────

    pub(super) fn on_no_ping(
        &mut self,
        last_received: Option<Instant>,
        now: Instant,
        out: &mut Outputs,
    ) {
        if self.state != State::Connected {
            return;
        }
        let Some(window) = self.conn.ping_window else {
            return;
        };
        // Data received within the window but not yet processed (a slow handler,
        // a full read-ahead window) keeps the connection alive (§6.4).
        if let Some(at) = last_received {
            if now.duration_since(at) < window {
                self.timers.arm(TimerKey::NoPing, at + window);
                return;
            }
        }
        let Some(t) = self.conn.current.as_ref().map(|t| t.id) else {
            return;
        };
        self.teardown(connecting::NO_PING, "no ping", true, now, out);
        let _ = t;
    }

    pub(super) fn on_resumed(&mut self, slept: Duration, now: Instant, out: &mut Outputs) {
        // A laptop wake: the monotonic clock didn't move while asleep.
        if let Some(exp) = self.conn.token_expires_at {
            // A wall clock that jumped years ahead counts as expired too.
            if now.checked_add(slept).is_none_or(|t| exp <= t) && self.cfg.has_get_token {
                self.conn.refresh_required = true;
            }
            self.conn.token_expires_at = Some(exp.checked_sub(slept).unwrap_or(now));
        }
        // Token refresh deadlines are wall-clock time in effect: what the sleep
        // used up comes off them (a refresh due during the sleep runs now).
        for key in self.timers.keys() {
            if matches!(key, TimerKey::Refresh | TimerKey::SubRefresh(_)) {
                if let Some(at) = self.timers.deadline(key) {
                    let earlier = at.checked_sub(slept).unwrap_or(now).max(now);
                    self.timers.arm(key, earlier);
                }
            }
        }
        if self.state == State::Connected {
            if let Some(window) = self.conn.ping_window {
                if slept >= window {
                    // Asleep longer than the server waits for a pong: it has
                    // dropped this connection, which may also be stuck on a
                    // network that's gone. Reconnect now rather than after a
                    // full no-ping window (spec §9.7).
                    if let Some(t) = self.conn.current.as_ref().map(|t| t.id) {
                        self.transport_lost(
                            t,
                            connecting::TRANSPORT_CLOSED,
                            "connection lost during system sleep",
                            now,
                            out,
                        );
                    }
                    return;
                }
                self.timers.arm_after(TimerKey::NoPing, now, window);
            }
        }
    }

    /// A browser lifecycle event (docs/wasm-plan.md, step 7a).
    pub(super) fn on_lifecycle(&mut self, e: super::Lifecycle, now: Instant, out: &mut Outputs) {
        use super::Lifecycle;
        match e {
            Lifecycle::Offline | Lifecycle::PageHidden => {
                if !matches!(self.state, State::Connecting | State::Connected) {
                    return;
                }
                let reason = if e == Lifecycle::Offline {
                    self.conn.went_offline = true;
                    "transport closed"
                } else {
                    self.conn.page_cached = true;
                    "page entered back/forward cache"
                };
                if let Some(t) = self.conn.current.as_ref().map(|t| t.id) {
                    self.transport_lost(t, connecting::TRANSPORT_CLOSED, reason, now, out);
                }
            }
            Lifecycle::Online => {
                let went_offline = std::mem::take(&mut self.conn.went_offline);
                if self.state != State::Connecting {
                    return;
                }
                if went_offline {
                    // Opened while offline: it may never report closed.
                    if let Some(t) = self.conn.current.as_ref().map(|t| t.id) {
                        self.transport_lost(
                            t,
                            connecting::TRANSPORT_CLOSED,
                            "transport closed",
                            now,
                            out,
                        );
                    }
                }
                self.reconnect_now(now, out);
            }
            Lifecycle::PageShown => {
                let cached = std::mem::take(&mut self.conn.page_cached);
                if cached && self.state == State::Connecting {
                    self.reconnect_now(now, out);
                }
            }
        }
    }

    /// Skips the backoff wait, if one is running; an attempt in progress (a
    /// transport opening, a token callback) carries on.
    fn reconnect_now(&mut self, now: Instant, out: &mut Outputs) {
        if self.timers.deadline(TimerKey::Reconnect).is_some() {
            self.timers.cancel(TimerKey::Reconnect);
            self.start_attempt(now, out);
        }
    }

    // ───────────────────────── connection token refresh ─────────────────────────

    pub(super) fn on_refresh_timer(&mut self, _now: Instant, out: &mut Outputs) {
        if self.state != State::Connected || !self.cfg.has_get_token {
            return;
        }
        let flow = self.new_flow(Flow::RefreshToken {
            generation: self.generation,
        });
        out.callbacks
            .push(super::Callback::ConnectionToken { flow });
    }

    fn refresh_retry_delay(&mut self, now: Instant) -> Duration {
        let mut d = self
            .rng
            .between(Duration::from_secs(5), Duration::from_secs(10));
        if let Some(exp) = self.conn.token_expires_at {
            let left = exp.saturating_duration_since(now);
            d = d.min(left.max(Duration::from_secs(1)));
        }
        d
    }

    pub(super) fn on_connection_token(
        &mut self,
        flow: Flow,
        result: Result<Option<String>, CallbackError>,
        now: Instant,
        out: &mut Outputs,
    ) {
        match flow {
            Flow::ConnectToken { attempt } => {
                if self.state != State::Connecting || attempt != self.conn.attempt {
                    return;
                }
                match result {
                    Ok(t) => {
                        self.conn.token = t.unwrap_or_default();
                        self.conn.refresh_required = false;
                        self.open_transport(now, out);
                    }
                    Err(CallbackError::Unauthorized) => {
                        self.teardown(disconnected::UNAUTHORIZED, "unauthorized", false, now, out);
                    }
                    Err(CallbackError::Other(e)) => {
                        out.event(Event::Error {
                            kind: ClientErrorKind::ConnectToken,
                            message: e.to_string(),
                            source: Some(e),
                        });
                        if self.state == State::Connecting && attempt == self.conn.attempt {
                            self.stats.reconnect_attempts += 1;
                            self.schedule_reconnect(now);
                        }
                    }
                }
            }
            Flow::RefreshToken { generation } => {
                if self.state != State::Connected || generation != self.generation {
                    return;
                }
                match result {
                    Ok(Some(t)) if !t.is_empty() => {
                        self.conn.token.clone_from(&t);
                        let Some(transport) = self.conn.current.as_ref().map(|t| t.id) else {
                            return;
                        };
                        let cmd = proto::Command {
                            refresh: Some(proto::RefreshRequest { token: t }),
                            ..Default::default()
                        };
                        self.send(transport, cmd, Pending::Refresh { generation }, now, out);
                    }
                    Ok(_) | Err(CallbackError::Unauthorized) => {
                        self.teardown(disconnected::UNAUTHORIZED, "unauthorized", false, now, out);
                    }
                    Err(CallbackError::Other(e)) => {
                        out.event(Event::Error {
                            kind: ClientErrorKind::Refresh,
                            message: e.to_string(),
                            source: Some(e),
                        });
                        let d = self.refresh_retry_delay(now);
                        self.timers.arm_after(TimerKey::Refresh, now, d);
                    }
                }
            }
            _ => {}
        }
    }

    pub(super) fn on_refresh_reply(
        &mut self,
        generation: u64,
        reply: proto::Reply,
        now: Instant,
        out: &mut Outputs,
    ) {
        if self.state != State::Connected || generation != self.generation {
            return;
        }
        if let Some(err) = reply.error {
            self.on_refresh_failed(generation, Some(err), now, out);
            return;
        }
        let Some(r) = reply.refresh else {
            self.terminate(disconnected::BAD_PROTOCOL, "malformed reply", now, out);
            return;
        };
        self.conn.client_refresh = r.expires;
        if r.expires {
            let ttl = Duration::from_secs(u64::from(r.ttl.max(1)));
            self.conn.token_expires_at = Some(now + ttl);
            self.timers.arm_after(TimerKey::Refresh, now, ttl);
        } else {
            self.conn.token_expires_at = None;
            self.timers.cancel(TimerKey::Refresh);
        }
    }

    pub(super) fn on_refresh_failed(
        &mut self,
        generation: u64,
        err: Option<proto::Error>,
        now: Instant,
        out: &mut Outputs,
    ) {
        if self.state != State::Connected || generation != self.generation {
            return;
        }
        match err {
            Some(e) if !e.temporary => {
                self.teardown(e.code, &e.message, false, now, out);
            }
            e => {
                let message = e.map_or_else(
                    || "refresh timed out".to_string(),
                    |e| format!("{} ({})", e.message, e.code),
                );
                out.event(Event::Error {
                    kind: ClientErrorKind::Refresh,
                    message,
                    source: None,
                });
                let d = self.refresh_retry_delay(now);
                self.timers.arm_after(TimerKey::Refresh, now, d);
            }
        }
    }

    /// A `refresh` push from the server API (§6.6): only adjusts a client-side
    /// refresh schedule.
    fn on_refresh_push(&mut self, expires: bool, ttl: u32, now: Instant) {
        if !self.conn.client_refresh {
            return;
        }
        if expires {
            let ttl = Duration::from_secs(u64::from(ttl.max(1)));
            self.conn.token_expires_at = Some(now + ttl);
            self.timers.arm_after(TimerKey::Refresh, now, ttl);
        } else {
            self.conn.client_refresh = false;
            self.conn.token_expires_at = None;
            self.timers.cancel(TimerKey::Refresh);
        }
    }
}

use super::sub;
