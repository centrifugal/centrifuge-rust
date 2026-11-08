//! Client-side subscription behaviour (docs/spec.md §7).

use std::time::Duration;

use crate::time::Instant;

use bytes::Bytes;

use super::timers::backoff;
use super::{
    CallbackError, Commit, Core, Delivery, Event, Flow, Outputs, Pending, State, SubConfig, SubKey,
    SubState, SubscribedInfo, SubscriptionErrorKind, TimerKey,
};
use crate::codec::Protocol;
use crate::codes::{connecting, disconnected, subscribing, unsubscribed};
use crate::types::{ClientInfo, Publication, StreamPosition};
use crate::{codes, delta, proto};

/// The sentinel epoch of an invalidated position: the server never matches it
/// (docs/spec.md §7.7).
pub(super) const INVALIDATED_EPOCH: &str = "_";

/// Subscribe flag: offer channel compaction.
const FLAG_COMPACTION: i64 = 1;
/// Subscribe flag: reject an unrecoverable position with error 112.
const FLAG_REJECT_UNRECOVERED: i64 = 2;

/// What the subscription's flow is waiting for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Inflight {
    Token,
    State,
    Command(u32),
}

/// Per-channel unsubscribe bookkeeping (TLA+ `App_Replace`).
#[derive(Debug, Default)]
pub(super) struct ChannelBook {
    /// Unsubscribe replies awaited.
    pub(super) unsub_waits: usize,
    /// A subscribe command whose unsubscribe is deferred until its reply.
    pub(super) deferred: Option<u32>,
}

impl ChannelBook {
    fn idle(&self) -> bool {
        self.unsub_waits == 0 && self.deferred.is_none()
    }
}

#[derive(Debug)]
pub(super) struct Sub {
    pub(super) key: SubKey,
    pub(super) cfg: SubConfig,
    pub(super) state: SubState,
    pub(super) token: String,
    /// Bumped by `unsubscribe()`.
    pub(super) attempt: u64,
    /// Bumped by each state invalidation.
    pub(super) invalidations: u64,
    /// Bumped on each entry into `subscribed`.
    pub(super) session: u64,
    /// Bumped on each entry into `unsubscribed` (waiters fail on change).
    pub(super) unsubscribes: u64,
    pub(super) inflight: Option<Inflight>,
    /// Recover from the stored position on subscribe.
    pub(super) recover: bool,
    pub(super) offset: u64,
    pub(super) epoch: String,
    pub(super) prev_data: Option<Bytes>,
    pub(super) delta_negotiated: bool,
    pub(super) push_id: i64,
    pub(super) resubscribe_attempts: u32,
}

impl Sub {
    pub(super) fn new(key: SubKey, cfg: SubConfig) -> Self {
        let token = cfg.token.clone();
        let (recover, offset, epoch) = match &cfg.since {
            Some(p) => (true, p.offset, p.epoch.clone()),
            None => (false, 0, String::new()),
        };
        Self {
            key,
            cfg,
            state: SubState::Unsubscribed,
            token,
            attempt: 0,
            invalidations: 0,
            session: 0,
            unsubscribes: 0,
            inflight: None,
            recover,
            offset,
            epoch,
            prev_data: None,
            delta_negotiated: false,
            push_id: 0,
            resubscribe_attempts: 0,
        }
    }

    /// Moves the position forward to a delivered publication (§7.4).
    pub(super) fn commit(&mut self, offset: u64, epoch: Option<String>) {
        if offset > self.offset {
            self.offset = offset;
        }
        if let Some(e) = epoch {
            if self.epoch.is_empty() {
                self.epoch = e;
            }
        }
    }
}

impl Core {
    /// Commands may reach the server out of order (HTTP-streaming emulation).
    fn emulation(&self) -> bool {
        self.conn
            .current
            .as_ref()
            .is_some_and(|t| t.kind.uses_emulation())
    }

    fn book(&mut self, channel: &str) -> &mut ChannelBook {
        self.channels.entry(channel.to_owned()).or_default()
    }

    fn mark_channel_ready(&mut self, channel: &str) {
        let keys: Vec<SubKey> = self
            .subs
            .values()
            .filter(|s| s.cfg.channel == channel && s.state == SubState::Subscribing)
            .map(|s| s.key)
            .collect();
        self.ready.extend(keys);
    }

    // ───────────────────────── app operations ─────────────────────────

    pub(super) fn op_subscribe(&mut self, key: SubKey, out: &mut Outputs) {
        let Some(s) = self.subs.get_mut(&key) else {
            return;
        };
        if s.state != SubState::Unsubscribed {
            return;
        }
        s.state = SubState::Subscribing;
        out.event(Event::Subscribing {
            sub: key,
            code: subscribing::SUBSCRIBE_CALLED,
            reason: "subscribe called".into(),
        });
        self.ready.insert(key);
    }

    pub(super) fn op_unsubscribe(&mut self, key: SubKey, now: Instant, out: &mut Outputs) {
        let Some(s) = self.subs.get(&key) else { return };
        if s.state == SubState::Unsubscribed {
            return;
        }
        let was_subscribed = s.state == SubState::Subscribed;
        let inflight = s.inflight;
        let channel = s.cfg.channel.clone();
        let connected = self.state == State::Connected;
        // Write the command, then emit, then await (pattern 44).
        if connected {
            match inflight {
                // Over emulation the unsubscribe could overtake the subscribe:
                // deferred until its reply (§7.3, TLA+ `Defer`).
                Some(Inflight::Command(id)) if self.emulation() => {
                    self.book(&channel).deferred = Some(id);
                }
                // In order (WebSocket): at once; the subscribe's reply is stale.
                // A flow waiting for get_token / get_state counts as in flight
                // too, as in centrifuge-js (`_inflight`) and the TLA+ model.
                Some(_) => self.send_unsubscribe(&channel, now, out),
                None if was_subscribed => self.send_unsubscribe(&channel, now, out),
                None => {}
            }
        }
        self.unsubscribe_local(
            key,
            unsubscribed::UNSUBSCRIBE_CALLED,
            "unsubscribe called",
            out,
        );
    }

    fn send_unsubscribe(&mut self, channel: &str, now: Instant, out: &mut Outputs) {
        let Some(transport) = self.conn.current.as_ref().map(|t| t.id) else {
            return;
        };
        let cmd = proto::Command {
            unsubscribe: Some(proto::UnsubscribeRequest {
                channel: channel.to_owned(),
            }),
            ..Default::default()
        };
        let generation = self.generation;
        self.send(
            transport,
            cmd,
            Pending::Unsubscribe {
                channel: channel.to_owned(),
                generation,
            },
            now,
            out,
        );
        self.book(channel).unsub_waits += 1;
    }

    /// Moves to unsubscribed with its event; sends nothing.
    pub(super) fn unsubscribe_local(
        &mut self,
        key: SubKey,
        code: u32,
        reason: &str,
        out: &mut Outputs,
    ) {
        let Some(s) = self.subs.get_mut(&key) else {
            return;
        };
        if s.state == SubState::Unsubscribed {
            return;
        }
        s.state = SubState::Unsubscribed;
        s.attempt += 1;
        s.unsubscribes += 1;
        s.inflight = None;
        s.resubscribe_attempts = 0;
        let push_id = std::mem::take(&mut s.push_id);
        self.timers.cancel(TimerKey::Resubscribe(key));
        self.timers.cancel(TimerKey::SubRefresh(key));
        if push_id > 0 && self.by_push_id.get(&push_id) == Some(&key) {
            self.by_push_id.remove(&push_id);
        }
        self.ready.remove(&key);
        out.event(Event::Unsubscribed {
            sub: key,
            code,
            reason: reason.into(),
        });
    }

    // ───────────────────────── teardown, invalidation ─────────────────────────

    pub(super) fn sub_on_teardown(&mut self, key: SubKey, out: &mut Outputs) {
        let Some(s) = self.subs.get_mut(&key) else {
            return;
        };
        s.push_id = 0;
        // A token or state callback may still be running: it settles into the
        // flow check. A sent command is gone with the connection.
        if matches!(s.inflight, Some(Inflight::Command(_))) {
            s.inflight = None;
        }
        self.timers.cancel(TimerKey::SubRefresh(key));
        if s.state == SubState::Subscribed {
            s.state = SubState::Subscribing;
            out.event(Event::Subscribing {
                sub: key,
                code: subscribing::TRANSPORT_CLOSED,
                reason: "transport closed".into(),
            });
        }
    }

    /// State invalidation (§7.7).
    pub(super) fn invalidate_sub(&mut self, key: SubKey) {
        let has_get_token = self.subs.get(&key).is_some_and(|s| s.cfg.has_get_token);
        let Some(s) = self.subs.get_mut(&key) else {
            return;
        };
        let push_id = std::mem::take(&mut s.push_id);
        if has_get_token {
            s.token.clear();
        }
        s.invalidations += 1;
        if s.recover {
            s.offset = 0;
            s.epoch = INVALIDATED_EPOCH.into();
        }
        s.prev_data = None;
        if push_id > 0 && self.by_push_id.get(&push_id) == Some(&key) {
            self.by_push_id.remove(&push_id);
        }
    }

    // ───────────────────────── the subscribe flow ─────────────────────────

    pub(super) fn maybe_start_flow(&mut self, key: SubKey, now: Instant, out: &mut Outputs) {
        if self.state != State::Connected || self.timers.is_armed(TimerKey::Resubscribe(key)) {
            return;
        }
        let Some(s) = self.subs.get(&key) else { return };
        if s.state != SubState::Subscribing || s.inflight.is_some() {
            return;
        }
        // Over emulation a subscribe could overtake an earlier unsubscribe of
        // the channel: it waits for their replies (TLA+ `CanStart`). In order,
        // the server handles them first anyway.
        if self.emulation()
            && !self
                .channels
                .get(&s.cfg.channel)
                .is_none_or(ChannelBook::idle)
        {
            return;
        }
        self.flow_step(key, now, out);
    }

    /// Runs the next flow step: `get_state`, `get_token`, then the command.
    fn flow_step(&mut self, key: SubKey, now: Instant, out: &mut Outputs) {
        let Some(s) = self.subs.get(&key) else { return };
        let (attempt, invalidations, channel) = (s.attempt, s.invalidations, s.cfg.channel.clone());
        if s.cfg.has_get_state && !s.recover {
            let flow = self.new_flow(Flow::SubState {
                sub: key,
                attempt,
                invalidations,
            });
            if let Some(s) = self.subs.get_mut(&key) {
                s.inflight = Some(Inflight::State);
            }
            out.callbacks.push(super::Callback::GetState {
                flow,
                sub: key,
                channel,
            });
            return;
        }
        if s.token.is_empty() && s.cfg.has_get_token {
            let flow = self.new_flow(Flow::SubToken {
                sub: key,
                attempt,
                invalidations,
            });
            if let Some(s) = self.subs.get_mut(&key) {
                s.inflight = Some(Inflight::Token);
            }
            out.callbacks.push(super::Callback::SubscriptionToken {
                flow,
                sub: key,
                channel,
            });
            return;
        }
        self.send_subscribe(key, now, out);
    }

    fn send_subscribe(&mut self, key: SubKey, now: Instant, out: &mut Outputs) {
        if self.state != State::Connected {
            if let Some(s) = self.subs.get_mut(&key) {
                s.inflight = None;
            }
            return;
        }
        let Some(transport) = self.conn.current.as_ref().map(|t| t.id) else {
            return;
        };
        let Some(s) = self.subs.get(&key) else { return };
        let mut flag = FLAG_COMPACTION;
        if s.cfg.has_get_state {
            flag |= FLAG_REJECT_UNRECOVERED;
        }
        let req = proto::SubscribeRequest {
            channel: s.cfg.channel.clone(),
            token: s.token.clone(),
            recover: s.recover,
            epoch: if s.recover {
                s.epoch.clone()
            } else {
                String::new()
            },
            offset: if s.recover { s.offset } else { 0 },
            data: s.cfg.data.clone(),
            positioned: s.cfg.positioned,
            recoverable: s.cfg.recoverable,
            join_leave: s.cfg.join_leave,
            delta: if s.cfg.delta {
                "fossil".into()
            } else {
                String::new()
            },
            tf: s
                .cfg
                .tags_filter
                .as_ref()
                .map(crate::filter::FilterNode::to_proto),
            flag,
            ..Default::default()
        };
        let pending = Pending::Subscribe {
            sub: key,
            channel: s.cfg.channel.clone(),
            attempt: s.attempt,
            generation: self.generation,
        };
        let cmd = proto::Command {
            subscribe: Some(req),
            ..Default::default()
        };
        let id = self.send(transport, cmd, pending, now, out);
        if let Some(s) = self.subs.get_mut(&key) {
            s.inflight = Some(Inflight::Command(id));
        }
    }

    /// Arms the resubscribe delay (§6.3).
    fn schedule_resubscribe(&mut self, key: SubKey, now: Instant) {
        let Some(s) = self.subs.get_mut(&key) else {
            return;
        };
        s.inflight = None;
        let d = backoff(
            &mut self.rng,
            s.resubscribe_attempts,
            s.cfg.min_resubscribe_delay,
            s.cfg.max_resubscribe_delay,
        );
        s.resubscribe_attempts = s.resubscribe_attempts.saturating_add(1);
        self.timers.arm_after(TimerKey::Resubscribe(key), now, d);
    }

    fn sub_error(
        key: SubKey,
        kind: SubscriptionErrorKind,
        message: String,
        source: Option<super::ArcError>,
        out: &mut Outputs,
    ) {
        out.event(Event::SubscriptionError {
            sub: key,
            kind,
            message,
            source,
        });
    }

    /// Whether a flow result still belongs to the subscription's current flow.
    /// A result outdated only by an invalidation restarts the flow (§7.2).
    fn flow_current(&mut self, key: SubKey, attempt: u64, invalidations: u64) -> bool {
        let Some(s) = self.subs.get_mut(&key) else {
            return false;
        };
        if s.state != SubState::Subscribing || s.attempt != attempt {
            return false;
        }
        if s.invalidations != invalidations {
            s.inflight = None;
            self.ready.insert(key);
            return false;
        }
        true
    }

    pub(super) fn on_sub_token(
        &mut self,
        flow: Flow,
        result: Result<Option<String>, CallbackError>,
        now: Instant,
        out: &mut Outputs,
    ) {
        match flow {
            Flow::SubToken {
                sub: key,
                attempt,
                invalidations,
            } => {
                if !self.flow_current(key, attempt, invalidations) {
                    return;
                }
                match result {
                    Ok(Some(t)) if !t.is_empty() => {
                        if let Some(s) = self.subs.get_mut(&key) {
                            s.token = t;
                        }
                        self.send_subscribe(key, now, out);
                    }
                    Ok(_) | Err(CallbackError::Unauthorized) => {
                        self.unsubscribe_local(
                            key,
                            unsubscribed::UNAUTHORIZED,
                            "unauthorized",
                            out,
                        );
                    }
                    Err(CallbackError::Other(e)) => {
                        Self::sub_error(
                            key,
                            SubscriptionErrorKind::Token,
                            e.to_string(),
                            Some(e),
                            out,
                        );
                        self.schedule_resubscribe(key, now);
                    }
                }
            }
            Flow::SubRefreshToken {
                sub: key,
                session,
                invalidations,
            } => {
                if !self.refresh_current(key, session, invalidations) {
                    return;
                }
                match result {
                    Ok(Some(t)) if !t.is_empty() => {
                        let Some(transport) = self.conn.current.as_ref().map(|t| t.id) else {
                            return;
                        };
                        let Some(s) = self.subs.get_mut(&key) else {
                            return;
                        };
                        s.token.clone_from(&t);
                        let channel = s.cfg.channel.clone();
                        let cmd = proto::Command {
                            sub_refresh: Some(proto::SubRefreshRequest {
                                channel,
                                token: t,
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                        self.send(
                            transport,
                            cmd,
                            Pending::SubRefresh {
                                sub: key,
                                session,
                                invalidations,
                            },
                            now,
                            out,
                        );
                    }
                    Ok(_) | Err(CallbackError::Unauthorized) => {
                        self.unsubscribe_with_command(
                            key,
                            unsubscribed::UNAUTHORIZED,
                            "unauthorized",
                            now,
                            out,
                        );
                    }
                    Err(CallbackError::Other(e)) => {
                        Self::sub_error(
                            key,
                            SubscriptionErrorKind::Refresh,
                            e.to_string(),
                            Some(e),
                            out,
                        );
                        self.arm_sub_refresh_retry(key, now);
                    }
                }
            }
            _ => {}
        }
    }

    pub(super) fn on_sub_state(
        &mut self,
        flow: Flow,
        result: Result<StreamPosition, CallbackError>,
        now: Instant,
        out: &mut Outputs,
    ) {
        let Flow::SubState {
            sub: key,
            attempt,
            invalidations,
        } = flow
        else {
            return;
        };
        if !self.flow_current(key, attempt, invalidations) {
            return;
        }
        match result {
            Ok(pos) => {
                if let Some(s) = self.subs.get_mut(&key) {
                    s.recover = true;
                    s.offset = pos.offset;
                    s.epoch = pos.epoch;
                    s.inflight = None;
                }
                if self.state == State::Connected {
                    self.flow_step(key, now, out);
                }
            }
            Err(e) => {
                let (message, source) = match e {
                    CallbackError::Other(e) => (e.to_string(), Some(e)),
                    CallbackError::Unauthorized => ("get_state refused".to_string(), None),
                };
                Self::sub_error(key, SubscriptionErrorKind::GetState, message, source, out);
                self.schedule_resubscribe(key, now);
            }
        }
    }

    // ───────────────────────── subscribe replies ─────────────────────────

    /// A subscribe reply of a flow that is no longer current: only its deferred
    /// unsubscribe matters (§7.2, §7.3).
    pub(super) fn settle_outdated_subscribe(
        &mut self,
        channel: &str,
        cmd_id: u32,
        ok: bool,
        now: Instant,
        out: &mut Outputs,
    ) {
        let book = self.book(channel);
        if book.deferred == Some(cmd_id) {
            book.deferred = None;
            if ok {
                self.send_unsubscribe(channel, now, out);
            } else {
                self.mark_channel_ready(channel);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_subscribe_reply(
        &mut self,
        key: SubKey,
        channel: &str,
        attempt: u64,
        generation: u64,
        cmd_id: u32,
        reply: proto::Reply,
        now: Instant,
        out: &mut Outputs,
    ) {
        let current = generation == self.generation
            && self
                .subs
                .get(&key)
                .is_some_and(|s| s.attempt == attempt && s.state == SubState::Subscribing);
        if !current {
            let ok = reply.error.is_none() && reply.subscribe.is_some();
            self.settle_outdated_subscribe(channel, cmd_id, ok, now, out);
            return;
        }
        if let Some(s) = self.subs.get_mut(&key) {
            s.inflight = None;
        }
        if let Some(err) = reply.error {
            self.on_subscribe_error(key, err, now, out);
            return;
        }
        let Some(res) = reply.subscribe else {
            self.terminate(disconnected::BAD_PROTOCOL, "malformed reply", now, out);
            return;
        };
        self.on_subscribed(key, res, now, out);
    }

    fn on_subscribe_error(
        &mut self,
        key: SubKey,
        err: proto::Error,
        now: Instant,
        out: &mut Outputs,
    ) {
        let Some(s) = self.subs.get_mut(&key) else {
            return;
        };
        let (has_get_state, has_get_token) = (s.cfg.has_get_state, s.cfg.has_get_token);
        let message = format!("{} ({})", err.message, err.code);
        if err.code == codes::UNRECOVERABLE_POSITION && has_get_state {
            s.recover = false;
            s.offset = 0;
            s.epoch.clear();
            s.prev_data = None;
            self.schedule_resubscribe(key, now);
            return;
        }
        if err.code == codes::TOKEN_EXPIRED {
            if has_get_token {
                s.token.clear();
                Self::sub_error(key, SubscriptionErrorKind::Subscribe, message, None, out);
                self.schedule_resubscribe(key, now);
            } else {
                Self::sub_error(
                    key,
                    SubscriptionErrorKind::Configuration,
                    "subscription token expired and no token getter is set".into(),
                    None,
                    out,
                );
                self.unsubscribe_local(key, unsubscribed::UNAUTHORIZED, "unauthorized", out);
            }
            return;
        }
        if err.temporary || err.code < 100 {
            Self::sub_error(key, SubscriptionErrorKind::Subscribe, message, None, out);
            self.schedule_resubscribe(key, now);
            return;
        }
        // Permanent: unsubscribed only, as centrifuge-js `_subscribeError`.
        self.unsubscribe_local(key, err.code, &err.message, out);
    }

    fn on_subscribed(
        &mut self,
        key: SubKey,
        res: proto::SubscribeResult,
        now: Instant,
        out: &mut Outputs,
    ) {
        let protocol = self.cfg.protocol;
        let max = self.cfg.max_message_size;
        let Some(s) = self.subs.get_mut(&key) else {
            return;
        };
        // Delta negotiation applies to the recovered publications too (pattern
        // 69); the base resets at every subscribe.
        s.delta_negotiated = res.delta;
        s.prev_data = None;
        let channel = s.cfg.channel.clone();
        let mut recovered = Vec::with_capacity(res.publications.len());
        for p in res.publications {
            match resolve_data(s, protocol, max, &p) {
                Ok(data) => recovered.push((p, data)),
                Err(e) => {
                    let offset = p.offset;
                    self.delta_failed(&channel, offset, &e, now, out);
                    return;
                }
            }
        }
        s.state = SubState::Subscribed;
        s.session += 1;
        s.resubscribe_attempts = 0;
        // Position (§7.4): only a recoverable reply sets one; take its offset at
        // once (after a recovery it's the offset recovered from).
        if res.recoverable {
            s.recover = true;
            if !res.epoch.is_empty() || s.epoch.is_empty() || s.epoch == INVALIDATED_EPOCH {
                s.epoch.clone_from(&res.epoch);
            }
            s.offset = res.offset;
        } else {
            s.recover = false;
        }
        if res.id > 0 {
            s.push_id = res.id;
        }
        let session = s.session;
        let info = SubscribedInfo {
            was_recovering: res.was_recovering,
            recovered: res.recovered,
            recoverable: res.recoverable,
            positioned: res.positioned,
            stream_position: (res.positioned || res.recoverable)
                .then(|| StreamPosition::new(res.offset, res.epoch.clone())),
            data: res.data,
        };
        if res.id > 0 {
            self.by_push_id.insert(res.id, key);
        }
        if res.expires {
            let ttl = Duration::from_secs(u64::from(res.ttl.max(1)));
            self.timers.arm_after(TimerKey::SubRefresh(key), now, ttl);
        }
        out.event(Event::Subscribed { sub: key, info });
        for (publication, data) in recovered {
            self.deliveries.push_back(Delivery::SubPublication {
                sub: key,
                session,
                publication,
                data,
            });
        }
    }

    pub(super) fn delta_failed(
        &mut self,
        channel: &str,
        offset: u64,
        e: &delta::DeltaError,
        now: Instant,
        out: &mut Outputs,
    ) {
        out.event(Event::Error {
            kind: super::ClientErrorKind::Delta {
                channel: channel.to_owned(),
                offset,
            },
            message: e.to_string(),
            source: None,
        });
        self.terminate(disconnected::BAD_PROTOCOL, "delta error", now, out);
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_subscribe_timeout(
        &mut self,
        key: SubKey,
        channel: &str,
        attempt: u64,
        generation: u64,
        cmd_id: u32,
        now: Instant,
        out: &mut Outputs,
    ) {
        if generation != self.generation || self.state != State::Connected {
            return;
        }
        let current = self
            .subs
            .get(&key)
            .is_some_and(|s| s.attempt == attempt && s.state == SubState::Subscribing);
        let deferred = self
            .channels
            .get(channel)
            .is_some_and(|b| b.deferred == Some(cmd_id));
        if current || deferred {
            // Whether the server subscribed is unknown: start clean (§7.2).
            self.teardown(
                connecting::SUBSCRIBE_TIMEOUT,
                "subscribe timeout",
                true,
                now,
                out,
            );
        }
    }

    pub(super) fn on_subscribe_write_failed(
        &mut self,
        key: SubKey,
        attempt: u64,
        generation: u64,
        now: Instant,
        out: &mut Outputs,
    ) {
        if generation != self.generation || self.subs.get(&key).is_none_or(|s| s.attempt != attempt)
        {
            return;
        }
        Self::sub_error(
            key,
            SubscriptionErrorKind::Subscribe,
            "subscribe command could not be written".into(),
            None,
            out,
        );
        self.schedule_resubscribe(key, now);
    }

    // ───────────────────────── unsubscribe replies ─────────────────────────

    pub(super) fn on_unsubscribe_reply(
        &mut self,
        channel: &str,
        generation: u64,
        reply: proto::Reply,
        now: Instant,
        out: &mut Outputs,
    ) {
        if reply.error.is_some() {
            self.on_unsubscribe_failed(channel, generation, now, out);
            return;
        }
        if generation != self.generation {
            return;
        }
        if let Some(b) = self.channels.get_mut(channel) {
            b.unsub_waits = b.unsub_waits.saturating_sub(1);
        }
        self.mark_channel_ready(channel);
    }

    /// An unsubscribe that failed or timed out: the server may keep the
    /// subscription, so reconnect (pattern 79) — but only within its session.
    pub(super) fn on_unsubscribe_failed(
        &mut self,
        channel: &str,
        generation: u64,
        now: Instant,
        out: &mut Outputs,
    ) {
        if generation != self.generation || self.state != State::Connected {
            return;
        }
        let _ = channel;
        self.teardown(
            connecting::UNSUBSCRIBE_ERROR,
            "unsubscribe error",
            true,
            now,
            out,
        );
    }

    // ───────────────────────── subscription refresh ─────────────────────────

    fn refresh_current(&self, key: SubKey, session: u64, invalidations: u64) -> bool {
        self.state == State::Connected
            && self.subs.get(&key).is_some_and(|s| {
                s.state == SubState::Subscribed
                    && s.session == session
                    && s.invalidations == invalidations
            })
    }

    pub(super) fn on_sub_refresh_timer(&mut self, key: SubKey, out: &mut Outputs) {
        let Some(s) = self.subs.get(&key) else { return };
        if s.state != SubState::Subscribed || !s.cfg.has_get_token || self.state != State::Connected
        {
            return;
        }
        let (session, invalidations, channel) = (s.session, s.invalidations, s.cfg.channel.clone());
        let flow = self.new_flow(Flow::SubRefreshToken {
            sub: key,
            session,
            invalidations,
        });
        out.callbacks.push(super::Callback::SubscriptionToken {
            flow,
            sub: key,
            channel,
        });
    }

    fn arm_sub_refresh_retry(&mut self, key: SubKey, now: Instant) {
        let d = self
            .rng
            .between(Duration::from_secs(10), Duration::from_secs(20));
        self.timers.arm_after(TimerKey::SubRefresh(key), now, d);
    }

    fn unsubscribe_with_command(
        &mut self,
        key: SubKey,
        code: u32,
        reason: &str,
        now: Instant,
        out: &mut Outputs,
    ) {
        let Some(s) = self.subs.get(&key) else { return };
        let channel = s.cfg.channel.clone();
        if s.state == SubState::Subscribed && self.state == State::Connected {
            self.send_unsubscribe(&channel, now, out);
        }
        self.unsubscribe_local(key, code, reason, out);
    }

    pub(super) fn on_sub_refresh_reply(
        &mut self,
        key: SubKey,
        session: u64,
        invalidations: u64,
        reply: proto::Reply,
        now: Instant,
        out: &mut Outputs,
    ) {
        if !self.refresh_current(key, session, invalidations) {
            return;
        }
        if let Some(err) = reply.error {
            self.on_sub_refresh_failed(key, session, invalidations, Some(err), now, out);
            return;
        }
        let Some(r) = reply.sub_refresh else {
            self.terminate(disconnected::BAD_PROTOCOL, "malformed reply", now, out);
            return;
        };
        if r.expires {
            let ttl = Duration::from_secs(u64::from(r.ttl.max(1)));
            self.timers.arm_after(TimerKey::SubRefresh(key), now, ttl);
        }
    }

    pub(super) fn on_sub_refresh_failed(
        &mut self,
        key: SubKey,
        session: u64,
        invalidations: u64,
        err: Option<proto::Error>,
        now: Instant,
        out: &mut Outputs,
    ) {
        if !self.refresh_current(key, session, invalidations) {
            return;
        }
        match err {
            Some(e) if !e.temporary => {
                self.unsubscribe_with_command(key, e.code, &e.message, now, out);
            }
            e => {
                let message = e.map_or_else(
                    || "subscription refresh timed out".to_string(),
                    |e| format!("{} ({})", e.message, e.code),
                );
                Self::sub_error(key, SubscriptionErrorKind::Refresh, message, None, out);
                self.arm_sub_refresh_retry(key, now);
            }
        }
    }

    // ───────────────────────── channel pushes ─────────────────────────

    /// Routes a channel push (§8, pattern 65): to a client-side subscription by
    /// compaction id, else by channel unless a server-side entry owns it.
    pub(super) fn on_channel_push(&mut self, push: proto::Push, now: Instant, out: &mut Outputs) {
        let client_key = if push.id > 0 {
            match self.by_push_id.get(&push.id) {
                Some(k) => Some(*k),
                None => return, // compacted push with an unknown id: dropped
            }
        } else {
            let candidate = self
                .subs
                .values()
                .find(|s| s.cfg.channel == push.channel)
                .map(|s| (s.key, s.state));
            match candidate {
                Some((k, SubState::Subscribed)) => Some(k),
                Some((k, _))
                    if !self.server_subs.contains_key(&push.channel)
                        && push.subscribe.is_none() =>
                {
                    Some(k)
                }
                _ => None,
            }
        };
        match client_key {
            Some(key) => self.client_push(key, push, now, out),
            None => self.server_push(push, out),
        }
    }

    fn client_push(&mut self, key: SubKey, push: proto::Push, now: Instant, out: &mut Outputs) {
        let Some(s) = self.subs.get(&key) else { return };
        if s.state != SubState::Subscribed {
            return; // pushes only while subscribed (patterns 16, 37)
        }
        if let Some(p) = push.r#pub {
            self.live_publication(key, p, now, out);
        } else if let Some(j) = push.join {
            out.event(Event::Join {
                sub: key,
                info: j.info.map(ClientInfo::from).unwrap_or_default(),
            });
        } else if let Some(l) = push.leave {
            out.event(Event::Leave {
                sub: key,
                info: l.info.map(ClientInfo::from).unwrap_or_default(),
            });
        } else if let Some(u) = push.unsubscribe {
            if u.code < 2500 {
                self.unsubscribe_local(key, u.code, &u.reason, out);
            } else {
                if u.code == codes::UNSUBSCRIBE_STATE_INVALIDATED {
                    self.invalidate_sub(key);
                }
                self.timers.cancel(TimerKey::SubRefresh(key));
                if let Some(s) = self.subs.get_mut(&key) {
                    s.state = SubState::Subscribing;
                    let push_id = std::mem::take(&mut s.push_id);
                    // The id was this subscription's; the resubscribe gets a new one.
                    if push_id > 0 && self.by_push_id.get(&push_id) == Some(&key) {
                        self.by_push_id.remove(&push_id);
                    }
                }
                out.event(Event::Subscribing {
                    sub: key,
                    code: u.code,
                    reason: u.reason,
                });
                self.ready.insert(key);
            }
        }
    }

    fn live_publication(
        &mut self,
        key: SubKey,
        p: proto::Publication,
        now: Instant,
        out: &mut Outputs,
    ) {
        let protocol = self.cfg.protocol;
        let max = self.cfg.max_message_size;
        let Some(s) = self.subs.get_mut(&key) else {
            return;
        };
        let own_stream = p.channel.is_empty() || p.channel == s.cfg.channel;
        // Defensive dedupe of live publications (§7.4).
        if own_stream
            && s.recover
            && p.offset > 0
            && p.offset <= s.offset
            && (p.epoch.is_empty() || p.epoch == s.epoch)
        {
            return;
        }
        let data = match resolve_data(s, protocol, max, &p) {
            Ok(d) => d,
            Err(e) => {
                let (channel, offset) = (s.cfg.channel.clone(), p.offset);
                self.delta_failed(&channel, offset, &e, now, out);
                return;
            }
        };
        let session = s.session;
        if own_stream && p.offset > 0 {
            let epoch = (!p.epoch.is_empty()).then(|| p.epoch.clone());
            self.commits.push(Commit::Sub {
                sub: key,
                session,
                offset: p.offset,
                epoch,
            });
        }
        out.event(Event::Publication {
            sub: key,
            publication: Publication::from_proto(p, data),
        });
    }
}

/// The publication's full payload: delta applied when negotiated (§7.5).
fn resolve_data(
    s: &mut Sub,
    protocol: Protocol,
    max: usize,
    p: &proto::Publication,
) -> Result<Bytes, delta::DeltaError> {
    if !s.delta_negotiated {
        if p.delta {
            return Err(delta::DeltaError::Invalid(
                "delta publication without negotiated delta",
            ));
        }
        return Ok(p.data.clone());
    }
    let raw = unwrap_json_string(protocol, &p.data)?;
    if p.delta {
        let Some(base) = &s.prev_data else {
            return Err(delta::DeltaError::Invalid(
                "delta publication without a base",
            ));
        };
        let full = Bytes::from(delta::apply(base, &raw, max)?);
        s.prev_data = Some(full.clone());
        Ok(full)
    } else {
        s.prev_data = Some(raw.clone());
        Ok(raw)
    }
}

/// With JSON, delta-channel payloads travel as a JSON string (§7.5).
#[cfg_attr(not(feature = "json"), allow(clippy::unnecessary_wraps))]
fn unwrap_json_string(protocol: Protocol, data: &Bytes) -> Result<Bytes, delta::DeltaError> {
    match protocol {
        Protocol::Protobuf => Ok(data.clone()),
        #[cfg(feature = "json")]
        Protocol::Json => serde_json::from_slice::<String>(data)
            .map(Bytes::from)
            .map_err(|_| delta::DeltaError::Invalid("delta payload is not a JSON string")),
    }
}
