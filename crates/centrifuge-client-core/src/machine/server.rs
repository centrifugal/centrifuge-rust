//! Server-side subscriptions (docs/spec.md §8).

use std::collections::HashMap;

use super::{Core, Delivery, Event, Outputs, SubscribedInfo};
use crate::proto;
use crate::types::{ClientInfo, Publication, StreamPosition};

#[derive(Debug, Clone, Default)]
pub(super) struct ServerSub {
    pub(super) offset: u64,
    pub(super) epoch: String,
    pub(super) recoverable: bool,
}

impl ServerSub {
    pub(super) fn has_position(&self) -> bool {
        self.recoverable
    }

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

fn info_of(res: &proto::SubscribeResult) -> SubscribedInfo {
    SubscribedInfo {
        was_recovering: res.was_recovering,
        recovered: res.recovered,
        recoverable: res.recoverable,
        positioned: res.positioned,
        stream_position: (res.positioned || res.recoverable)
            .then(|| StreamPosition::new(res.offset, res.epoch.clone())),
        data: res.data.clone(),
    }
}

impl Core {
    /// Connect reply: drop entries the server no longer has, then announce each
    /// one in its own step (TLA+ `Sdk_SSSubscribed`).
    pub(super) fn process_server_subs_of_reply(
        &mut self,
        subs: HashMap<String, proto::SubscribeResult>,
        out: &mut Outputs,
    ) {
        let mut gone: Vec<String> = self
            .server_subs
            .keys()
            .filter(|c| !subs.contains_key(*c))
            .cloned()
            .collect();
        gone.sort();
        for channel in gone {
            self.server_subs.remove(&channel);
            out.event(Event::ServerUnsubscribed { channel });
        }
        let mut subs: Vec<_> = subs.into_iter().collect();
        subs.sort_by(|a, b| a.0.cmp(&b.0));
        let attempt = self.conn.attempt;
        for (channel, result) in subs {
            self.deliveries.push_back(Delivery::ServerSubscribed {
                attempt,
                channel,
                result: Box::new(result),
            });
        }
    }

    pub(super) fn server_subscribed_from_reply(
        &mut self,
        attempt: u64,
        channel: &str,
        res: proto::SubscribeResult,
        out: &mut Outputs,
    ) {
        let entry = self.server_subs.entry(channel.to_owned()).or_default();
        if !res.epoch.is_empty()
            || entry.epoch.is_empty()
            || entry.epoch == super::sub::INVALIDATED_EPOCH
        {
            entry.epoch.clone_from(&res.epoch);
        }
        entry.offset = res.offset;
        entry.recoverable = res.recoverable;
        out.event(Event::ServerSubscribed {
            channel: channel.to_owned(),
            info: info_of(&res),
        });
        // Recovered publications go next, one per step, before any inbound item.
        let mut pending = Vec::with_capacity(res.publications.len());
        for p in res.publications {
            pending.push(Delivery::ServerPublication {
                attempt,
                channel: channel.to_owned(),
                publication: p,
            });
        }
        for d in pending.into_iter().rev() {
            self.deliveries.push_front(d);
        }
    }

    /// A channel push not routed to a client-side subscription.
    pub(super) fn server_push(&mut self, push: proto::Push, out: &mut Outputs) {
        let channel = push.channel;
        if let Some(s) = push.subscribe {
            if self.server_subs.contains_key(&channel) {
                return;
            }
            self.server_subs.insert(
                channel.clone(),
                ServerSub {
                    offset: s.offset,
                    epoch: s.epoch.clone(),
                    recoverable: s.recoverable,
                },
            );
            let info = SubscribedInfo {
                was_recovering: false,
                recovered: false,
                recoverable: s.recoverable,
                positioned: s.positioned,
                stream_position: (s.positioned || s.recoverable)
                    .then(|| StreamPosition::new(s.offset, s.epoch)),
                data: s.data,
            };
            out.event(Event::ServerSubscribed { channel, info });
            return;
        }
        if !self.server_subs.contains_key(&channel) {
            return;
        }
        if push.unsubscribe.is_some() {
            // Any code: a server-side subscription can't resubscribe itself.
            self.server_subs.remove(&channel);
            out.event(Event::ServerUnsubscribed { channel });
        } else if let Some(p) = push.r#pub {
            let own_stream = p.channel.is_empty() || p.channel == channel;
            // Defensive dedupe, as for client-side subscriptions (§7.4): a live
            // publication at or below the committed offset of the same epoch
            // was delivered already.
            let duplicate = self.server_subs.get(&channel).is_some_and(|s| {
                s.has_position()
                    && own_stream
                    && p.offset > 0
                    && p.offset <= s.offset
                    && (p.epoch.is_empty() || p.epoch == s.epoch)
            });
            if duplicate {
                return;
            }
            if p.offset > 0 && own_stream {
                let epoch = (!p.epoch.is_empty()).then(|| p.epoch.clone());
                self.commits.push(super::Commit::Server {
                    channel: channel.clone(),
                    offset: p.offset,
                    epoch,
                });
            }
            let data = p.data.clone();
            out.event(Event::ServerPublication {
                channel,
                publication: Publication::from_proto(p, data),
            });
        } else if let Some(j) = push.join {
            out.event(Event::ServerJoin {
                channel,
                info: j.info.map(ClientInfo::from).unwrap_or_default(),
            });
        } else if let Some(l) = push.leave {
            out.event(Event::ServerLeave {
                channel,
                info: l.info.map(ClientInfo::from).unwrap_or_default(),
            });
        }
    }

    // ───────────────────────── callback dispatch ─────────────────────────

    pub(super) fn on_token(
        &mut self,
        flow: super::FlowId,
        result: Result<Option<String>, super::CallbackError>,
        now: crate::time::Instant,
        out: &mut Outputs,
    ) {
        let Some(f) = self.flows.remove(&flow) else {
            return;
        };
        match f {
            super::Flow::ConnectToken { .. } | super::Flow::RefreshToken { .. } => {
                self.on_connection_token(f, result, now, out);
            }
            super::Flow::SubToken { .. } | super::Flow::SubRefreshToken { .. } => {
                self.on_sub_token(f, result, now, out);
            }
            super::Flow::SubState { .. } => {}
        }
    }

    pub(super) fn on_state(
        &mut self,
        flow: super::FlowId,
        result: Result<StreamPosition, super::CallbackError>,
        now: crate::time::Instant,
        out: &mut Outputs,
    ) {
        let Some(f) = self.flows.remove(&flow) else {
            return;
        };
        self.on_sub_state(f, result, now, out);
    }
}
