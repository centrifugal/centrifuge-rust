//! Step-level tests of the core: no runtime, no sockets, a fake clock.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::*;
use crate::proto;

/// Drives a [`Core`] the way the driver does, recording what it asks for.
struct H {
    core: Core,
    now: Instant,
    events: Vec<Event>,
    commands: Vec<(TransportId, proto::Command)>,
    opens: Vec<OpenRequest>,
    closes: Vec<TransportId>,
    callbacks: Vec<Callback>,
}

impl H {
    fn new(cfg: CoreConfig) -> Self {
        Self {
            core: Core::new(cfg),
            now: Instant::now(),
            events: vec![],
            commands: vec![],
            opens: vec![],
            closes: vec![],
            callbacks: vec![],
        }
    }

    /// The JSON protocol when built with it (the payload rules differ).
    fn json() -> Self {
        #[cfg(feature = "json")]
        let protocol = Protocol::Json;
        #[cfg(not(feature = "json"))]
        let protocol = Protocol::Protobuf;
        Self::new(CoreConfig {
            protocol,
            ..CoreConfig::default()
        })
    }

    /// HTTP-streaming: commands may reach the server out of order.
    fn emulation() -> Self {
        let mut h = Self::json();
        h.core = Core::new(CoreConfig {
            protocol: h.core.protocol(),
            transports: vec![TransportKind::HttpStream],
            ..CoreConfig::default()
        });
        h
    }

    /// One step, then the driver's follow-ups: the delivery ack (handlers ran
    /// fine) and every queued delivery.
    fn step(&mut self, input: Input) {
        self.step_raw(input, true);
        while self.core.has_pending_delivery() {
            self.step_raw(Input::DeliverNext, true);
        }
    }

    fn step_raw(&mut self, input: Input, ack: bool) {
        let out = self.core.step(input, self.now);
        let had_events = !out.events.is_empty();
        self.events.extend(out.events);
        self.commands.extend(out.commands);
        self.opens.extend(out.open);
        self.closes.extend(out.close);
        self.callbacks.extend(out.callbacks);
        if had_events && ack {
            self.step_raw(
                Input::Delivered {
                    ok: true,
                    panic: None,
                },
                true,
            );
        }
    }

    fn op(&mut self, op: Op) {
        self.step(Input::App(op));
    }

    /// Advances the clock by `d`, firing every deadline that passes, in order.
    fn advance(&mut self, d: Duration) {
        let until = self.now + d;
        while let Some((at, key)) = self.core.next_deadline() {
            if at > until {
                break;
            }
            self.now = at;
            self.step(Input::TimerFired {
                key,
                late: false,
                last_received: None,
            });
        }
        self.now = until;
    }

    fn deadline(&self, key: TimerKey) -> Instant {
        self.core.timers.deadline(key).expect("timer armed")
    }

    fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    fn take_commands(&mut self) -> Vec<proto::Command> {
        std::mem::take(&mut self.commands)
            .into_iter()
            .map(|(_, c)| c)
            .collect()
    }

    fn last_open(&self) -> &OpenRequest {
        self.opens.last().expect("an open request")
    }

    fn reply(&mut self, transport: TransportId, reply: proto::Reply) {
        self.step(Input::Item {
            transport,
            reply: Box::new(reply),
        });
    }

    fn push(&mut self, transport: TransportId, push: proto::Push) {
        self.reply(
            transport,
            proto::Reply {
                push: Some(push),
                ..Default::default()
            },
        );
    }

    /// Connects over WebSocket with `res` as the connect result. Returns the
    /// transport id.
    fn connect_with(&mut self, res: proto::ConnectResult) -> TransportId {
        self.op(Op::Connect);
        let t = self.last_open().transport;
        self.step(Input::Opened { transport: t });
        let mut cmds = self.take_commands();
        // Over emulation the connect is the stream request's body.
        cmds.extend(self.last_open().initial.clone());
        let connect = cmds
            .iter()
            .find(|c| c.connect.is_some())
            .expect("connect command");
        self.reply(
            t,
            proto::Reply {
                id: connect.id,
                connect: Some(res),
                ..Default::default()
            },
        );
        t
    }

    fn connect(&mut self) -> TransportId {
        self.connect_with(proto::ConnectResult {
            client: "c1".into(),
            ping: 25,
            pong: true,
            ..Default::default()
        })
    }

    fn new_sub(&mut self, key: SubKey, cfg: SubConfig) {
        self.op(Op::NewSubscription {
            key,
            config: Box::new(cfg),
        });
    }
}

fn names(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| match e {
            Event::Connecting { code, .. } => format!("connecting({code})"),
            Event::Connected { .. } => "connected".into(),
            Event::Disconnected { code, .. } => format!("disconnected({code})"),
            Event::Error { kind, .. } => format!("error({kind:?})"),
            Event::Message { .. } => "message".into(),
            Event::ServerSubscribed { channel, .. } => format!("server_subscribed({channel})"),
            Event::ServerSubscribing { channel } => format!("server_subscribing({channel})"),
            Event::ServerUnsubscribed { channel } => format!("server_unsubscribed({channel})"),
            Event::ServerPublication {
                channel,
                publication,
            } => format!("server_pub({channel},{})", publication.offset),
            Event::ServerJoin { .. } => "server_join".into(),
            Event::ServerLeave { .. } => "server_leave".into(),
            Event::Subscribing { sub, code, .. } => format!("subscribing({sub},{code})"),
            Event::Subscribed { sub, .. } => format!("subscribed({sub})"),
            Event::Unsubscribed { sub, code, .. } => format!("unsubscribed({sub},{code})"),
            Event::SubscriptionError { sub, kind, .. } => format!("sub_error({sub},{kind:?})"),
            Event::Publication { sub, publication } => format!("pub({sub},{})", publication.offset),
            Event::Join { .. } => "join".into(),
            Event::Leave { .. } => "leave".into(),
        })
        .collect()
}

fn subscribe_ok(cmd_id: u32, res: proto::SubscribeResult) -> proto::Reply {
    proto::Reply {
        id: cmd_id,
        subscribe: Some(res),
        ..Default::default()
    }
}

fn publication(offset: u64, data: &'static [u8]) -> proto::Publication {
    proto::Publication {
        offset,
        data: Bytes::from_static(data),
        ..Default::default()
    }
}

fn subscribe_cmd(cmds: &[proto::Command]) -> &proto::Command {
    cmds.iter()
        .find(|c| c.subscribe.is_some())
        .expect("a subscribe command")
}

// ───────────────────────────── connection ─────────────────────────────

#[test]
fn connect_events_and_first_frame() {
    let mut h = H::json();
    h.op(Op::Connect);
    assert_eq!(names(&h.take_events()), ["connecting(0)"]);
    assert_eq!(h.core.state(), State::Connecting);
    let t = h.last_open().transport;
    assert!(
        h.last_open().initial.is_none(),
        "WebSocket sends connect after the upgrade"
    );
    assert!(h.commands.is_empty(), "nothing before the upgrade");
    h.step(Input::Opened { transport: t });
    let cmds = h.take_commands();
    assert_eq!(cmds.len(), 1);
    let c = cmds[0].connect.as_ref().unwrap();
    assert_eq!(c.name, "rust");
    h.reply(
        t,
        proto::Reply {
            id: cmds[0].id,
            connect: Some(proto::ConnectResult {
                client: "x".into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    assert_eq!(names(&h.take_events()), ["connected"]);
    assert_eq!(h.core.state(), State::Connected);
    assert!(h.core.session().is_some());
}

#[test]
fn token_is_fetched_before_dialing_and_none_means_anonymous() {
    let mut h = H::new(CoreConfig {
        has_get_token: true,
        ..CoreConfig::default()
    });
    h.op(Op::Connect);
    assert!(
        h.opens.is_empty(),
        "no transport while the getter runs (centrifuge-go #165)"
    );
    let Callback::ConnectionToken { flow } = h.callbacks.pop().unwrap() else {
        panic!()
    };
    h.step(Input::TokenSettled {
        flow,
        result: Ok(None),
    });
    let t = h.last_open().transport;
    h.step(Input::Opened { transport: t });
    let cmds = h.take_commands();
    assert_eq!(cmds[0].connect.as_ref().unwrap().token, "");
}

#[test]
fn token_unauthorized_disconnects_and_other_errors_retry() {
    let mut h = H::new(CoreConfig {
        has_get_token: true,
        ..CoreConfig::default()
    });
    h.op(Op::Connect);
    let Callback::ConnectionToken { flow } = h.callbacks.pop().unwrap() else {
        panic!()
    };
    h.step(Input::TokenSettled {
        flow,
        result: Err(CallbackError::Other(Arc::new(std::io::Error::other(
            "down",
        )))),
    });
    assert_eq!(
        names(&h.take_events()),
        ["connecting(0)", "error(ConnectToken)"]
    );
    assert_eq!(h.core.state(), State::Connecting);
    h.advance(Duration::from_secs(2));
    let Callback::ConnectionToken { flow } = h.callbacks.pop().unwrap() else {
        panic!("retried")
    };
    h.step(Input::TokenSettled {
        flow,
        result: Err(CallbackError::Unauthorized),
    });
    assert_eq!(names(&h.take_events()), ["disconnected(1)"]);
    assert_eq!(h.core.state(), State::Disconnected);
}

#[test]
fn stale_token_result_of_a_superseded_attempt_is_ignored() {
    let mut h = H::new(CoreConfig {
        has_get_token: true,
        ..CoreConfig::default()
    });
    h.op(Op::Connect);
    let Callback::ConnectionToken { flow: old } = h.callbacks.pop().unwrap() else {
        panic!()
    };
    h.op(Op::Disconnect);
    h.op(Op::Connect);
    let Callback::ConnectionToken { flow: new } = h.callbacks.pop().unwrap() else {
        panic!()
    };
    h.step(Input::TokenSettled {
        flow: old,
        result: Ok(Some("old".into())),
    });
    assert!(
        h.opens.is_empty(),
        "the old attempt's token opens nothing (pattern 11)"
    );
    h.step(Input::TokenSettled {
        flow: new,
        result: Ok(Some("new".into())),
    });
    assert_eq!(h.opens.len(), 1);
}

#[test]
fn disconnect_from_disconnected_emits_nothing() {
    let mut h = H::json();
    h.op(Op::Disconnect);
    assert!(h.take_events().is_empty());
    let before = h.core.disconnects();
    h.op(Op::Disconnect);
    assert!(
        h.core.disconnects() > before,
        "still fails calls waiting for a connect"
    );
}

#[test]
fn server_disconnect_reconnect_vs_terminal() {
    let mut h = H::json();
    let t = h.connect();
    h.take_events();
    h.push(
        t,
        proto::Push {
            disconnect: Some(proto::Disconnect {
                code: 3001,
                reason: "shutdown".into(),
                reconnect: true,
            }),
            ..Default::default()
        },
    );
    assert_eq!(names(&h.take_events()), ["connecting(3001)"]);
    assert_eq!(h.core.state(), State::Connecting);

    let mut h = H::json();
    let t = h.connect();
    h.take_events();
    h.push(
        t,
        proto::Push {
            disconnect: Some(proto::Disconnect {
                code: 3501,
                reason: "bad request".into(),
                reconnect: false,
            }),
            ..Default::default()
        },
    );
    assert_eq!(names(&h.take_events()), ["disconnected(3501)"]);
}

/// HTTP-streaming reports a refused connect (invalid token) with a disconnect
/// push before EOF, not a close code: honored while connecting, no retry loop.
#[test]
fn disconnect_push_while_connecting_is_honored() {
    let mut h = H::json();
    h.op(Op::Connect);
    let t = h.last_open().transport;
    h.step(Input::Opened { transport: t });
    h.take_events();
    h.push(
        t,
        proto::Push {
            disconnect: Some(proto::Disconnect {
                code: 3500,
                reason: "invalid token".into(),
                reconnect: false,
            }),
            ..Default::default()
        },
    );
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Eof,
    });
    assert_eq!(names(&h.take_events()), ["disconnected(3500)"]);
    assert_eq!(h.core.state(), State::Disconnected);
    assert!(h.core.next_deadline().is_none(), "no reconnect scheduled");
}

#[test]
fn close_frame_codes_below_3000_reconnect_and_1009_stops() {
    let mut h = H::json();
    let t = h.connect();
    h.take_events();
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Frame {
            code: 1006,
            reason: String::new(),
        },
    });
    assert_eq!(names(&h.take_events()), ["connecting(1)"]);

    let mut h = H::json();
    let t = h.connect();
    h.take_events();
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Frame {
            code: 1009,
            reason: String::new(),
        },
    });
    assert_eq!(names(&h.take_events()), ["disconnected(3)"]);
}

#[test]
fn ping_pong_on_current_transport_and_no_ping_reconnects() {
    let mut h = H::json();
    let t = h.connect();
    h.take_events();
    h.reply(t, proto::Reply::default());
    let cmds = h.take_commands();
    assert_eq!(cmds, vec![proto::Command::default()], "pong");
    // A ping from an old transport is never answered (pattern 29).
    h.reply(t + 100, proto::Reply::default());
    assert_eq!(h.take_commands(), Vec::<proto::Command>::new());
    h.advance(Duration::from_secs(36));
    assert_eq!(names(&h.take_events()), ["connecting(2)"]);
}

#[test]
fn liveness_counts_received_data_not_processed_data() {
    let mut h = H::json();
    h.connect();
    h.take_events();
    let (at, key) = (h.deadline(TimerKey::NoPing), TimerKey::NoPing);
    h.now = at;
    // Data arrived a moment ago but waits behind a slow handler.
    h.step(Input::TimerFired {
        key,
        late: false,
        last_received: at.checked_sub(Duration::from_secs(1)),
    });
    assert!(
        h.take_events().is_empty(),
        "no false no-ping (centrifuge-java F12b)"
    );
    assert_eq!(h.core.state(), State::Connected);
}

/// The busy-connection grace of a command timeout is granted once: a server
/// that keeps pinging can't keep an unanswered subscribe pending forever
/// (found by `sdk_validation` chaos-fake, seed 195).
#[test]
fn command_timeout_grace_while_data_arrives_is_granted_once() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    h.take_events();
    let key = TimerKey::Command(id);
    for round in 0..2 {
        let at = h.deadline(key);
        h.now = at;
        // A server ping arrived just before: data keeps flowing.
        h.step(Input::TimerFired {
            key,
            late: false,
            last_received: at.checked_sub(Duration::from_millis(100)),
        });
        if round == 0 {
            assert_eq!(
                h.core.state(),
                State::Connected,
                "one grace while data arrives"
            );
        }
    }
    assert_eq!(
        h.core.state(),
        State::Connecting,
        "the subscribe timed out: reconnect (the server state is unknown)"
    );
    assert!(names(&h.take_events()).iter().any(|e| e == "connecting(3)"));
}

/// A system sleep longer than the ping window reconnects at once; a short one
/// only re-arms the liveness check (spec §9.7).
#[test]
fn system_sleep_past_the_ping_window_reconnects_at_once() {
    let mut h = H::json();
    h.connect();
    h.take_events();
    h.step(Input::Resumed {
        slept: Duration::from_secs(5),
    });
    assert_eq!(
        h.core.state(),
        State::Connected,
        "a short sleep: still connected"
    );
    assert!(h.take_events().is_empty());
    h.step(Input::Resumed {
        slept: Duration::from_secs(3600),
    });
    assert_eq!(h.core.state(), State::Connecting);
    assert_eq!(names(&h.take_events()), ["connecting(1)"]);
}

/// After a system sleep, refresh deadlines move earlier by the time slept: the
/// token expired in wall-clock time meanwhile.
#[test]
fn refresh_deadlines_move_earlier_after_a_sleep() {
    let mut h = H::new(CoreConfig {
        token: "t".into(),
        has_get_token: true,
        ..CoreConfig::default()
    });
    h.connect_with(proto::ConnectResult {
        client: "c".into(),
        expires: true,
        ttl: 40,
        ping: 25,
        pong: true,
        ..Default::default()
    });
    let before = h.deadline(TimerKey::Refresh);
    h.step(Input::Resumed {
        slept: Duration::from_secs(20),
    });
    assert_eq!(
        h.deadline(TimerKey::Refresh),
        before.checked_sub(Duration::from_secs(20)).unwrap()
    );
    // Shorter than the ping window (25 + 10 s): still connected, refresh due.
    h.step(Input::Resumed {
        slept: Duration::from_secs(30),
    });
    assert_eq!(h.core.state(), State::Connected);
    assert!(h.deadline(TimerKey::Refresh) <= h.now, "due now");
}

/// The defensive dedupe of live publications (§7.4) covers server-side
/// subscriptions too: an offset already delivered isn't delivered again.
#[test]
fn server_side_live_publication_dedupe() {
    let mut h = H::json();
    let mut subs = HashMap::new();
    subs.insert(
        "s1".to_owned(),
        proto::SubscribeResult {
            recoverable: true,
            positioned: true,
            epoch: "e".into(),
            offset: 5,
            ..Default::default()
        },
    );
    let t = h.connect_with(proto::ConnectResult {
        client: "c".into(),
        subs,
        ..Default::default()
    });
    h.take_events();
    for offset in [5, 6, 6] {
        h.push(
            t,
            proto::Push {
                channel: "s1".into(),
                r#pub: Some(publication(offset, b"{}")),
                ..Default::default()
            },
        );
    }
    let offsets: Vec<u64> = h
        .take_events()
        .into_iter()
        .filter_map(|e| match e {
            Event::ServerPublication { publication, .. } => Some(publication.offset),
            _ => None,
        })
        .collect();
    assert_eq!(offsets, [6], "5 was the reply's position, 6 arrives once");
}

#[test]
fn late_timer_gets_one_grace_period() {
    let mut h = H::json();
    h.connect();
    h.take_events();
    let (at, key) = (h.deadline(TimerKey::NoPing), TimerKey::NoPing);
    h.now = at;
    h.step(Input::TimerFired {
        key,
        late: true,
        last_received: None,
    });
    assert_eq!(h.core.state(), State::Connected, "graced");
    assert_eq!(h.deadline(TimerKey::NoPing), at + Duration::from_secs(1));
    h.now = at + Duration::from_secs(1);
    h.step(Input::TimerFired {
        key,
        late: true,
        last_received: None,
    });
    assert_eq!(h.core.state(), State::Connecting, "only one grace");
}

#[test]
fn connect_error_rules() {
    // Temporary: retried.
    let mut h = H::json();
    h.op(Op::Connect);
    let t = h.last_open().transport;
    h.step(Input::Opened { transport: t });
    let id = h.take_commands()[0].id;
    h.reply(
        t,
        proto::Reply {
            id,
            error: Some(proto::Error {
                code: 100,
                message: "internal".into(),
                temporary: true,
            }),
            ..Default::default()
        },
    );
    assert_eq!(names(&h.take_events()), ["connecting(0)", "error(Connect)"]);
    assert_eq!(h.core.state(), State::Connecting);
    // Permanent: disconnected with the server's code.
    h.advance(Duration::from_secs(2));
    let t = h.last_open().transport;
    h.step(Input::Opened { transport: t });
    let id = h.take_commands()[0].id;
    h.reply(
        t,
        proto::Reply {
            id,
            error: Some(proto::Error {
                code: 103,
                message: "permission denied".into(),
                temporary: false,
            }),
            ..Default::default()
        },
    );
    assert_eq!(
        names(&h.take_events()),
        ["error(Connect)", "disconnected(103)"]
    );
}

#[test]
fn static_token_expired_without_getter_stops_after_one_retry() {
    let mut h = H::new(CoreConfig {
        token: "static".into(),
        ..CoreConfig::default()
    });
    h.op(Op::Connect);
    let t = h.last_open().transport;
    h.step(Input::Opened { transport: t });
    let id = h.take_commands()[0].id;
    h.reply(
        t,
        proto::Reply {
            id,
            error: Some(proto::Error {
                code: 109,
                message: "token expired".into(),
                temporary: false,
            }),
            ..Default::default()
        },
    );
    h.advance(Duration::from_secs(2));
    assert!(names(&h.take_events()).ends_with(&[
        "error(Configuration)".to_string(),
        "disconnected(1)".to_string()
    ]));
}

#[test]
fn undecodable_data_before_first_message_is_a_transport_failure() {
    let mut h = H::json();
    h.op(Op::Connect);
    let t = h.last_open().transport;
    h.step(Input::Opened { transport: t });
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Undecodable(crate::codec::DecodeError::Json("html".into())),
    });
    assert_eq!(
        h.core.state(),
        State::Connecting,
        "a captive portal never stops the client (centrifuge-dart #160)"
    );
    // After the connection is proven it's terminal.
    let mut h = H::json();
    let t = h.connect();
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Undecodable(crate::codec::DecodeError::Json("junk".into())),
    });
    assert_eq!(h.core.state(), State::Disconnected);
}

#[test]
fn fallback_first_round_is_immediate_and_proven_transport_sticks() {
    let mut h = H::new(CoreConfig {
        transports: vec![TransportKind::WebSocket, TransportKind::HttpStream],
        ..CoreConfig::default()
    });
    h.op(Op::Connect);
    assert_eq!(h.last_open().kind, TransportKind::WebSocket);
    h.advance(Duration::from_secs(5)); // open timeout
    assert_eq!(h.opens.len(), 2, "next transport at once");
    assert_eq!(h.last_open().kind, TransportKind::HttpStream);
    assert!(
        h.last_open()
            .initial
            .as_ref()
            .is_some_and(|c| c.connect.is_some()),
        "connect in the open request"
    );
    let t = h.last_open().transport;
    h.step(Input::Opened { transport: t });
    let id = h.last_open().initial.as_ref().unwrap().id;
    h.reply(
        t,
        proto::Reply {
            id,
            connect: Some(proto::ConnectResult {
                client: "c".into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    assert_eq!(h.core.state(), State::Connected);
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Eof,
    });
    h.advance(Duration::from_secs(30));
    assert_eq!(
        h.last_open().kind,
        TransportKind::HttpStream,
        "a proven transport is kept"
    );
}

#[test]
fn backoff_resets_on_first_ping_not_on_connect_reply() {
    let mut h = H::json();
    let t = h.connect();
    assert!(h.core.conn.reset_pending);
    h.reply(t, proto::Reply::default());
    assert!(!h.core.conn.reset_pending);
    assert_eq!(h.core.conn.reconnect_attempts, 0);
}

// ───────────────────────────── subscriptions ─────────────────────────────

#[test]
fn subscribe_before_connect_goes_out_with_the_connect_step() {
    let mut h = H::json();
    for (k, ch) in [(1, "a"), (2, "b"), (3, "c")] {
        h.new_sub(k, SubConfig::new(ch));
        h.op(Op::Subscribe(k));
    }
    assert_eq!(
        names(&h.take_events()),
        ["subscribing(1,0)", "subscribing(2,0)", "subscribing(3,0)"]
    );
    h.op(Op::Connect);
    let t = h.last_open().transport;
    h.step(Input::Opened { transport: t });
    let id = h.take_commands()[0].id;
    let out = h.core.step(
        Input::Item {
            transport: t,
            reply: Box::new(proto::Reply {
                id,
                connect: Some(proto::ConnectResult::default()),
                ..Default::default()
            }),
        },
        h.now,
    );
    let subscribes = out
        .commands
        .iter()
        .filter(|(_, c)| c.subscribe.is_some())
        .count();
    assert_eq!(
        subscribes, 3,
        "every subscribe in the connect reply's step: one frame (§9.10)"
    );
}

#[test]
fn recovery_delivers_one_per_step_and_commits_on_delivery() {
    let mut h = H::json();
    let mut cfg = SubConfig::new("news");
    cfg.recoverable = true;
    cfg.since = Some(StreamPosition::new(10, "e1"));
    h.new_sub(1, cfg);
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let cmds = h.take_commands();
    let sub = subscribe_cmd(&cmds);
    let req = sub.subscribe.as_ref().unwrap();
    assert!(req.recover && req.offset == 10 && req.epoch == "e1");
    h.take_events();
    // Without acking deliveries: each recovered publication is its own step.
    let reply = subscribe_ok(
        sub.id,
        proto::SubscribeResult {
            recoverable: true,
            was_recovering: true,
            recovered: true,
            epoch: "e1".into(),
            offset: 10,
            publications: vec![publication(11, b"{}"), publication(12, b"{}")],
            ..Default::default()
        },
    );
    let out = h.core.step(
        Input::Item {
            transport: t,
            reply: Box::new(reply),
        },
        h.now,
    );
    assert_eq!(names(&out.events), ["subscribed(1)"]);
    h.core.step(
        Input::Delivered {
            ok: true,
            panic: None,
        },
        h.now,
    );
    assert!(h.core.has_pending_delivery());
    let out = h.core.step(Input::DeliverNext, h.now);
    assert_eq!(names(&out.events), ["pub(1,11)"]);
    assert_eq!(
        h.core.subs[&1].offset, 10,
        "not committed before the handler returns"
    );
    h.core.step(
        Input::Delivered {
            ok: true,
            panic: None,
        },
        h.now,
    );
    assert_eq!(h.core.subs[&1].offset, 11);
    // unsubscribe() from that handler stops the rest (pattern 21).
    h.core.step(Input::App(Op::Unsubscribe(1)), h.now);
    let out = h.core.step(Input::DeliverNext, h.now);
    assert!(out.events.is_empty());
    assert_eq!(
        h.core.subs[&1].offset, 11,
        "the position is kept for a later subscribe()"
    );
}

#[test]
fn panicking_handler_does_not_commit_and_stops_the_client() {
    let mut h = H::json();
    let mut cfg = SubConfig::new("news");
    cfg.recoverable = true;
    h.new_sub(1, cfg);
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    h.reply(
        t,
        subscribe_ok(
            id,
            proto::SubscribeResult {
                recoverable: true,
                epoch: "e".into(),
                offset: 5,
                ..Default::default()
            },
        ),
    );
    h.take_events();
    let push = proto::Push {
        channel: "news".into(),
        r#pub: Some(publication(6, b"{}")),
        ..Default::default()
    };
    h.step_raw(
        Input::Item {
            transport: t,
            reply: Box::new(proto::Reply {
                push: Some(push),
                ..Default::default()
            }),
        },
        false,
    );
    h.step_raw(
        Input::Delivered {
            ok: false,
            panic: Some("boom".into()),
        },
        true,
    );
    assert_eq!(
        h.core.subs[&1].offset, 5,
        "redelivered by the next recovery"
    );
    let ev = names(&h.take_events());
    assert!(
        ev.contains(&"error(HandlerPanicked)".to_string())
            && ev.last() == Some(&"disconnected(2)".to_string()),
        "{ev:?}"
    );
}

#[test]
fn live_publication_dedupe_and_other_stream_publications() {
    let mut h = H::json();
    let mut cfg = SubConfig::new("news");
    cfg.recoverable = true;
    h.new_sub(1, cfg);
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    h.reply(
        t,
        subscribe_ok(
            id,
            proto::SubscribeResult {
                recoverable: true,
                epoch: "e".into(),
                offset: 5,
                ..Default::default()
            },
        ),
    );
    h.take_events();
    for off in [5, 6, 6] {
        h.push(
            t,
            proto::Push {
                channel: "news".into(),
                r#pub: Some(publication(off, b"{}")),
                ..Default::default()
            },
        );
    }
    assert_eq!(
        names(&h.take_events()),
        ["pub(1,6)"],
        "duplicates of delivered offsets are dropped"
    );
    let mut other = publication(100, b"{}");
    other.channel = "news:other".into();
    h.push(
        t,
        proto::Push {
            channel: "news".into(),
            r#pub: Some(other),
            ..Default::default()
        },
    );
    assert_eq!(names(&h.take_events()), ["pub(1,100)"]);
    assert_eq!(
        h.core.subs[&1].offset, 6,
        "another stream's offset never moves the position"
    );
}

/// Over WebSocket the server handles commands in order: the unsubscribe goes
/// at once, and so does the next subscribe (TLA+ `Ordered`).
#[test]
fn unsubscribe_while_subscribe_in_flight_goes_at_once_in_order() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let first = subscribe_cmd(&h.take_commands()).id;
    h.op(Op::Unsubscribe(1));
    let cmds = h.take_commands();
    assert!(cmds[0].unsubscribe.is_some(), "at once");
    h.op(Op::Subscribe(1));
    let second = subscribe_cmd(&h.take_commands()).id;
    // The first subscribe's reply is stale.
    h.reply(t, subscribe_ok(first, proto::SubscribeResult::default()));
    assert_eq!(h.core.subs[&1].state, SubState::Subscribing);
    h.reply(t, subscribe_ok(second, proto::SubscribeResult::default()));
    assert_eq!(h.core.subs[&1].state, SubState::Subscribed);
}

#[test]
fn unsubscribe_while_subscribe_in_flight_is_deferred_and_next_subscribe_waits() {
    let mut h = H::emulation();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let first = subscribe_cmd(&h.take_commands()).id;
    h.op(Op::Unsubscribe(1));
    assert!(
        h.take_commands().is_empty(),
        "deferred until the subscribe reply (bug_no_unsub_defer)"
    );
    h.op(Op::Subscribe(1));
    assert!(
        h.take_commands().is_empty(),
        "a new subscribe of the channel waits"
    );
    h.reply(t, subscribe_ok(first, proto::SubscribeResult::default()));
    let cmds = h.take_commands();
    assert_eq!(cmds.len(), 1);
    let unsub = cmds[0]
        .unsubscribe
        .as_ref()
        .expect("the deferred unsubscribe");
    assert_eq!(unsub.channel, "ch");
    h.reply(
        t,
        proto::Reply {
            id: cmds[0].id,
            unsubscribe: Some(proto::UnsubscribeResult {}),
            ..Default::default()
        },
    );
    let cmds = h.take_commands();
    assert!(
        cmds[0].subscribe.is_some(),
        "subscribes once the unsubscribe was answered"
    );
    assert_eq!(h.core.subs[&1].state, SubState::Subscribing);
}

/// Over emulation: a write failure of the subscribe whose unsubscribe is
/// deferred clears the deferral (TLA+ `Env_WriteError`: nothing to undo), so a
/// later subscribe of the channel isn't stuck.
#[test]
fn write_failure_of_a_deferred_subscribe_clears_the_deferral() {
    let mut h = H::emulation();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let first = subscribe_cmd(&h.take_commands()).id;
    h.op(Op::Unsubscribe(1));
    assert!(h.take_commands().is_empty(), "deferred");
    h.step(Input::WriteFailed {
        transport: t,
        command_ids: vec![first],
        had_pong: false,
    });
    h.op(Op::Subscribe(1));
    let cmds = h.take_commands();
    assert!(
        cmds.iter().any(|c| c.subscribe.is_some()),
        "the channel isn't left waiting for the failed subscribe: {cmds:?}"
    );
    assert!(
        cmds.iter().all(|c| c.unsubscribe.is_none()),
        "nothing to undo"
    );
}

#[test]
fn replace_subscription_waits_for_old_unsubscribe() {
    let mut h = H::emulation();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    h.reply(t, subscribe_ok(id, proto::SubscribeResult::default()));
    h.take_commands();
    h.op(Op::RemoveSubscription(1));
    let unsub = h.take_commands();
    assert!(unsub[0].unsubscribe.is_some());
    h.new_sub(2, SubConfig::new("ch"));
    h.op(Op::Subscribe(2));
    assert!(
        h.take_commands().is_empty(),
        "no 105: waits for the removed object's unsubscribe (App_Replace)"
    );
    h.reply(
        t,
        proto::Reply {
            id: unsub[0].id,
            unsubscribe: Some(proto::UnsubscribeResult {}),
            ..Default::default()
        },
    );
    assert!(h.take_commands()[0].subscribe.is_some());
}

#[test]
fn outdated_subscribe_reply_is_ignored_and_starts_nothing() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    // A reconnect while the reply is in flight.
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Eof,
    });
    h.reply(t, subscribe_ok(id, proto::SubscribeResult::default()));
    assert_eq!(h.core.subs[&1].state, SubState::Subscribing);
}

#[test]
fn teardown_events_order_and_resubscribe_with_position() {
    let mut h = H::json();
    let mut cfg = SubConfig::new("news");
    cfg.recoverable = true;
    h.new_sub(1, cfg);
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    h.reply(
        t,
        subscribe_ok(
            id,
            proto::SubscribeResult {
                recoverable: true,
                epoch: "e".into(),
                offset: 7,
                ..Default::default()
            },
        ),
    );
    h.take_events();
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Eof,
    });
    assert_eq!(
        names(&h.take_events()),
        ["subscribing(1,1)", "connecting(1)"],
        "subscription events first (§6.1)"
    );
    h.advance(Duration::from_secs(2));
    let t2 = h.last_open().transport;
    h.step(Input::Opened { transport: t2 });
    let id = h.take_commands()[0].id;
    h.reply(
        t2,
        proto::Reply {
            id,
            connect: Some(proto::ConnectResult::default()),
            ..Default::default()
        },
    );
    let cmds = h.take_commands();
    let req = subscribe_cmd(&cmds).subscribe.as_ref().unwrap();
    assert!(req.recover && req.offset == 7 && req.epoch == "e");
}

#[test]
fn resubscribe_delay_survives_a_reconnect() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    h.reply(
        t,
        proto::Reply {
            id,
            error: Some(proto::Error {
                code: 100,
                message: "internal".into(),
                temporary: true,
            }),
            ..Default::default()
        },
    );
    assert!(h.core.timers.is_armed(TimerKey::Resubscribe(1)));
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Eof,
    });
    h.advance(Duration::from_millis(1));
    assert!(
        h.core.timers.is_armed(TimerKey::Resubscribe(1)),
        "kept through the teardown"
    );
}

#[test]
fn subscribe_error_codes() {
    let run = |err: proto::Error, has_get_token: bool| {
        let mut h = H::json();
        let mut cfg = SubConfig::new("ch");
        cfg.has_get_token = has_get_token;
        cfg.token = "t".into();
        h.new_sub(1, cfg);
        h.op(Op::Subscribe(1));
        let t = h.connect();
        let id = subscribe_cmd(&h.take_commands()).id;
        h.take_events();
        h.reply(
            t,
            proto::Reply {
                id,
                error: Some(err),
                ..Default::default()
            },
        );
        names(&h.take_events())
    };
    let e = |code, temporary| proto::Error {
        code,
        message: "x".into(),
        temporary,
    };
    assert_eq!(
        run(e(103, false), false),
        ["unsubscribed(1,103)"],
        "permanent: unsubscribed only"
    );
    assert_eq!(run(e(100, true), false), ["sub_error(1,Subscribe)"]);
    assert_eq!(run(e(109, false), true), ["sub_error(1,Subscribe)"]);
    assert_eq!(
        run(e(109, false), false),
        ["sub_error(1,Configuration)", "unsubscribed(1,1)"]
    );
}

#[test]
fn get_state_runs_without_position_and_again_after_112() {
    let mut h = H::json();
    let mut cfg = SubConfig::new("orders");
    cfg.has_get_state = true;
    cfg.recoverable = true;
    h.new_sub(1, cfg);
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let Some(Callback::GetState { flow, .. }) = h.callbacks.pop() else {
        panic!("get_state first")
    };
    h.step(Input::StateSettled {
        flow,
        result: Ok(StreamPosition::new(3, "e")),
    });
    let cmds = h.take_commands();
    let sub = subscribe_cmd(&cmds);
    let req = sub.subscribe.as_ref().unwrap();
    assert_eq!(
        (req.recover, req.offset, req.flag & 2),
        (true, 3, 2),
        "asks the server to reject unrecoverable positions"
    );
    h.reply(
        t,
        proto::Reply {
            id: sub.id,
            error: Some(proto::Error {
                code: 112,
                message: "unrecoverable".into(),
                temporary: false,
            }),
            ..Default::default()
        },
    );
    h.advance(Duration::from_secs(2));
    assert!(
        matches!(h.callbacks.pop(), Some(Callback::GetState { .. })),
        "position reset: get_state again"
    );
}

#[test]
fn state_invalidation_push_resubscribes_with_sentinel() {
    let mut h = H::json();
    let mut cfg = SubConfig::new("news");
    cfg.recoverable = true;
    h.new_sub(1, cfg);
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    h.reply(
        t,
        subscribe_ok(
            id,
            proto::SubscribeResult {
                recoverable: true,
                epoch: "e".into(),
                offset: 9,
                ..Default::default()
            },
        ),
    );
    h.take_events();
    h.push(
        t,
        proto::Push {
            channel: "news".into(),
            unsubscribe: Some(proto::Unsubscribe {
                code: 2502,
                reason: "invalidated".into(),
            }),
            ..Default::default()
        },
    );
    assert_eq!(names(&h.take_events()), ["subscribing(1,2502)"]);
    let cmds = h.take_commands();
    let req = subscribe_cmd(&cmds).subscribe.as_ref().unwrap();
    assert_eq!(
        (req.recover, req.offset, req.epoch.as_str()),
        (true, 0, "_")
    );
}

#[test]
fn unsubscribe_push_ignored_unless_subscribed() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    h.take_events();
    h.push(
        t,
        proto::Push {
            channel: "ch".into(),
            unsubscribe: Some(proto::Unsubscribe {
                code: 2000,
                reason: "server".into(),
            }),
            ..Default::default()
        },
    );
    assert!(
        h.take_events().is_empty(),
        "belongs to a previous subscription (unsub_push_any_state)"
    );
}

#[test]
fn unsubscribe_failure_reconnects_only_within_its_session() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let id = subscribe_cmd(&h.take_commands()).id;
    h.reply(t, subscribe_ok(id, proto::SubscribeResult::default()));
    h.op(Op::Unsubscribe(1));
    let unsub = h.take_commands()[0].id;
    h.take_events();
    h.op(Op::Disconnect);
    h.take_events();
    h.reply(
        t,
        proto::Reply {
            id: unsub,
            error: Some(proto::Error {
                code: 100,
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    assert_eq!(
        h.core.state(),
        State::Disconnected,
        "a teardown-caused failure never reconnects (centrifuge-js #402)"
    );
}

#[test]
fn subscribe_timeout_reconnects() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("ch"));
    h.op(Op::Subscribe(1));
    h.connect();
    h.take_events();
    h.advance(Duration::from_secs(6));
    assert!(names(&h.take_events()).contains(&"connecting(3)".to_string()));
}

#[test]
fn compaction_routes_by_id_and_drops_unknown_ids() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("news"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let cmds = h.take_commands();
    let sub = subscribe_cmd(&cmds);
    assert_eq!(
        sub.subscribe.as_ref().unwrap().flag & 1,
        1,
        "compaction offered"
    );
    h.reply(
        t,
        subscribe_ok(
            sub.id,
            proto::SubscribeResult {
                id: 42,
                ..Default::default()
            },
        ),
    );
    h.take_events();
    h.push(
        t,
        proto::Push {
            id: 42,
            r#pub: Some(publication(0, b"{}")),
            ..Default::default()
        },
    );
    h.push(
        t,
        proto::Push {
            id: 7,
            r#pub: Some(publication(0, b"{}")),
            ..Default::default()
        },
    );
    assert_eq!(names(&h.take_events()), ["pub(1,0)"]);
}

/// An unsubscribe push (code >= 2500) retires the subscription's compaction
/// id: after the resubscribe gets a new id, pushes with the old one are dropped.
#[test]
fn resubscribe_push_retires_the_old_compaction_id() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("news"));
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let cmds = h.take_commands();
    let sub = subscribe_cmd(&cmds);
    h.reply(
        t,
        subscribe_ok(
            sub.id,
            proto::SubscribeResult {
                id: 42,
                ..Default::default()
            },
        ),
    );
    h.push(
        t,
        proto::Push {
            channel: "news".into(),
            unsubscribe: Some(proto::Unsubscribe {
                code: 2501,
                reason: "resubscribe".into(),
            }),
            ..Default::default()
        },
    );
    h.advance(Duration::from_secs(3));
    let cmds = h.take_commands();
    let sub = subscribe_cmd(&cmds);
    h.reply(
        t,
        subscribe_ok(
            sub.id,
            proto::SubscribeResult {
                id: 43,
                ..Default::default()
            },
        ),
    );
    h.take_events();
    for id in [42, 43] {
        h.push(
            t,
            proto::Push {
                id,
                r#pub: Some(publication(0, b"{}")),
                ..Default::default()
            },
        );
    }
    assert_eq!(names(&h.take_events()), ["pub(1,0)"]);
}

#[test]
fn delta_fossil_applies_and_broken_chain_stops() {
    let base = b"{\"text\":\"hello world, this is the base payload\"}".to_vec();
    let mut h = H::new(CoreConfig {
        protocol: Protocol::Protobuf,
        ..CoreConfig::default()
    });
    let mut cfg = SubConfig::new("d");
    cfg.delta = true;
    h.new_sub(1, cfg);
    h.op(Op::Subscribe(1));
    let t = h.connect();
    let cmds = h.take_commands();
    let sub = subscribe_cmd(&cmds);
    assert_eq!(sub.subscribe.as_ref().unwrap().delta, "fossil");
    h.reply(
        t,
        subscribe_ok(
            sub.id,
            proto::SubscribeResult {
                delta: true,
                ..Default::default()
            },
        ),
    );
    h.take_events();
    let full = proto::Publication {
        data: Bytes::from(base.clone()),
        ..Default::default()
    };
    h.push(
        t,
        proto::Push {
            channel: "d".into(),
            r#pub: Some(full),
            ..Default::default()
        },
    );
    // An identity delta: copy the whole base.
    let n = base.len();
    let enc = |v: usize| -> String {
        const A: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";
        if v == 0 {
            return "0".into();
        }
        let mut s = vec![];
        let mut v = v;
        while v > 0 {
            s.push(A[v & 63]);
            v >>= 6;
        }
        s.reverse();
        String::from_utf8(s).unwrap()
    };
    let sum = {
        let mut sum: u32 = 0;
        for c in base.chunks(4) {
            let mut w = [0u8; 4];
            w[..c.len()].copy_from_slice(c);
            sum = sum.wrapping_add(u32::from_be_bytes(w));
        }
        sum as usize
    };
    let d = format!("{}\n{}@0,{};", enc(n), enc(n), enc(sum));
    let dp = proto::Publication {
        data: Bytes::from(d),
        delta: true,
        ..Default::default()
    };
    h.push(
        t,
        proto::Push {
            channel: "d".into(),
            r#pub: Some(dp),
            ..Default::default()
        },
    );
    let ev = h.take_events();
    assert_eq!(ev.len(), 2);
    let Event::Publication { publication, .. } = &ev[1] else {
        panic!()
    };
    assert_eq!(
        &publication.data[..],
        &base[..],
        "delta applied to the base"
    );
    let bad = proto::Publication {
        data: Bytes::from_static(b"zz\n"),
        delta: true,
        offset: 3,
        ..Default::default()
    };
    h.push(
        t,
        proto::Push {
            channel: "d".into(),
            r#pub: Some(bad),
            ..Default::default()
        },
    );
    // Terminal, and the subscription keeps its place as subscribing (TLA+ teardown).
    assert_eq!(
        names(&h.take_events()),
        [
            "error(Delta { channel: \"d\", offset: 3 })",
            "subscribing(1,1)",
            "disconnected(2)"
        ]
    );
}

// ───────────────────────────── server-side subscriptions ─────────────────────────────

#[test]
fn server_subs_announced_per_step_with_recovered_publications() {
    let mut h = H::json();
    let mut subs = std::collections::HashMap::new();
    subs.insert(
        "a".to_string(),
        proto::SubscribeResult {
            recoverable: true,
            epoch: "e".into(),
            offset: 1,
            publications: vec![publication(2, b"{}")],
            ..Default::default()
        },
    );
    subs.insert("b".to_string(), proto::SubscribeResult::default());
    h.connect_with(proto::ConnectResult {
        subs,
        ..Default::default()
    });
    assert_eq!(
        names(&h.take_events()),
        [
            "connecting(0)",
            "connected",
            "server_subscribed(a)",
            "server_pub(a,2)",
            "server_subscribed(b)"
        ]
    );
    assert_eq!(h.core.server_subs["a"].offset, 2);
}

#[test]
fn server_sub_unsubscribed_by_any_code_and_recovered_on_reconnect() {
    let mut h = H::json();
    let mut subs = std::collections::HashMap::new();
    subs.insert(
        "a".to_string(),
        proto::SubscribeResult {
            recoverable: true,
            epoch: "e".into(),
            offset: 4,
            ..Default::default()
        },
    );
    let t = h.connect_with(proto::ConnectResult {
        subs,
        ..Default::default()
    });
    h.take_events();
    h.step(Input::Closed {
        transport: t,
        reason: CloseReason::Eof,
    });
    assert_eq!(
        names(&h.take_events()),
        ["server_subscribing(a)", "connecting(1)"]
    );
    h.advance(Duration::from_secs(2));
    let t2 = h.last_open().transport;
    h.step(Input::Opened { transport: t2 });
    let cmds = h.take_commands();
    let req = &cmds[0].connect.as_ref().unwrap().subs["a"];
    assert!(req.recover && req.offset == 4 && req.epoch == "e");
    h.reply(
        t2,
        proto::Reply {
            id: cmds[0].id,
            connect: Some(proto::ConnectResult {
                subs: [("a".to_string(), proto::SubscribeResult::default())].into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    h.take_events();
    h.push(
        t2,
        proto::Push {
            channel: "a".into(),
            unsubscribe: Some(proto::Unsubscribe {
                code: 2500,
                reason: "x".into(),
            }),
            ..Default::default()
        },
    );
    assert_eq!(names(&h.take_events()), ["server_unsubscribed(a)"]);
}

#[test]
fn close_unsubscribes_everything_before_disconnected() {
    let mut h = H::json();
    h.new_sub(1, SubConfig::new("x"));
    h.op(Op::Subscribe(1));
    h.connect();
    h.take_events();
    h.op(Op::Close);
    assert_eq!(
        names(&h.take_events()),
        ["unsubscribed(1,2)", "disconnected(0)"]
    );
    assert_eq!(h.core.state(), State::Closed);
    h.op(Op::Connect);
    assert!(h.take_events().is_empty(), "closed is final");
}

#[test]
fn connection_refresh_flow() {
    let mut h = H::new(CoreConfig {
        has_get_token: true,
        token: "t0".into(),
        ..CoreConfig::default()
    });
    let t = h.connect_with(proto::ConnectResult {
        expires: true,
        ttl: 10,
        ..Default::default()
    });
    h.take_events();
    h.advance(Duration::from_secs(10));
    let Some(Callback::ConnectionToken { flow }) = h.callbacks.pop() else {
        panic!("refresh getter")
    };
    h.step(Input::TokenSettled {
        flow,
        result: Ok(Some("t1".into())),
    });
    let cmds = h.take_commands();
    let r = cmds.iter().find(|c| c.refresh.is_some()).unwrap();
    assert_eq!(r.refresh.as_ref().unwrap().token, "t1");
    h.reply(
        t,
        proto::Reply {
            id: r.id,
            refresh: Some(proto::RefreshResult {
                expires: true,
                ttl: 20,
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    assert_eq!(h.core.conn.token, "t1");
}

#[test]
fn refresh_push_only_adjusts_client_side_refresh() {
    let mut h = H::new(CoreConfig {
        has_get_token: true,
        token: "t".into(),
        ..CoreConfig::default()
    });
    let t = h.connect_with(proto::ConnectResult {
        expires: false,
        ..Default::default()
    });
    h.push(
        t,
        proto::Push {
            refresh: Some(proto::Refresh {
                expires: true,
                ttl: 5,
            }),
            ..Default::default()
        },
    );
    assert!(
        !h.core.timers.is_armed(TimerKey::Refresh),
        "server-side refresh mode: ignored"
    );
    let mut h = H::new(CoreConfig {
        has_get_token: true,
        token: "t".into(),
        ..CoreConfig::default()
    });
    let t = h.connect_with(proto::ConnectResult {
        expires: true,
        ttl: 100,
        ..Default::default()
    });
    h.push(
        t,
        proto::Push {
            refresh: Some(proto::Refresh {
                expires: false,
                ttl: 0,
            }),
            ..Default::default()
        },
    );
    assert!(
        !h.core.timers.is_armed(TimerKey::Refresh),
        "no longer expires: cancelled"
    );
}

/// Jitter with `Duration::MAX` bounds (a direct core user without the
/// client's 24 h cap) saturates instead of overflowing.
#[test]
fn backoff_jitter_with_huge_bounds_does_not_overflow() {
    let mut r = Rng::new(1);
    for _ in 0..100 {
        let _ = r.between(Duration::ZERO, Duration::MAX);
        let _ = r.between(Duration::from_secs(1), Duration::MAX);
    }
    let _ = super::timers::backoff(&mut r, 1000, Duration::from_millis(500), Duration::MAX);
    for attempt in [0, 1, 1000] {
        let _ = super::timers::backoff(&mut r, attempt, Duration::MAX, Duration::MAX);
    }
}

fn connecting_reason(events: &[Event]) -> Option<(u32, String)> {
    events.iter().find_map(|e| match e {
        Event::Connecting { code, reason } => Some((*code, reason.clone())),
        _ => None,
    })
}

/// `offline` closes the transport (code 1); `online` reconnects at once
/// instead of waiting out the backoff (docs/wasm-plan.md, step 7a).
#[test]
fn offline_closes_and_online_reconnects_at_once() {
    let mut h = H::json();
    let t = h.connect();
    h.take_events();
    h.step(Input::Lifecycle(Lifecycle::Offline));
    assert_eq!(h.core.state(), State::Connecting);
    assert_eq!(
        connecting_reason(&h.take_events()),
        Some((1, "transport closed".into()))
    );
    assert!(h.closes.contains(&t));
    let opens = h.opens.len();
    h.step(Input::Lifecycle(Lifecycle::Online));
    assert_eq!(h.opens.len(), opens + 1, "a new attempt at once");
    assert!(h.core.timers.deadline(TimerKey::Reconnect).is_none());
}

/// While disconnected, lifecycle events do nothing; `online` doesn't start a
/// second attempt while one is in progress.
#[test]
fn lifecycle_events_respect_the_state() {
    let mut h = H::json();
    for e in [
        Lifecycle::Offline,
        Lifecycle::Online,
        Lifecycle::PageHidden,
        Lifecycle::PageShown,
    ] {
        h.step(Input::Lifecycle(e));
    }
    assert_eq!(h.core.state(), State::Disconnected);
    assert!(h.opens.is_empty() && h.take_events().is_empty());

    // Connecting, the transport opening: `online` without a prior `offline`
    // leaves it alone.
    h.op(Op::Connect);
    let opens = h.opens.len();
    h.step(Input::Lifecycle(Lifecycle::Online));
    assert_eq!(h.opens.len(), opens);
    assert_eq!(h.closes, Vec::<TransportId>::new());
}

/// An attempt opened while offline is replaced on `online`: mobile Safari
/// may never report it closed.
#[test]
fn online_replaces_a_transport_opened_while_offline() {
    let mut h = H::json();
    h.connect();
    h.step(Input::Lifecycle(Lifecycle::Offline));
    // The backoff runs out while still offline: an attempt opens.
    h.advance(Duration::from_secs(30));
    let offline_attempt = h.last_open().transport;
    let opens = h.opens.len();
    h.step(Input::Lifecycle(Lifecycle::Online));
    assert!(h.closes.contains(&offline_attempt));
    assert_eq!(h.opens.len(), opens + 1);
    assert_ne!(h.last_open().transport, offline_attempt);
}

/// `pagehide` (persisted) closes with its own reason; `pageshow` reconnects at
/// once, only after a `pagehide`.
#[test]
fn back_forward_cache_closes_and_restores() {
    let mut h = H::json();
    h.connect();
    h.take_events();
    h.step(Input::Lifecycle(Lifecycle::PageShown));
    assert_eq!(h.core.state(), State::Connected, "pageshow alone: nothing");
    h.step(Input::Lifecycle(Lifecycle::PageHidden));
    assert_eq!(
        connecting_reason(&h.take_events()),
        Some((1, "page entered back/forward cache".into()))
    );
    let opens = h.opens.len();
    h.step(Input::Lifecycle(Lifecycle::PageShown));
    assert_eq!(h.opens.len(), opens + 1, "reconnects at once");
}

/// The app's own `disconnect()` ends what `offline` started: a later
/// `online` doesn't reconnect a client the app disconnected.
#[test]
fn disconnect_clears_the_lifecycle_flags() {
    let mut h = H::json();
    h.connect();
    h.step(Input::Lifecycle(Lifecycle::Offline));
    h.op(Op::Disconnect);
    let opens = h.opens.len();
    h.step(Input::Lifecycle(Lifecycle::Online));
    assert_eq!(h.core.state(), State::Disconnected);
    assert_eq!(h.opens.len(), opens);
}
