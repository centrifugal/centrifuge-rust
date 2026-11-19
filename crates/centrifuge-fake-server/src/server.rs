//! Connections and the default command handling.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use centrifuge_client_core::proto;
use futures_util::{SinkExt as _, StreamExt as _};
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::body::{Frame, Incoming};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role};

use crate::codec::{self, Protocol};
use crate::{
    Action, Channel, Conn, EMULATION_PATH, HTTP_STREAM_PATH, Inner, Out, Received, St, SubInfo,
    Transport, WS_PATH,
};

type Body = BoxBody<Bytes, std::io::Error>;

fn full(status: StatusCode, body: impl Into<Bytes>) -> Response<Body> {
    let mut r = Response::new(
        Full::new(body.into())
            .map_err(|e: Infallible| match e {})
            .boxed(),
    );
    *r.status_mut() = status;
    r
}

pub(crate) async fn accept(listener: TcpListener, inner: Weak<Inner>) {
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            continue;
        };
        let inner = inner.clone();
        tokio::spawn(async move {
            let svc = hyper::service::service_fn(move |req| {
                let inner = inner.clone();
                async move {
                    let Some(inner) = inner.upgrade() else {
                        return Ok::<_, Infallible>(full(StatusCode::SERVICE_UNAVAILABLE, ""));
                    };
                    Ok(route(inner, req).await)
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(tcp), svc)
                .with_upgrades()
                .await;
        });
    }
}

async fn route(inner: Arc<Inner>, req: Request<Incoming>) -> Response<Body> {
    inner
        .st()
        .requests
        .push((req.uri().path().to_owned(), req.headers().clone()));
    match req.uri().path() {
        WS_PATH => websocket(inner, req).await,
        HTTP_STREAM_PATH => http_stream(inner, req).await,
        EMULATION_PATH => emulation(inner, req).await,
        _ => full(StatusCode::NOT_FOUND, "not found"),
    }
}

fn register(
    inner: &Inner,
    transport: Transport,
    protocol: Protocol,
) -> (u64, mpsc::UnboundedReceiver<Out>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut st = inner.st();
    let id = st.next_conn;
    st.next_conn += 1;
    st.conns.insert(
        id,
        Conn {
            transport,
            protocol,
            tx,
            connected: false,
            client: String::new(),
            user: String::new(),
            session: format!("session-{id}"),
            subs: std::collections::HashMap::new(),
        },
    );
    drop(st);
    inner.bump();
    (id, rx)
}

fn unregister(inner: &Inner, id: u64) {
    inner.st().conns.remove(&id);
    inner.bump();
}

/// Removes the connection when its task ends, however it ends.
struct ConnGuard(Arc<Inner>, u64);

impl Drop for ConnGuard {
    fn drop(&mut self) {
        unregister(&self.0, self.1);
    }
}

fn stale_watch(inner: &Arc<Inner>, id: u64) {
    let Some(d) = inner.st().faults.stale_close else {
        return;
    };
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        tokio::time::sleep(d).await;
        let Some(inner) = weak.upgrade() else { return };
        let st = inner.st();
        if let Some(c) = st.conns.get(&id).filter(|c| !c.connected) {
            let _ = c.tx.send(Out::Close(3501, "stale".into()));
        }
    });
}

// ---- WebSocket ----

async fn websocket(inner: Arc<Inner>, mut req: Request<Incoming>) -> Response<Body> {
    let (refuse, hang) = {
        let st = inner.st();
        (st.faults.refuse_upgrade, st.faults.hang_upgrade)
    };
    if hang {
        std::future::pending::<()>().await;
    }
    if let Some(status) = refuse {
        return full(
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST),
            "refused",
        );
    }
    let Some(key) = req.headers().get("sec-websocket-key").cloned() else {
        return full(StatusCode::BAD_REQUEST, "not a WebSocket upgrade");
    };
    let offered: Vec<String> = req
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').map(|p| p.trim().to_owned()).collect())
        .unwrap_or_default();
    let protobuf = offered.iter().any(|p| p == "centrifuge-protobuf");
    let protocol = if protobuf {
        Protocol::Protobuf
    } else {
        Protocol::Json
    };
    let upgrade = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        let Ok(upgraded) = upgrade.await else { return };
        let ws = WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None).await;
        ws_conn(inner, ws, protocol).await;
    });
    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    let mut resp = full(StatusCode::SWITCHING_PROTOCOLS, "");
    let h = resp.headers_mut();
    h.insert("upgrade", "websocket".parse().expect("static header"));
    h.insert("connection", "Upgrade".parse().expect("static header"));
    h.insert(
        "sec-websocket-accept",
        accept.parse().expect("base64 is a valid header"),
    );
    let selected = if protobuf {
        Some("centrifuge-protobuf")
    } else {
        offered.first().map(String::as_str)
    };
    if let Some(p) = selected {
        h.insert(
            "sec-websocket-protocol",
            p.parse().expect("a valid subprotocol token"),
        );
    }
    resp
}

async fn ws_conn(
    inner: Arc<Inner>,
    ws: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    protocol: Protocol,
) {
    let (id, mut rx) = register(&inner, Transport::WebSocket, protocol);
    let _guard = ConnGuard(inner.clone(), id);
    stale_watch(&inner, id);
    let (mut sink, mut stream) = ws.split();
    loop {
        tokio::select! {
            msg = stream.next() => {
                let data: Bytes = match msg {
                    Some(Ok(Message::Binary(b))) => b,
                    Some(Ok(Message::Text(t))) => Bytes::from(t),
                    Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                    Some(Ok(_)) => continue,
                };
                let pause = inner.st().faults.pause_reads;
                match codec::decode_commands(protocol, &data) {
                    Ok(cmds) => {
                        for cmd in cmds {
                            for out in handle(&inner, id, cmd) {
                                // Through the queue, so writes stay in order.
                                let st = inner.st();
                                if let Some(c) = st.conns.get(&id) {
                                    let _ = c.tx.send(out);
                                }
                            }
                        }
                    }
                    Err(_) => {
                        let _ = sink.send(close_msg(3501, "bad request")).await;
                        return;
                    }
                }
                if let Some(p) = pause {
                    tokio::time::sleep(p).await;
                }
            }
            out = rx.recv() => {
                let Some(out) = out else { return };
                match out {
                    Out::Replies(r) => {
                        let frame = codec::encode_frame(protocol, &r);
                        if sink.send(ws_msg(protocol, frame)).await.is_err() {
                            return;
                        }
                    }
                    Out::Raw(b) => {
                        let msg = if protocol == Protocol::Json {
                            match String::from_utf8(b.to_vec()) {
                                Ok(s) => Message::Text(s.into()),
                                Err(_) => Message::Binary(b),
                            }
                        } else {
                            Message::Binary(b)
                        };
                        if sink.send(msg).await.is_err() {
                            return;
                        }
                    }
                    Out::Close(code, reason) => {
                        let code = if code == 0 { 1000 } else { code };
                        let _ = sink.send(close_msg(code, &reason)).await;
                        // Let the close handshake finish briefly, then drop.
                        let _ = tokio::time::timeout(Duration::from_secs(1), async {
                            while let Some(Ok(_)) = stream.next().await {}
                        })
                        .await;
                        return;
                    }
                    Out::Terminate => return,
                }
            }
        }
    }
}

fn close_msg(code: u32, reason: &str) -> Message {
    Message::Close(Some(CloseFrame {
        code: CloseCode::from(u16::try_from(code).unwrap_or(3000)),
        reason: reason.to_owned().into(),
    }))
}

fn ws_msg(protocol: Protocol, frame: Vec<u8>) -> Message {
    if protocol == Protocol::Json {
        match String::from_utf8(frame) {
            Ok(s) => Message::Text(s.into()),
            Err(e) => Message::Binary(Bytes::from(e.into_bytes())),
        }
    } else {
        Message::Binary(Bytes::from(frame))
    }
}

// ---- HTTP-streaming ----

fn body_protocol(req: &Request<Incoming>) -> Protocol {
    let ct = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if ct.starts_with("application/octet-stream") {
        Protocol::Protobuf
    } else {
        Protocol::Json
    }
}

#[allow(clippy::too_many_lines)]
async fn http_stream(inner: Arc<Inner>, req: Request<Incoming>) -> Response<Body> {
    let (refuse, hang, html, split) = {
        let st = inner.st();
        (
            st.faults.refuse_stream,
            st.faults.hang_stream,
            st.faults.html_stream,
            st.faults.split_writes,
        )
    };
    if hang {
        std::future::pending::<()>().await;
    }
    if let Some(status) = refuse {
        return full(
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST),
            "refused",
        );
    }
    if html {
        let mut r = full(
            StatusCode::OK,
            "<html><body>Sign in to the network</body></html>",
        );
        r.headers_mut().insert(
            "content-type",
            "text/html; charset=utf-8".parse().expect("static header"),
        );
        return r;
    }
    let protocol = body_protocol(&req);
    let Ok(body) = req
        .into_body()
        .collect()
        .await
        .map(http_body_util::Collected::to_bytes)
    else {
        return full(StatusCode::BAD_REQUEST, "body");
    };
    let Ok(cmds) = codec::decode_commands(protocol, &body) else {
        return full(StatusCode::BAD_REQUEST, "bad request");
    };
    let (id, rx) = register(&inner, Transport::HttpStream, protocol);
    let guard = ConnGuard(inner.clone(), id);
    for cmd in cmds {
        for out in handle(&inner, id, cmd) {
            let st = inner.st();
            if let Some(c) = st.conns.get(&id) {
                let _ = c.tx.send(out);
            }
        }
    }
    struct S {
        rx: mpsc::UnboundedReceiver<Out>,
        buf: VecDeque<Bytes>,
        split: Option<usize>,
        protocol: Protocol,
        done: bool,
        _guard: ConnGuard,
    }
    let state = S {
        rx,
        buf: VecDeque::new(),
        split,
        protocol,
        done: false,
        _guard: guard,
    };
    let stream = futures_util::stream::unfold(state, |mut s| async move {
        loop {
            if let Some(b) = s.buf.pop_front() {
                return Some((Ok(Frame::data(b)), s));
            }
            if s.done {
                return None;
            }
            let data = match s.rx.recv().await? {
                Out::Replies(r) => Bytes::from(codec::encode_stream(s.protocol, &r)),
                Out::Raw(b) => b,
                Out::Close(code, reason) => {
                    s.done = true;
                    if code == 0 {
                        continue;
                    }
                    let push = proto::Push {
                        disconnect: Some(proto::Disconnect {
                            code,
                            reason,
                            reconnect: false,
                        }),
                        ..Default::default()
                    };
                    Bytes::from(codec::encode_stream(s.protocol, &[push_reply(push)]))
                }
                Out::Terminate => return Some((Err(std::io::Error::other("terminated")), s)),
            };
            match s.split {
                Some(n) if n > 0 => {
                    let mut d = data;
                    while !d.is_empty() {
                        s.buf.push_back(d.split_to(n.min(d.len())));
                    }
                }
                _ => s.buf.push_back(data),
            }
        }
    });
    let mut resp = Response::new(BodyExt::boxed(StreamBody::new(stream)));
    let ct = if protocol == Protocol::Protobuf {
        "application/octet-stream"
    } else {
        "application/json"
    };
    resp.headers_mut()
        .insert("content-type", ct.parse().expect("static header"));
    stale_watch(&inner, id);
    resp
}

async fn emulation(inner: Arc<Inner>, req: Request<Incoming>) -> Response<Body> {
    let (status, hang) = {
        let st = inner.st();
        (st.faults.emulation_status, st.faults.hang_emulation)
    };
    if hang {
        std::future::pending::<()>().await;
    }
    if let Some(s) = status {
        return full(
            StatusCode::from_u16(s).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            "",
        );
    }
    let protocol = body_protocol(&req);
    let Ok(body) = req
        .into_body()
        .collect()
        .await
        .map(http_body_util::Collected::to_bytes)
    else {
        return full(StatusCode::BAD_REQUEST, "body");
    };
    let Ok((session, _node, cmds)) = codec::decode_emulation(protocol, &body) else {
        return full(StatusCode::BAD_REQUEST, "bad request");
    };
    let id = inner
        .st()
        .conns
        .iter()
        .find(|(_, c)| c.session == session && c.transport == Transport::HttpStream)
        .map(|(id, _)| *id);
    let Some(id) = id else {
        return full(StatusCode::NOT_FOUND, "session not found");
    };
    for cmd in cmds {
        for out in handle(&inner, id, cmd) {
            let st = inner.st();
            if let Some(c) = st.conns.get(&id) {
                let _ = c.tx.send(out);
            }
        }
    }
    full(StatusCode::OK, "")
}

// ---- command handling ----

pub(crate) fn push_reply(push: proto::Push) -> proto::Reply {
    proto::Reply {
        push: Some(push),
        ..Default::default()
    }
}

fn error_reply(id: u32, code: u32, message: &str, temporary: bool) -> proto::Reply {
    proto::Reply {
        id,
        error: Some(proto::Error {
            code,
            message: message.to_owned(),
            temporary,
        }),
        ..Default::default()
    }
}

pub(crate) fn channel<'a>(st: &'a mut St, name: &str) -> &'a mut Channel {
    if !st.channels.contains_key(name) {
        let epoch = format!("e{}", st.next_epoch);
        st.next_epoch += 1;
        st.channels.insert(
            name.to_owned(),
            Channel {
                epoch,
                ..Channel::default()
            },
        );
    }
    st.channels.get_mut(name).expect("inserted above")
}

/// The JSON protocol carries delta-channel payloads as a JSON string.
fn for_conn(c: &Conn, channel: &str, p: &proto::Publication) -> proto::Publication {
    let mut p = p.clone();
    if c.protocol == Protocol::Json && c.subs.get(channel).is_some_and(|s| s.delta) {
        p.data =
            Bytes::from(serde_json::to_vec(&String::from_utf8_lossy(&p.data)).unwrap_or_default());
    }
    p
}

pub(crate) fn broadcast_pub(inner: &Inner, channel: &str, p: &proto::Publication) {
    let st = inner.st();
    for c in st.conns.values().filter(|c| c.subs.contains_key(channel)) {
        let push = proto::Push {
            channel: channel.to_owned(),
            r#pub: Some(for_conn(c, channel, p)),
            ..Default::default()
        };
        let _ = c.tx.send(Out::Replies(vec![push_reply(push)]));
    }
}

pub(crate) fn publish(inner: &Inner, channel_name: &str, mut p: proto::Publication) -> u64 {
    let offset = {
        let mut st = inner.st();
        let size = st.cfg.history_size;
        let ch = channel(&mut st, channel_name);
        ch.offset += 1;
        p.offset = ch.offset;
        ch.pubs.push_back(p.clone());
        while ch.pubs.len() > size {
            ch.pubs.pop_front();
        }
        ch.offset
    };
    broadcast_pub(inner, channel_name, &p);
    offset
}

fn default_subscribe(st: &mut St, req: &proto::SubscribeRequest) -> proto::SubscribeResult {
    let ch = channel(st, &req.channel);
    let positioned = req.positioned || req.recoverable;
    let mut res = proto::SubscribeResult {
        recoverable: req.recoverable,
        positioned,
        delta: req.delta == "fossil",
        ..Default::default()
    };
    if positioned {
        res.epoch = ch.epoch.clone();
        res.offset = ch.offset;
    }
    if req.recover && req.recoverable {
        res.was_recovering = true;
        let first = ch.pubs.front().map_or(ch.offset + 1, |p| p.offset);
        // Recoverable when the epoch matches and nothing after req.offset was
        // trimmed (centrifuge client.go).
        if req.epoch == ch.epoch && req.offset <= ch.offset && req.offset + 1 >= first {
            res.publications = ch
                .pubs
                .iter()
                .filter(|p| p.offset > req.offset)
                .cloned()
                .collect();
            res.recovered = true;
            res.offset = req.offset;
        }
    }
    res
}

fn history(st: &mut St, req: &proto::HistoryRequest) -> proto::HistoryResult {
    let ch = channel(st, &req.channel);
    let mut pubs: Vec<proto::Publication> = ch.pubs.iter().cloned().collect();
    if let Some(since) = &req.since {
        pubs.retain(|p| p.offset > since.offset);
    }
    if req.reverse {
        pubs.reverse();
    }
    match req.limit {
        0 => pubs.clear(),
        n if n > 0 => pubs.truncate(usize::try_from(n).unwrap_or(usize::MAX)),
        _ => {}
    }
    proto::HistoryResult {
        publications: pubs,
        epoch: ch.epoch.clone(),
        offset: ch.offset,
    }
}

/// Handles one command: records it, then the hook or the default.
pub(crate) fn handle(inner: &Arc<Inner>, id: u64, cmd: proto::Command) -> Vec<Out> {
    let (received, hook, hold) = {
        let mut st = inner.st();
        let Some(c) = st.conns.get(&id) else {
            return vec![];
        };
        let r = Received {
            conn: id,
            transport: c.transport,
            protocol: c.protocol,
            command: cmd.clone(),
        };
        st.received.push(r.clone());
        (r, st.on_command.clone(), st.faults.hold_replies)
    };
    inner.bump();
    if hold {
        return vec![];
    }
    if let Some(action) = hook.and_then(|h| h(&received)) {
        return act(inner, id, cmd, action);
    }
    default_handle(inner, id, cmd)
}

fn act(inner: &Arc<Inner>, id: u64, cmd: proto::Command, action: Action) -> Vec<Out> {
    let cid = cmd.id;
    match action {
        Action::Delay(d, then) => {
            let inner = inner.clone();
            tokio::spawn(async move {
                tokio::time::sleep(d).await;
                let outs = match then {
                    Some(a) => act(&inner, id, cmd, *a),
                    None => default_handle(&inner, id, cmd),
                };
                let st = inner.st();
                if let Some(c) = st.conns.get(&id) {
                    for o in outs {
                        let _ = c.tx.send(o);
                    }
                }
            });
            vec![]
        }
        Action::Reply(mut r) => {
            r.id = cid;
            if r.connect.is_some() {
                mark_connected(inner, id, r.connect.as_ref());
            }
            vec![Out::Replies(vec![r])]
        }
        Action::Error {
            code,
            message,
            temporary,
        } => vec![Out::Replies(vec![error_reply(
            cid, code, &message, temporary,
        )])],
        Action::NoReply => vec![],
        Action::Disconnect { code, reason } => vec![Out::Close(code, reason)],
        Action::Frames(frames) => frames.into_iter().map(Out::Replies).collect(),
    }
}

fn mark_connected(inner: &Inner, id: u64, res: Option<&proto::ConnectResult>) {
    let mut st = inner.st();
    // A (delayed) connect reply for a connection already gone counts for nothing.
    if st.conns.contains_key(&id) {
        st.connects += 1;
    }
    if let Some(c) = st.conns.get_mut(&id) {
        c.connected = true;
        if let Some(r) = res {
            c.client.clone_from(&r.client);
            for ch in r.subs.keys() {
                c.subs.insert(ch.clone(), SubInfo { delta: false });
            }
        }
    }
    drop(st);
    inner.bump();
}

#[allow(clippy::too_many_lines)]
fn default_handle(inner: &Arc<Inner>, id: u64, cmd: proto::Command) -> Vec<Out> {
    let cid = cmd.id;
    let reply = |r: proto::Reply| vec![Out::Replies(vec![proto::Reply { id: cid, ..r }])];
    let connected = inner.st().conns.get(&id).is_some_and(|c| c.connected);
    if let Some(req) = &cmd.connect {
        if connected {
            return vec![Out::Close(3501, "bad request".into())];
        }
        let hook = inner.st().on_connect.clone();
        let res = match hook {
            Some(h) => h(req),
            None => Ok(inner.st().cfg.connect_result.clone()),
        };
        let mut res = match res {
            Ok(r) => r,
            Err(e) => {
                return reply(proto::Reply {
                    error: Some(e),
                    ..Default::default()
                });
            }
        };
        if res.client.is_empty() {
            res.client = format!("fake-client-{id}");
        }
        let (transport, session) = {
            let st = inner.st();
            st.conns
                .get(&id)
                .map(|c| (c.transport, c.session.clone()))
                .unwrap_or((Transport::WebSocket, String::new()))
        };
        if transport == Transport::HttpStream {
            res.session = session;
            res.node = "fake-node".into();
        }
        mark_connected(inner, id, Some(&res));
        start_pings(inner, id, res.ping);
        return reply(proto::Reply {
            connect: Some(res),
            ..Default::default()
        });
    }
    if !connected {
        return vec![Out::Close(3501, "bad request".into())];
    }
    if let Some(req) = &cmd.subscribe {
        let already = inner
            .st()
            .conns
            .get(&id)
            .is_some_and(|c| c.subs.contains_key(&req.channel));
        if already {
            return reply(error_reply(cid, 105, "already subscribed", false));
        }
        let hook = inner.st().on_subscribe.clone();
        let res = match hook {
            Some(h) => h(req),
            None => Ok(default_subscribe(&mut inner.st(), req)),
        };
        return match res {
            Ok(res) => {
                let mut st = inner.st();
                if let Some(c) = st.conns.get_mut(&id) {
                    c.subs
                        .insert(req.channel.clone(), SubInfo { delta: res.delta });
                }
                let mut res = res;
                if let Some(c) = st.conns.get(&id) {
                    res.publications = res
                        .publications
                        .iter()
                        .map(|p| for_conn(c, &req.channel, p))
                        .collect();
                }
                drop(st);
                reply(proto::Reply {
                    subscribe: Some(res),
                    ..Default::default()
                })
            }
            Err(e) => reply(proto::Reply {
                error: Some(e),
                ..Default::default()
            }),
        };
    }
    if let Some(req) = &cmd.unsubscribe {
        if let Some(c) = inner.st().conns.get_mut(&id) {
            c.subs.remove(&req.channel);
        }
        return reply(proto::Reply {
            unsubscribe: Some(proto::UnsubscribeResult {}),
            ..Default::default()
        });
    }
    if let Some(req) = &cmd.publish {
        let info = inner.st().conns.get(&id).map(|c| proto::ClientInfo {
            user: c.user.clone(),
            client: c.client.clone(),
            ..Default::default()
        });
        publish(
            inner,
            &req.channel,
            proto::Publication {
                data: req.data.clone(),
                info,
                ..Default::default()
            },
        );
        return reply(proto::Reply {
            publish: Some(proto::PublishResult {}),
            ..Default::default()
        });
    }
    if let Some(req) = &cmd.history {
        let res = history(&mut inner.st(), req);
        return reply(proto::Reply {
            history: Some(res),
            ..Default::default()
        });
    }
    if let Some(req) = &cmd.presence {
        let st = inner.st();
        let presence = st
            .conns
            .values()
            .filter(|c| c.subs.contains_key(&req.channel))
            .map(|c| {
                (
                    c.client.clone(),
                    proto::ClientInfo {
                        user: c.user.clone(),
                        client: c.client.clone(),
                        ..Default::default()
                    },
                )
            })
            .collect();
        return reply(proto::Reply {
            presence: Some(proto::PresenceResult { presence }),
            ..Default::default()
        });
    }
    if let Some(req) = &cmd.presence_stats {
        let st = inner.st();
        let subs: Vec<&Conn> = st
            .conns
            .values()
            .filter(|c| c.subs.contains_key(&req.channel))
            .collect();
        let users: std::collections::HashSet<&str> = subs.iter().map(|c| c.user.as_str()).collect();
        let res = proto::PresenceStatsResult {
            num_clients: u32::try_from(subs.len()).unwrap_or(u32::MAX),
            num_users: u32::try_from(users.len()).unwrap_or(u32::MAX),
        };
        return reply(proto::Reply {
            presence_stats: Some(res),
            ..Default::default()
        });
    }
    if let Some(req) = &cmd.rpc {
        // Echo.
        return reply(proto::Reply {
            rpc: Some(proto::RpcResult {
                data: req.data.clone(),
            }),
            ..Default::default()
        });
    }
    if cmd.send.is_some() {
        return vec![];
    }
    if cmd.refresh.is_some() {
        let client = inner
            .st()
            .conns
            .get(&id)
            .map(|c| c.client.clone())
            .unwrap_or_default();
        return reply(proto::Reply {
            refresh: Some(proto::RefreshResult {
                client,
                version: "fake".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    if cmd.sub_refresh.is_some() {
        return reply(proto::Reply {
            sub_refresh: Some(proto::SubRefreshResult::default()),
            ..Default::default()
        });
    }
    if cmd.ping.is_some() {
        return reply(proto::Reply {
            ping: Some(proto::PingResult {}),
            ..Default::default()
        });
    }
    // An empty command is a pong.
    vec![]
}

fn start_pings(inner: &Arc<Inner>, id: u64, ping: u32) {
    if ping == 0 {
        return;
    }
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        let period = Duration::from_secs(ping.into());
        loop {
            tokio::time::sleep(period).await;
            let Some(inner) = weak.upgrade() else { return };
            let st = inner.st();
            let Some(c) = st.conns.get(&id) else { return };
            let _ = c.tx.send(Out::Replies(vec![proto::Reply::default()]));
        }
    });
}
