//! Tests against a real Centrifugo (docs/spec.md §11.4): `docker compose up`,
//! then `CENTRIFUGO_TESTS=1 cargo test --test real_server`. Skipped without
//! the variable.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)] // test code

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use centrifuge::{
    Client, DeltaType, Filter, HistoryOptions, Protocol, ProxyConfig, State, TokenError,
    TransportEndpoint, TransportKind,
};
use common::{MATRIX, init_tracing, record_client, record_sub, recorder};
use hmac::{Hmac, Mac as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const INSECURE: &str = "localhost:8010";
const SECURE: &str = "localhost:8012";
const API_KEY: &str = "test-api-key";
const SECRET: &[u8] = b"secret";

fn enabled() -> bool {
    init_tracing();
    let on = std::env::var_os("CENTRIFUGO_TESTS").is_some();
    if !on {
        eprintln!("skipped: set CENTRIFUGO_TESTS=1 with `docker compose up` running");
    }
    on
}

fn endpoint(host: &str, kind: TransportKind) -> TransportEndpoint {
    match kind {
        TransportKind::HttpStream => {
            TransportEndpoint::http_stream(format!("http://{host}/connection/http_stream"))
        }
        _ => TransportEndpoint::websocket(format!("ws://{host}/connection/websocket")),
    }
}

fn builder(host: &str, kind: TransportKind, protocol: Protocol) -> centrifuge::ClientBuilder {
    Client::builder_with_transports([endpoint(host, kind)])
        .protocol(protocol)
        .proxy(ProxyConfig::None)
        .min_reconnect_delay(Duration::from_millis(50))
        .max_reconnect_delay(Duration::from_millis(500))
}

fn unique(prefix: &str) -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // The pid too: concurrent test processes share the server, and a coarse
    // clock (microseconds on macOS) can give them the same time.
    format!("{prefix}{}_{n}", std::process::id())
}

fn b64url(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..=chunk.len() {
            out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

/// An HS256 JWT.
fn jwt(claims: &serde_json::Value) -> String {
    let head = b64url(br#"{"alg":"HS256","typ":"JWT"}"#);
    let body = b64url(claims.to_string().as_bytes());
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(SECRET).unwrap();
    mac.update(format!("{head}.{body}").as_bytes());
    format!("{head}.{body}.{}", b64url(&mac.finalize().into_bytes()))
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Server API call (plain HTTP/1.1, no client library needed).
async fn api(host: &str, method: &str, body: serde_json::Value) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(10), api_call(host, method, body))
        .await
        .expect("the server API answers")
}

async fn api_call(host: &str, method: &str, body: serde_json::Value) -> serde_json::Value {
    let body = body.to_string();
    let mut s = tokio::net::TcpStream::connect(host).await.unwrap();
    let req = format!(
        "POST /api/{method} HTTP/1.1\r\nHost: {host}\r\nX-API-Key: {API_KEY}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).await.unwrap();
    let json = resp.split("\r\n\r\n").nth(1).unwrap_or_default();
    serde_json::from_str(json).unwrap_or(serde_json::Value::Null)
}

/// `close()` within 10 s: a hang fails here, with the captured log.
async fn close(client: &Client) {
    tokio::time::timeout(Duration::from_secs(10), client.close())
        .await
        .expect("close() returns");
}

async fn api_publish(channel: &str, data: serde_json::Value) {
    let r = api(
        INSECURE,
        "publish",
        serde_json::json!({ "channel": channel, "data": data }),
    )
    .await;
    assert!(r.get("error").is_none(), "{r}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_basic_matrix() {
    if !enabled() {
        return;
    }
    for (kind, protocol) in MATRIX {
        let ch = unique("recovery:basic");
        let (r, mut ev) = recorder();
        let client = record_client(builder(INSECURE, kind, protocol), &r)
            .build()
            .unwrap();
        let sub = record_sub(client.new_subscription(&ch), &r)
            .build()
            .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        sub.ready(Some(Duration::from_secs(5))).await.unwrap();
        assert_eq!(ev.wait("connected").await, format!("connected {kind:?}"));
        sub.publish(r#"{"n":1}"#).await.unwrap();
        assert_eq!(
            ev.wait("pub").await,
            r#"pub 1 {"n":1}"#,
            "{kind:?} {protocol:?}"
        );
        api_publish(&ch, serde_json::json!({"n": 2})).await;
        assert_eq!(ev.wait("pub").await, r#"pub 2 {"n":2}"#);
        let h = sub.history(HistoryOptions::new().all()).await.unwrap();
        assert_eq!(h.publications.len(), 2);
        assert_eq!(h.offset, 2);
        let p = client.presence(&ch).await;
        // Presence is on only for channels without namespace.
        assert!(p.is_err());
        let top_ch = unique("top");
        let s2 = client.new_subscription(&top_ch).build().unwrap();
        s2.subscribe();
        s2.ready(Some(Duration::from_secs(5))).await.unwrap();
        assert_eq!(s2.presence().await.unwrap().clients.len(), 1);
        assert_eq!(s2.presence_stats().await.unwrap().num_users, 1);
        let rpc = client.rpc("missing", "{}").await;
        assert!(matches!(rpc, Err(centrifuge::Error::Server(_))), "{rpc:?}");
        close(&client).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_recovery_after_disconnect() {
    if !enabled() {
        return;
    }
    for (kind, protocol) in MATRIX {
        let ch = unique("recovery:r");
        let (r, mut ev) = recorder();
        let client = builder(INSECURE, kind, protocol).build().unwrap();
        let sub = record_sub(client.new_subscription(&ch).recoverable(true), &r)
            .build()
            .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        sub.ready(Some(Duration::from_secs(5))).await.unwrap();
        api_publish(&ch, serde_json::json!(1)).await;
        assert_eq!(ev.wait("pub").await, "pub 1 1");
        client.disconnect();
        for i in 2..=4 {
            api_publish(&ch, serde_json::json!(i)).await;
        }
        client.connect().unwrap();
        assert_eq!(
            ev.wait("subscribed").await,
            "subscribed recovered=true pos=Some(1)",
            "{kind:?} {protocol:?}"
        );
        for i in 2..=4 {
            assert_eq!(ev.wait("pub").await, format!("pub {i} {i}"));
        }
        // Too far behind for a 2-entry history: not recovered.
        let small = unique("smallhistory:r");
        let s2 = record_sub(client.new_subscription(&small).recoverable(true), &r)
            .build()
            .unwrap();
        s2.subscribe();
        s2.ready(Some(Duration::from_secs(5))).await.unwrap();
        ev.wait("subscribed").await;
        client.disconnect();
        for i in 1..=5 {
            api_publish(&small, serde_json::json!(i)).await;
        }
        client.connect().unwrap();
        let mut seen = Vec::new();
        loop {
            let e = ev.wait("subscribed").await;
            seen.push(e.clone());
            if e.starts_with("subscribed recovered=false") {
                break;
            }
            assert!(seen.len() < 3, "{seen:?}");
        }
        close(&client).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_delta() {
    if !enabled() {
        return;
    }
    for (kind, protocol) in MATRIX {
        let ch = unique("delta:d");
        let (r, mut ev) = recorder();
        let client = builder(INSECURE, kind, protocol).build().unwrap();
        let sub = record_sub(client.new_subscription(&ch).delta(DeltaType::Fossil), &r)
            .build()
            .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        sub.ready(Some(Duration::from_secs(5))).await.unwrap();
        let big = "x".repeat(2000);
        for i in 0..5 {
            let v = serde_json::json!({ "i": i, "payload": big });
            api(
                INSECURE,
                "publish",
                serde_json::json!({ "channel": ch, "data": v, "delta": true }),
            )
            .await;
            let e = ev.wait("pub").await;
            let data = e.splitn(3, ' ').nth(2).unwrap();
            let got: serde_json::Value = serde_json::from_str(data).unwrap();
            assert_eq!(got, v, "{kind:?} {protocol:?}");
        }
        close(&client).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_tags_filter() {
    if !enabled() {
        return;
    }
    let ch = unique("tagged:t");
    let (r, mut ev) = recorder();
    let client = builder(INSECURE, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    let sub = record_sub(
        client
            .new_subscription(&ch)
            .tags_filter(Filter::eq("kind", "keep")),
        &r,
    )
    .build()
    .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    sub.ready(Some(Duration::from_secs(5))).await.unwrap();
    for (i, kind) in ["drop", "keep", "drop", "keep"].iter().enumerate() {
        api(
            INSECURE,
            "publish",
            serde_json::json!({ "channel": ch, "data": i, "tags": { "kind": kind } }),
        )
        .await;
    }
    // The namespace keeps history, so publications carry offsets.
    assert_eq!(ev.wait("pub").await, "pub 2 1");
    assert_eq!(ev.wait("pub").await, "pub 4 3");
    close(&client).await;
}

/// Connection token with a short expiry: refreshed through `get_token`; a
/// private channel needs a subscription token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_tokens() {
    if !enabled() {
        return;
    }
    for kind in [TransportKind::WebSocket, TransportKind::HttpStream] {
        let refreshes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let n = refreshes.clone();
        let (r, mut ev) = recorder();
        let client = record_client(builder(SECURE, kind, Protocol::Json), &r)
            .get_token(move |_| {
                n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async {
                    Ok(Some(jwt(
                        &serde_json::json!({ "sub": "u1", "exp": now() + 2 }),
                    )))
                }
            })
            .build()
            .unwrap();
        let ch = unique("private:p");
        let sub = record_sub(
            client.new_subscription(&ch).get_token(|ctx| async move {
                Ok::<_, TokenError>(jwt(&serde_json::json!({ "sub": "u1", "channel": ctx.channel, "exp": now() + 3600 })))
            }),
            &r,
        )
        .build()
        .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        sub.ready(Some(Duration::from_secs(5))).await.unwrap();
        // The token expires in ~2 s: the client refreshes it and stays connected.
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(
            refreshes.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "{kind:?}"
        );
        assert_eq!(client.state(), State::Connected);
        assert_eq!(client.statistics().connects, 1, "{:?}", ev.drain());
        sub.publish(r#"{"ok":true}"#).await.unwrap();
        close(&client).await;

        // An invalid token: the server disconnects for good.
        let (r, mut ev) = recorder();
        let bad = record_client(
            builder(SECURE, kind, Protocol::Protobuf).token("not-a-jwt"),
            &r,
        )
        .build()
        .unwrap();
        bad.connect().unwrap();
        let e = ev.wait("disconnected").await;
        assert!(e.starts_with("disconnected 3500"), "{e}");
    }
}

/// Channel compaction (Centrifugo PRO, `docker compose --profile pro up -d`,
/// `CENTRIFUGO_PRO_TESTS=1`): pushes carry the numeric id negotiated at
/// subscribe instead of the channel name, over both transports, across a
/// reconnect with recovery. A forwarder records what the server sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_pro_channel_compaction() {
    const PRO: &str = "localhost:8014";
    init_tracing();
    if std::env::var_os("CENTRIFUGO_PRO_TESTS").is_none() {
        eprintln!("skipped: set CENTRIFUGO_PRO_TESTS=1 with `docker compose --profile pro up -d`");
        return;
    }
    for kind in [TransportKind::WebSocket, TransportKind::HttpStream] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let s = seen.clone();
        tokio::spawn(async move {
            loop {
                let (c, _) = listener.accept().await.unwrap();
                let up = tokio::net::TcpStream::connect(PRO).await.unwrap();
                let s = s.clone();
                tokio::spawn(async move {
                    let (mut cr, mut cw) = c.into_split();
                    let (mut ur, mut uw) = up.into_split();
                    let down = async {
                        let mut buf = [0u8; 8192];
                        while let Ok(n) = ur.read(&mut buf).await {
                            if n == 0 || cw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                            s.lock().unwrap().extend_from_slice(&buf[..n]);
                        }
                    };
                    let _ = tokio::join!(tokio::io::copy(&mut cr, &mut uw), down);
                });
            }
        });
        let ch = unique("compact:c");
        let (r, mut ev) = recorder();
        let client = builder(&format!("127.0.0.1:{port}"), kind, Protocol::Json)
            .build()
            .unwrap();
        let sub = record_sub(client.new_subscription(&ch).recoverable(true), &r)
            .build()
            .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        sub.ready(Some(Duration::from_secs(5))).await.unwrap();
        let publish = |n: u32| {
            let ch = ch.clone();
            async move {
                let r = api(
                    PRO,
                    "publish",
                    serde_json::json!({ "channel": ch, "data": { "n": n } }),
                )
                .await;
                assert!(r.get("error").is_none(), "{r}");
            }
        };
        publish(1).await;
        assert_eq!(ev.wait("pub").await, r#"pub 1 {"n":1}"#, "{kind:?}");
        // Recovery after a reconnect: the subscribe negotiates a new id.
        client.disconnect();
        publish(2).await;
        publish(3).await;
        client.connect().unwrap();
        assert_eq!(
            ev.wait("subscribed").await,
            "subscribed recovered=true pos=Some(1)"
        );
        assert_eq!(ev.wait("pub").await, r#"pub 2 {"n":2}"#);
        assert_eq!(ev.wait("pub").await, r#"pub 3 {"n":3}"#);
        publish(4).await;
        assert_eq!(ev.wait("pub").await, r#"pub 4 {"n":4}"#);
        close(&client).await;
        let wire = String::from_utf8_lossy(&seen.lock().unwrap()).to_string();
        assert!(
            wire.contains(r#""push":{"id":"#),
            "{kind:?}: no compacted push on the wire"
        );
        let live = wire
            .matches(&format!(r#""push":{{"channel":"{ch}""#))
            .count();
        assert_eq!(live, 0, "{kind:?}: pushes still carry the channel name");
    }
}
