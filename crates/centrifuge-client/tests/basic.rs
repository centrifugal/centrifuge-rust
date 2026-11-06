//! Connect, subscribe, publications and calls over every transport and codec.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)] // test code

mod common;

use std::time::Duration;

use centrifuge::{Error, HistoryOptions, Protocol, State, SubState, TransportKind};
use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_subscribe_publish_matrix() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        let (r, mut ev) = recorder();
        let client = record_client(builder(&srv, kind, protocol), &r)
            .build()
            .unwrap();
        let sub = record_sub(client.new_subscription("news").recoverable(true), &r)
            .build()
            .unwrap();
        sub.subscribe();
        assert_eq!(sub.state(), SubState::Subscribing);
        client.connect().unwrap();
        assert_eq!(client.state(), State::Connecting);
        client.ready(Some(Duration::from_secs(5))).await.unwrap();
        sub.ready(Some(Duration::from_secs(5))).await.unwrap();
        assert_eq!(ev.wait("connected").await, format!("connected {kind:?}"));
        ev.wait("subscribed").await;

        srv.publish("news", r#"{"n":1}"#);
        assert_eq!(
            ev.wait("pub").await,
            r#"pub 1 {"n":1}"#,
            "{kind:?} {protocol:?}"
        );

        sub.publish(r#"{"n":2}"#).await.unwrap();
        assert_eq!(ev.wait("pub").await, r#"pub 2 {"n":2}"#);

        let rpc = client.rpc("echo", r#"{"x":true}"#).await.unwrap();
        assert_eq!(&rpc.data[..], br#"{"x":true}"#);

        let h = sub.history(HistoryOptions::new().limit(10)).await.unwrap();
        assert_eq!(h.publications.len(), 2);
        assert_eq!(h.offset, 2);
        let top = sub.history(HistoryOptions::new()).await.unwrap();
        assert_eq!(top.publications, [] as [centrifuge::Publication; 0]);
        assert_eq!(top.offset, 2);

        let p = sub.presence().await.unwrap();
        assert_eq!(p.clients.len(), 1);
        let ps = sub.presence_stats().await.unwrap();
        assert_eq!(ps.num_clients, 1);

        client.send(r#"{"async":1}"#).await.unwrap();
        srv.wait_for(|r| r.command.send.is_some()).await;

        srv.message(r#"{"m":1}"#);
        assert_eq!(ev.wait("message").await, r#"message {"m":1}"#);

        let stats = client.statistics();
        assert_eq!(stats.state, State::Connected);
        assert_eq!(stats.transport, Some(kind));
        assert_eq!(stats.connects, 1);
        assert_eq!(stats.subscriptions, 1);
        assert!(stats.bytes_in > 0 && stats.bytes_out > 0);

        client.close().await;
        assert_eq!(client.state(), State::Closed);
        assert_eq!(sub.state(), SubState::Unsubscribed);
        assert!(matches!(client.connect(), Err(Error::ClientClosed)));
        let r = sub.publish("{}").await;
        assert!(matches!(r, Err(Error::ClientClosed)), "{r:?}");
        let tail = ev.drain();
        assert!(
            tail.iter().any(|e| e.starts_with("unsubscribed 2")),
            "{tail:?}"
        );
        assert!(
            tail.last().is_some_and(|e| e.starts_with("disconnected 0")),
            "{tail:?}"
        );
        srv.wait_until(|s| (s.connection_count() == 0).then_some(()))
            .await;
    }
}

/// Calls made while connecting (or before connect) wait for the connection,
/// as in centrifuge-js.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls_wait_for_connect() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        // A slow connect reply.
        let gate = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let g = gate.clone();
        srv.on_command(move |r| {
            (r.command.connect.is_some() && g.swap(false, std::sync::atomic::Ordering::SeqCst))
                .then_some(Action::NoReply)
        });
        let client = builder(&srv, kind, protocol)
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        // Before connect(): waits for the next connect.
        let c = client.clone();
        let early = tokio::spawn(async move { c.publish("chan", r#"{"early":1}"#).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.connect().unwrap();
        let c = client.clone();
        let during = tokio::spawn(async move { c.rpc("m", r#"{"during":1}"#).await });
        // The first connect gets no reply. Drop that connection: the client
        // reconnects, and both calls, still waiting, go out once connected.
        srv.wait_for(|r| r.command.connect.is_some()).await;
        srv.terminate();
        early.await.unwrap().unwrap();
        let rpc = during.await.unwrap().unwrap();
        assert_eq!(&rpc.data[..], br#"{"during":1}"#, "{kind:?} {protocol:?}");
        client.close().await;
    }
}

/// A waiting call fails with `ClientDisconnected` on `disconnect()`, and with
/// Timeout when nothing happens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_calls_fail_on_disconnect_and_timeout() {
    let srv = server().await;
    let client = builder(
        &srv,
        centrifuge::TransportKind::WebSocket,
        centrifuge::Protocol::Json,
    )
    .timeout(Duration::from_millis(300))
    .build()
    .unwrap();
    // Nobody connects: timeout.
    assert!(matches!(
        client.publish("a", "{}").await,
        Err(Error::Timeout)
    ));
    // Hold the connect reply, then disconnect(): the waiter fails at once.
    srv.hold_replies(true);
    client.connect().unwrap();
    let c = client.clone();
    let waiting = tokio::spawn(async move { c.publish("a", "{}").await });
    srv.wait_for(|r| r.command.connect.is_some()).await;
    client.disconnect();
    assert!(matches!(
        waiting.await.unwrap(),
        Err(Error::ClientDisconnected)
    ));
    // ready() fails at once while disconnected.
    assert!(matches!(
        client.ready(None).await,
        Err(Error::ClientDisconnected)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_errors_and_invalid_json() {
    let srv = server().await;
    srv.on_command(|r| {
        r.command
            .publish
            .is_some()
            .then(|| Action::error(103, "permission denied", false))
    });
    let client = builder(
        &srv,
        centrifuge::TransportKind::WebSocket,
        centrifuge::Protocol::Json,
    )
    .build()
    .unwrap();
    client.connect().unwrap();
    match client.publish("x", "{}").await {
        Err(Error::Server(e)) => assert_eq!(e.code, 103),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        client.publish("x", "not json").await,
        Err(Error::InvalidData(_))
    ));
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry() {
    let srv = server().await;
    let client = builder(
        &srv,
        centrifuge::TransportKind::WebSocket,
        centrifuge::Protocol::Protobuf,
    )
    .build()
    .unwrap();
    let a = client.new_subscription("a").build().unwrap();
    assert!(matches!(
        client.new_subscription("a").build(),
        Err(Error::DuplicateSubscription)
    ));
    assert_eq!(client.get_subscription("a"), Some(a.clone()));
    assert_eq!(client.subscriptions().len(), 1);
    client.remove_subscription(&a).unwrap();
    assert!(client.get_subscription("a").is_none());
    assert_eq!(a.state(), SubState::Unsubscribed);
    assert!(matches!(
        a.publish("x").await,
        Err(Error::SubscriptionUnsubscribed)
    ));
    // The channel can be registered again.
    let a2 = client.new_subscription("a").build().unwrap();
    assert_ne!(a, a2);
    client.remove_subscription(&a).unwrap(); // a different object: no-op
    assert!(client.get_subscription("a").is_some());
}

/// Real HTTP headers go with every request (WebSocket upgrade, HTTP-stream
/// request, every emulation request); emulated headers go inside the connect
/// command; `set_*` change them, and the data, from the next attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_emulated_headers_and_data() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        let mut h = http::HeaderMap::new();
        h.insert("x-app", "one".parse().unwrap());
        let mut eh = std::collections::HashMap::new();
        eh.insert("Authorization".to_owned(), "Bearer one".to_owned());
        let client = builder(&srv, kind, protocol)
            .http_headers(h)
            .emulated_headers(eh)
            .data(r#"{"v":1}"#)
            .build()
            .unwrap();
        let sub = client
            .new_subscription("hd")
            .data(r#"{"s":1}"#)
            .build()
            .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        sub.ready(None).await.unwrap();
        client.publish("hd", "{}").await.unwrap();
        // The next attempt's values.
        let mut h2 = http::HeaderMap::new();
        h2.insert("x-app", "two".parse().unwrap());
        client.set_http_headers(h2).unwrap();
        let mut eh2 = std::collections::HashMap::new();
        eh2.insert("Authorization".to_owned(), "Bearer two".to_owned());
        client.set_emulated_headers(eh2).unwrap();
        client.set_data(r#"{"v":2}"#).unwrap();
        sub.set_data(r#"{"s":2}"#).unwrap();
        srv.terminate();
        srv.wait_until(|s| (s.connects() == 2).then_some(())).await;
        sub.ready(Some(Duration::from_secs(3))).await.unwrap();
        client.publish("hd", "{}").await.unwrap();

        let reqs = srv.requests();
        let values: Vec<(String, String)> = reqs
            .iter()
            .map(|(p, h)| {
                (
                    p.clone(),
                    h.get("x-app")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("-")
                        .to_owned(),
                )
            })
            .collect();
        assert!(
            values.iter().all(|(_, v)| v == "one" || v == "two"),
            "{kind:?}: every request has the header: {values:?}"
        );
        assert!(values.iter().any(|(_, v)| v == "two"), "{values:?}");
        if kind == TransportKind::HttpStream {
            assert!(values.iter().any(|(p, _)| p == "/emulation"), "{values:?}");
        }
        let connects: Vec<_> = srv
            .commands()
            .into_iter()
            .filter_map(|c| c.connect)
            .collect();
        assert_eq!(
            connects[0].headers.get("Authorization").map(String::as_str),
            Some("Bearer one")
        );
        assert_eq!(
            connects[1].headers.get("Authorization").map(String::as_str),
            Some("Bearer two")
        );
        assert_eq!(&connects[0].data[..], br#"{"v":1}"#);
        assert_eq!(&connects[1].data[..], br#"{"v":2}"#);
        let subs: Vec<_> = srv
            .commands()
            .into_iter()
            .filter_map(|c| c.subscribe)
            .collect();
        assert_eq!(&subs[0].data[..], br#"{"s":1}"#, "{kind:?} {protocol:?}");
        assert_eq!(&subs.last().unwrap().data[..], br#"{"s":2}"#);
        client.close().await;
    }
}

/// `get_state` is asked before a subscribe without a position; its position is
/// used to recover.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_state_position_is_used() {
    let srv = server().await;
    for _ in 0..3 {
        srv.publish("gs", r#"{"n":1}"#);
    }
    let epoch_tx = srv.stream_position("gs").1;
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = calls.clone();
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .build()
        .unwrap();
    let (r, mut ev) = recorder();
    let sub = record_sub(
        client
            .new_subscription("gs")
            .recoverable(true)
            .get_state(move |ctx| {
                assert_eq!(ctx.channel, "gs");
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let epoch = epoch_tx.clone();
                async move { Ok(centrifuge::StreamPosition::new(2, epoch)) }
            }),
        &r,
    )
    .build()
    .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    assert_eq!(
        ev.wait("subscribed").await,
        "subscribed recovered=true pos=Some(2)"
    );
    assert_eq!(
        ev.wait("pub").await,
        r#"pub 3 {"n":1}"#,
        "only the publication after the loaded position"
    );
    let s = srv.last_subscribe().unwrap();
    assert!(s.recover && s.offset == 2);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    // A reconnect recovers from what was delivered, without asking again.
    srv.terminate();
    sub.ready(Some(Duration::from_secs(3))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(srv.last_subscribe().unwrap().offset, 3);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_and_leave() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::HttpStream, Protocol::Protobuf)
        .build()
        .unwrap();
    let (r, mut ev) = recorder();
    let sub = record_sub(client.new_subscription("jl").join_leave(true), &r)
        .build()
        .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    sub.ready(None).await.unwrap();
    assert!(srv.last_subscribe().unwrap().join_leave);
    let info = centrifuge_fake_server::proto::ClientInfo {
        user: "u".into(),
        client: "c-9".into(),
        ..Default::default()
    };
    srv.join("jl", info.clone());
    srv.leave("jl", info);
    assert_eq!(ev.wait("join").await, "join c-9");
    assert_eq!(ev.wait("leave").await, "leave c-9");
    client.close().await;
}

/// `send()` resolves once its command is written, not when it's queued: a
/// stalled emulation request means no success (review finding).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_resolves_when_written() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::HttpStream, Protocol::Json)
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();
    client.send(r#"{"a":1}"#).await.unwrap();
    srv.hang_emulation(true);
    let r = client.send(r#"{"a":2}"#).await;
    assert!(r.is_err(), "not written: {r:?}");
    client.close().await;
}

/// Closing an HTTP-stream session stops an emulation request already in
/// flight: its `send()` fails at once instead of after the request timeout,
/// and the request's connection isn't held after the session is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_session_stops_inflight_emulation_request() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::HttpStream, Protocol::Json)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();
    srv.hang_emulation(true);
    let c = client.clone();
    let send = tokio::spawn(async move { c.send(r#"{"a":1}"#).await });
    // Let the request reach the (hanging) server.
    tokio::time::sleep(Duration::from_millis(200)).await;
    client.disconnect();
    let r = tokio::time::timeout(Duration::from_secs(2), send)
        .await
        .expect("send() still waiting on the closed session's request")
        .unwrap();
    assert!(r.is_err(), "not written: {r:?}");
    client.close().await;
}

/// A `send()` whose emulation request the server rejects (e.g. 413) fails with
/// the transport's status, as documented, not as a disconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_send_reports_the_status() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::HttpStream, Protocol::Json)
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();
    srv.emulation_status(Some(413));
    let r = client.send(r#"{"big":1}"#).await;
    assert!(
        matches!(
            r,
            Err(Error::Transport {
                status: Some(413),
                ..
            })
        ),
        "{r:?}"
    );
    srv.emulation_status(None);
    client.close().await;
}
