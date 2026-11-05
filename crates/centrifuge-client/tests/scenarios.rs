//! Reconnects, recovery, fallback, tokens, server-side subscriptions,
//! re-entrancy and streams against the fake server.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)] // test code

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use centrifuge::{
    Client, ClientEvent, Error, Protocol, ProxyConfig, State, SubState, SubscriptionEvent,
    TokenError, TransportEndpoint, TransportKind,
};
use common::*;
use futures_util::StreamExt as _;

/// Publications missed during a reconnect are recovered: the offsets the app
/// sees continue without gap or duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_recovers_exactly_once() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        let (r, mut ev) = recorder();
        let client = record_client(builder(&srv, kind, protocol), &r)
            .build()
            .unwrap();
        let sub = record_sub(client.new_subscription("feed").recoverable(true), &r)
            .build()
            .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        if let Err(e) = sub.ready(None).await {
            panic!(
                "{kind:?} {protocol:?}: {e:?}: {:?} {:?}",
                ev.drain(),
                srv.commands()
            );
        }
        srv.publish("feed", r#"{"n":1}"#);
        assert_eq!(ev.wait("pub").await, r#"pub 1 {"n":1}"#);

        // Drop the connection, publish while the client is away.
        srv.hold_replies(true);
        srv.terminate();
        ev.wait("connecting").await;
        srv.publish("feed", r#"{"n":2}"#);
        srv.publish("feed", r#"{"n":3}"#);
        srv.hold_replies(false);

        let subscribed = ev.wait("subscribed").await;
        assert_eq!(
            subscribed, "subscribed recovered=true pos=Some(1)",
            "{kind:?} {protocol:?}"
        );
        assert_eq!(ev.wait("pub").await, r#"pub 2 {"n":2}"#);
        assert_eq!(ev.wait("pub").await, r#"pub 3 {"n":3}"#);
        srv.publish("feed", r#"{"n":4}"#);
        assert_eq!(ev.wait("pub").await, r#"pub 4 {"n":4}"#);
        let last = srv.last_subscribe().unwrap();
        assert!(last.recover && last.offset == 1, "{last:?}");
        client.close().await;
    }
}

/// A lost epoch: resubscribe reports recovered = false.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_change_is_not_recovered() {
    let srv = server().await;
    let (r, mut ev) = recorder();
    let client = record_client(builder(&srv, TransportKind::WebSocket, Protocol::Json), &r)
        .build()
        .unwrap();
    let sub = record_sub(client.new_subscription("feed").recoverable(true), &r)
        .build()
        .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    sub.ready(None).await.unwrap();
    srv.publish("feed", "1");
    ev.wait("pub").await;
    srv.reset_stream("feed");
    srv.terminate();
    assert_eq!(
        ev.wait("subscribed").await,
        "subscribed recovered=false pos=Some(0)"
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_disconnect_code_stops_reconnecting() {
    let srv = server().await;
    let (r, mut ev) = recorder();
    let client = record_client(
        builder(&srv, TransportKind::WebSocket, Protocol::Protobuf),
        &r,
    )
    .build()
    .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();
    srv.disconnect(3500, "invalid token");
    assert_eq!(
        ev.wait("disconnected").await,
        "disconnected 3500 invalid token"
    );
    assert_eq!(client.state(), State::Disconnected);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(srv.connects(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_code_reconnects() {
    for kind in [TransportKind::WebSocket, TransportKind::HttpStream] {
        let srv = server().await;
        let (r, mut ev) = recorder();
        let client = record_client(builder(&srv, kind, Protocol::Json), &r)
            .build()
            .unwrap();
        client.connect().unwrap();
        ev.wait("connected").await;
        srv.disconnect(3001, "shutdown");
        assert_eq!(
            ev.wait("connecting").await,
            "connecting 3001 shutdown",
            "{kind:?}"
        );
        ev.wait("connected").await;
        assert_eq!(srv.connects(), 2);
        client.close().await;
    }
}

/// WebSocket refused: falls back to HTTP-streaming at once, in the first round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn falls_back_to_http_stream() {
    for refusal in ["status", "hang", "html-ws"] {
        let srv = server().await;
        match refusal {
            "status" => srv.refuse_upgrade(Some(404)),
            "hang" => srv.hang_upgrade(true),
            _ => srv.refuse_upgrade(Some(200)),
        }
        let (r, mut ev) = recorder();
        let client = record_client(
            Client::builder_with_transports([
                TransportEndpoint::websocket(srv.ws_url()),
                TransportEndpoint::http_stream(srv.http_stream_url()),
            ])
            .proxy(ProxyConfig::None)
            .handshake_timeout(Duration::from_millis(300))
            .min_reconnect_delay(Duration::from_secs(30))
            .max_reconnect_delay(Duration::from_secs(30)),
            &r,
        )
        .build()
        .unwrap();
        let started = tokio::time::Instant::now();
        client.connect().unwrap();
        assert_eq!(
            ev.wait("connected").await,
            "connected HttpStream",
            "{refusal}"
        );
        // No backoff delay in the first round (min_reconnect_delay is 30 s; the
        // bound is loose so a slow CI runner cannot trip it).
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{refusal}: {:?}",
            started.elapsed()
        );
        client.close().await;
    }
}

/// A captive portal answers 200 with HTML: a transport failure, retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn html_stream_body_is_a_transport_failure() {
    let srv = server().await;
    srv.html_stream(true);
    let (r, mut ev) = recorder();
    let client = record_client(builder(&srv, TransportKind::HttpStream, Protocol::Json), &r)
        .build()
        .unwrap();
    client.connect().unwrap();
    ev.wait("error").await;
    srv.html_stream(false);
    ev.wait("connected").await;
    client.close().await;
}

/// HTTP-stream bytes split at every offset reassemble.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_stream_writes() {
    for protocol in [Protocol::Json, Protocol::Protobuf] {
        let srv = server().await;
        srv.split_writes(Some(1));
        let (r, mut ev) = recorder();
        let client = record_client(builder(&srv, TransportKind::HttpStream, protocol), &r)
            .build()
            .unwrap();
        let sub = record_sub(client.new_subscription("c").positioned(true), &r)
            .build()
            .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        sub.ready(None).await.unwrap();
        for i in 1..=5 {
            srv.publish("c", format!(r#"{{"i":{i}}}"#));
        }
        for i in 1..=5 {
            assert_eq!(ev.wait("pub").await, format!(r#"pub {i} {{"i":{i}}}"#));
        }
        client.close().await;
    }
}

/// A broken emulation endpoint reconnects (the outcome of the commands is
/// unknown).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_emulation_reconnects() {
    let srv = server().await;
    let (r, mut ev) = recorder();
    let client = record_client(
        builder(&srv, TransportKind::HttpStream, Protocol::Protobuf),
        &r,
    )
    .build()
    .unwrap();
    client.connect().unwrap();
    ev.wait("connected").await;
    srv.emulation_status(Some(503));
    let res = client.publish("x", "data").await;
    assert!(matches!(res, Err(Error::ClientDisconnected)), "{res:?}");
    ev.wait("connecting").await;
    srv.emulation_status(None);
    ev.wait("connected").await;
    client.publish("x", "data").await.unwrap();
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_token_before_connect_and_unauthorized() {
    let srv = server().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .get_token(move |_ctx| {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                match n {
                    0 => Err(TokenError::Other("backend down".into())),
                    1 => Ok(Some("t1".to_owned())),
                    _ => Err(TokenError::Unauthorized),
                }
            }
        })
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(Some(Duration::from_secs(5))).await.unwrap();
    let connect = srv.wait_for(|r| r.command.connect.is_some()).await;
    assert_eq!(connect.command.connect.unwrap().token, "t1");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // An expired token: 109 → refresh via the getter → unauthorized.
    let (r, mut ev) = recorder();
    let r2 = r.clone();
    client.on_disconnected(move |_, e| r2.push(format!("disconnected {}", e.code)));
    srv.on_command(|r| {
        r.command
            .connect
            .is_some()
            .then(|| Action::error(109, "token expired", false))
    });
    srv.terminate();
    assert_eq!(ev.wait("disconnected").await, "disconnected 1");
    drop(r);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_token_refresh() {
    let srv = server().await;
    srv.set_connect_result(proto::ConnectResult {
        expires: true,
        ttl: 1,
        ping: 25,
        pong: true,
        ..Default::default()
    });
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .token("initial")
        .get_token(|_| async { Ok(Some("refreshed".to_owned())) })
        .build()
        .unwrap();
    client.connect().unwrap();
    let refresh = tokio::time::timeout(
        Duration::from_secs(5),
        srv.wait_for(|r| r.command.refresh.is_some()),
    )
    .await
    .unwrap();
    assert_eq!(refresh.command.refresh.unwrap().token, "refreshed");
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_token_and_refresh() {
    let srv = server().await;
    srv.on_subscribe(|req| {
        assert_eq!(req.token, "sub-token");
        Ok(proto::SubscribeResult {
            expires: true,
            ttl: 1,
            ..Default::default()
        })
    });
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .build()
        .unwrap();
    let sub = client
        .new_subscription("private")
        .get_token(|ctx| async move {
            assert_eq!(ctx.channel, "private");
            Ok("sub-token".to_owned())
        })
        .build()
        .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    sub.ready(None).await.unwrap();
    let r = srv.wait_for(|r| r.command.sub_refresh.is_some()).await;
    assert_eq!(r.command.sub_refresh.unwrap().token, "sub-token");
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_side_subscriptions() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        let mut subs = std::collections::HashMap::new();
        subs.insert(
            "personal:1".to_owned(),
            proto::SubscribeResult {
                recoverable: true,
                epoch: "x".into(),
                ..Default::default()
            },
        );
        srv.set_connect_result(proto::ConnectResult {
            subs,
            ping: 25,
            pong: true,
            ..Default::default()
        });
        let (r, mut ev) = recorder();
        let client = record_client(builder(&srv, kind, protocol), &r)
            .build()
            .unwrap();
        client.connect().unwrap();
        assert_eq!(
            ev.wait("server_subscribed").await,
            "server_subscribed personal:1"
        );
        srv.publish_raw(
            "personal:1",
            proto::Publication {
                data: r#"{"hi":1}"#.into(),
                offset: 1,
                ..Default::default()
            },
        );
        assert_eq!(
            ev.wait("server_publication").await,
            r#"server_publication personal:1 {"hi":1}"#,
            "{kind:?} {protocol:?}"
        );
        srv.unsubscribe_push("personal:1", 2000, "server unsubscribe");
        assert_eq!(
            ev.wait("server_unsubscribed").await,
            "server_unsubscribed personal:1"
        );
        // Publishing into it from the client needs no subscription.
        client.publish("personal:1", "{}").await.unwrap();
        client.close().await;
    }
}

/// The server unsubscribes a client-side subscription: with a
/// resubscribe-code (2500+) it resubscribes, otherwise it ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsubscribe_push() {
    let srv = server().await;
    let (r, mut ev) = recorder();
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .build()
        .unwrap();
    let sub = record_sub(client.new_subscription("c"), &r)
        .build()
        .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    sub.ready(None).await.unwrap();
    ev.wait("subscribed").await;
    srv.unsubscribe_push("c", 2500, "resubscribe");
    assert_eq!(ev.wait("subscribing").await, "subscribing 2500 resubscribe");
    ev.wait("subscribed").await;
    srv.unsubscribe_push("c", 2000, "kicked");
    assert_eq!(ev.wait("unsubscribed").await, "unsubscribed 2000 kicked");
    assert_eq!(sub.state(), SubState::Unsubscribed);
    client.close().await;
}

/// Inside a handler: a call works while connected (the reader resolves it),
/// waiting for something the loop must do first is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_reentrancy() {
    let srv = server().await;
    let (r, mut ev) = recorder();
    let rt = tokio::runtime::Handle::current();
    let (r1, r2) = (r.clone(), r.clone());
    let rt2 = rt.clone();
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .on_connected(move |client, _| {
            // Blocking on a call from the handler thread works.
            let res = rt.block_on(client.rpc("m", r#"{"in":"handler"}"#));
            r1.push(format!(
                "rpc {}",
                res.map_or_else(
                    |e| e.to_string(),
                    |r| String::from_utf8_lossy(&r.data).into_owned()
                )
            ));
            // State changes are projected at once inside handlers.
            client.disconnect();
            r1.push(format!("state {:?}", client.state()));
        })
        .on_disconnected(move |client, _| {
            let res = rt2.block_on(client.ready(None));
            r2.push(format!(
                "ready {}",
                matches!(res, Err(Error::ClientDisconnected))
            ));
            let res = rt2.block_on(client.publish("x", "{}"));
            r2.push(format!("publish {res:?}"));
        })
        .build()
        .unwrap();
    client.connect().unwrap();
    assert_eq!(ev.wait("rpc").await, r#"rpc {"in":"handler"}"#);
    assert_eq!(ev.wait("state").await, "state Disconnected");
    assert_eq!(ev.wait("ready").await, "ready true");
    assert_eq!(ev.wait("publish").await, "publish Err(WouldDeadlock)");
}

/// Streams deliver with hand-off: the next step waits until `next()` takes the item.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_streams() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    let mut events = client.events();
    let sub = client.new_subscription("s").build().unwrap();
    let mut pubs = sub.publications();
    let mut sub_events = sub.events();
    sub.subscribe();
    client.connect().unwrap();
    // subscribe() came first, and subscription events precede client ones.
    assert!(matches!(
        sub_events.next().await,
        Some(SubscriptionEvent::Subscribing(_))
    ));
    assert!(matches!(
        events.next().await,
        Some(ClientEvent::Connecting(_))
    ));
    assert!(matches!(
        events.next().await,
        Some(ClientEvent::Connected(_))
    ));
    assert!(matches!(
        sub_events.next().await,
        Some(SubscriptionEvent::Subscribed(_))
    ));
    srv.publish("s", "a");
    srv.publish("s", "b");
    assert!(
        matches!(sub_events.next().await, Some(SubscriptionEvent::Publication(p)) if &p.data[..] == b"a")
    );
    assert_eq!(&pubs.next().await.unwrap().data[..], b"a");
    assert!(
        matches!(sub_events.next().await, Some(SubscriptionEvent::Publication(p)) if &p.data[..] == b"b")
    );
    assert_eq!(&pubs.next().await.unwrap().data[..], b"b");
    // A new stream takes over; the old one ends.
    let mut events2 = client.events();
    assert!(events.next().await.is_none());
    drop(sub_events);
    drop(pubs);
    let close = client.close();
    assert!(matches!(
        events2.next().await,
        Some(ClientEvent::Disconnected(_))
    ));
    close.await;
    assert!(events2.next().await.is_none());
}

/// A panicking handler disconnects the client with an error event; the
/// client stays usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_panic() {
    let srv = server().await;
    let (r, mut ev) = recorder();
    let r1 = r.clone();
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .on_connected(|_, _| panic!("boom"))
        .on_error(move |_, e| r1.push(format!("error {:?}", e.error.kind)))
        .build()
        .unwrap();
    client.connect().unwrap();
    assert_eq!(ev.wait("error").await, "error HandlerPanicked");
    assert_eq!(client.state(), State::Disconnected);
    client.on_connected(|_, _| {});
    client.connect().unwrap();
    client.ready(None).await.unwrap();
    client.close().await;
}

/// Dropping every handle stops the client and closes the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_closes() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .build()
        .unwrap();
    let weak = client.downgrade();
    client.connect().unwrap();
    client.ready(None).await.unwrap();
    assert_eq!(srv.connection_count(), 1);
    drop(client);
    srv.wait_until(|s| (s.connection_count() == 0).then_some(()))
        .await;
    assert!(weak.upgrade().is_none());
}

/// Commands queued before the connect reply go out batched after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribes_are_batched_after_connect() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    let subs: Vec<_> = (0..20)
        .map(|i| client.new_subscription(format!("ch{i}")).build().unwrap())
        .collect();
    for s in &subs {
        s.subscribe();
    }
    client.connect().unwrap();
    for s in &subs {
        s.ready(None).await.unwrap();
    }
    assert_eq!(srv.count(|r| r.command.subscribe.is_some()), 20);
    client.close().await;
}

/// The inline event delivery mode works the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_delivery() {
    let srv = server().await;
    let (r, mut ev) = recorder();
    let client = record_client(builder(&srv, TransportKind::HttpStream, Protocol::Json), &r)
        .event_delivery(centrifuge::EventDelivery::Inline)
        .build()
        .unwrap();
    let sub = record_sub(client.new_subscription("c"), &r)
        .build()
        .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    sub.ready(None).await.unwrap();
    srv.publish("c", "1");
    assert_eq!(ev.wait("pub").await, "pub 1 1");
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configuration_errors() {
    let srv = server().await;
    let bad = |b: centrifuge::ClientBuilder| matches!(b.build(), Err(Error::Configuration(_)));
    assert!(bad(Client::builder("ftp://x")));
    assert!(bad(Client::builder_with_transports([])));
    assert!(bad(Client::builder_with_transports([
        TransportEndpoint::websocket("http://x")
    ])));
    assert!(bad(
        builder(&srv, TransportKind::WebSocket, Protocol::Json).timeout(Duration::ZERO)
    ));
    // "No limit" as Duration::MAX overflowed the deadline arithmetic (review finding).
    assert!(bad(
        builder(&srv, TransportKind::WebSocket, Protocol::Json).timeout(Duration::MAX)
    ));
    assert!(bad(
        builder(&srv, TransportKind::WebSocket, Protocol::Json).max_reconnect_delay(Duration::MAX)
    ));
    assert!(bad(
        builder(&srv, TransportKind::WebSocket, Protocol::Json).name("a-very-long-client-name")
    ));
    let mut h = http::HeaderMap::new();
    h.insert("upgrade", "x".parse().unwrap());
    assert!(bad(
        builder(&srv, TransportKind::WebSocket, Protocol::Json).http_headers(h)
    ));
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .build()
        .unwrap();
    let sub = client
        .new_subscription("d")
        .delta(centrifuge::DeltaType::Fossil)
        .tags_filter(centrifuge::Filter::eq("k", "v"))
        .build();
    assert!(matches!(sub, Err(Error::Configuration(_))));
}

#[test]
fn build_without_runtime_fails() {
    let r = Client::builder("ws://localhost:1/connection/websocket").build();
    assert!(matches!(r, Err(Error::Configuration(_))));
}

/// A terminal disconnect push in reply to connect (invalid token) over
/// HTTP-streaming: the server sends the push, then EOF.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_disconnect_push_on_connect() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        srv.on_command(|r| {
            r.command.connect.is_some().then(|| Action::Disconnect {
                code: 3500,
                reason: "invalid token".into(),
            })
        });
        let (r, mut ev) = recorder();
        let client = record_client(builder(&srv, kind, protocol), &r)
            .build()
            .unwrap();
        client.connect().unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let all = ev.drain();
        let e = all
            .iter()
            .find(|e| e.starts_with("disconnected"))
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            e, "disconnected 3500 invalid token",
            "{kind:?} {protocol:?}: {all:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            srv.count(|r| r.command.connect.is_some()),
            1,
            "{kind:?} {protocol:?}"
        );
    }
}

/// A subscribe that gets no reply times out and reconnects the client (the
/// server state is unknown), then the subscription is subscribed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_timeout_reconnects() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        let (r, mut ev) = recorder();
        let client = record_client(
            builder(&srv, kind, protocol).timeout(Duration::from_millis(500)),
            &r,
        )
        .build()
        .unwrap();
        client.connect().unwrap();
        client.ready(None).await.unwrap();
        ev.wait("connected").await;
        srv.hold_replies(true);
        let sub = record_sub(client.new_subscription("held"), &r)
            .build()
            .unwrap();
        sub.subscribe();
        srv.wait_for(|r| r.command.subscribe.is_some()).await;
        srv.hold_replies(false);
        let e = ev.wait("connecting").await;
        assert!(e.starts_with("connecting 3 "), "{kind:?} {protocol:?}: {e}");
        sub.ready(Some(Duration::from_secs(3))).await.unwrap();
        client.close().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_timeout_reconnects_inline() {
    let srv = server().await;
    let (r, mut ev) = recorder();
    let client = record_client(
        builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
            .timeout(Duration::from_millis(500))
            .event_delivery(centrifuge::EventDelivery::Inline),
        &r,
    )
    .build()
    .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();
    ev.wait("connected").await;
    srv.hold_replies(true);
    let sub = record_sub(client.new_subscription("held"), &r)
        .build()
        .unwrap();
    sub.subscribe();
    srv.wait_for(|r| r.command.subscribe.is_some()).await;
    srv.hold_replies(false);
    let e = ev.wait("connecting").await;
    assert!(e.starts_with("connecting 3 "), "{e}");
    sub.ready(Some(Duration::from_secs(3))).await.unwrap();
    client.close().await;
}

/// `close()` completes although streams are alive and never polled; their
/// last item stays buffered, then they end (review finding: it hung).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_with_unpolled_streams() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .build()
        .unwrap();
    let mut events = client.events();
    let sub = client.new_subscription("u").build().unwrap();
    let mut sub_events = sub.events();
    sub.subscribe();
    client.connect().unwrap();
    // Take what comes until connected and subscribed (both streams, as the
    // events interleave), then stop polling.
    let (mut connected, mut subscribed) = (false, false);
    while !(connected && subscribed) {
        tokio::select! {
            Some(e) = events.next(), if !connected => connected = matches!(e, ClientEvent::Connected(_)),
            Some(e) = sub_events.next(), if !subscribed => subscribed = matches!(e, SubscriptionEvent::Subscribed(_)),
        }
    }
    tokio::time::timeout(Duration::from_secs(2), client.close())
        .await
        .expect("close() doesn't wait for unpolled streams");
    assert!(matches!(
        events.next().await,
        Some(ClientEvent::Disconnected(_))
    ));
    assert!(events.next().await.is_none());
    assert!(matches!(
        sub_events.next().await,
        Some(SubscriptionEvent::Unsubscribed(_))
    ));
    assert!(sub_events.next().await.is_none());
}

/// Dropping the last client while a stream is alive and unpolled stops the
/// client: the connection closes and the stream ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_with_unpolled_stream() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::HttpStream, Protocol::Protobuf)
        .build()
        .unwrap();
    let mut events = client.events();
    client.connect().unwrap();
    while !matches!(events.next().await, Some(ClientEvent::Connected(_))) {}
    // A pending item nobody takes.
    srv.message("{}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(client);
    srv.wait_until(|s| (s.connection_count() == 0).then_some(()))
        .await;
    let rest: Vec<_> = tokio::time::timeout(Duration::from_secs(2), events.collect::<Vec<_>>())
        .await
        .expect("the stream ends");
    assert!(rest.len() <= 2, "{rest:?}");
}

/// The runtime the client runs on shuts down: waiters on other runtimes are
/// released (`close()` resolves, `ready()` fails, streams end).
#[test]
fn runtime_shutdown_releases_waiters() {
    let main = tokio::runtime::Runtime::new().unwrap();
    let other = tokio::runtime::Runtime::new().unwrap();
    let srv = main.block_on(server());
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .runtime(other.handle().clone())
        .build()
        .unwrap();
    // Never connected: ready() would fail at once; hold one on a subscription.
    let sub = client.new_subscription("rt").build().unwrap();
    sub.subscribe();
    let mut events = client.events();
    let waiting = main.spawn({
        let sub = sub.clone();
        async move { sub.ready(None).await }
    });
    std::thread::sleep(Duration::from_millis(100));
    other.shutdown_timeout(Duration::from_secs(1));
    main.block_on(async {
        let r = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .expect("released")
            .unwrap();
        assert!(matches!(r, Err(Error::ClientClosed)), "{r:?}");
        tokio::time::timeout(Duration::from_secs(2), client.close())
            .await
            .expect("close() resolves");
        let end = tokio::time::timeout(Duration::from_secs(2), async {
            while events.next().await.is_some() {}
        })
        .await;
        assert!(end.is_ok(), "the stream ends");
    });
}

/// A `connect()` queued but never applied when the driver dies: `ready()` fails
/// with `ClientClosed`, `state()` reads Closed and `connect()` is refused (review
/// finding: `ready()` hung on the projected Connecting).
#[test]
fn driver_dies_with_unapplied_ops() {
    let main = tokio::runtime::Runtime::new().unwrap();
    // A current-thread runtime nobody drives: the driver never runs.
    let idle = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let srv = main.block_on(server());
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .runtime(idle.handle().clone())
        .build()
        .unwrap();
    client.connect().unwrap();
    assert_eq!(client.state(), State::Connecting);
    drop(idle);
    main.block_on(async {
        let r = tokio::time::timeout(Duration::from_secs(2), client.ready(None))
            .await
            .expect("ready() returns");
        assert!(matches!(r, Err(Error::ClientClosed)), "{r:?}");
    });
    assert_eq!(client.state(), State::Closed);
    assert!(matches!(client.connect(), Err(Error::ClientClosed)));
}

/// A consumer busy with an item when `close()` runs (the next one already
/// offered) still gets the final unsubscribed event before the stream ends
/// (review finding: it was dropped because the slot was full).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_stream_keeps_the_close_event() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    let sub = client.new_subscription("busy").build().unwrap();
    let mut events = sub.events();
    sub.subscribe();
    client.connect().unwrap();
    while !matches!(events.next().await, Some(SubscriptionEvent::Subscribed(_))) {}
    srv.publish("busy", "1");
    srv.publish("busy", "2");
    assert!(matches!(
        events.next().await,
        Some(SubscriptionEvent::Publication(_))
    ));
    // Busy with publication 1; publication 2 is offered meanwhile.
    tokio::time::sleep(Duration::from_millis(200)).await;
    tokio::time::timeout(Duration::from_secs(2), client.close())
        .await
        .unwrap();
    let rest: Vec<_> = events.collect().await;
    assert_eq!(rest.len(), 2, "{rest:?}");
    assert!(
        matches!(&rest[0], SubscriptionEvent::Publication(p) if &p.data[..] == b"2"),
        "{rest:?}"
    );
    assert!(
        matches!(&rest[1], SubscriptionEvent::Unsubscribed(_)),
        "{rest:?}"
    );
}
