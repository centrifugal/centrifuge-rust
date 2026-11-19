//! Extra WebSocket subprotocols: `ClientBuilder::websocket_subprotocols`.
//! Native only: the browser's `WebSocket` is covered by `tests/browser.rs`.
#![cfg(not(target_arch = "wasm32"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::Duration;

use centrifuge::{ClientBuilder, Error, Protocol, TransportKind};
use common::{builder, server};

async fn offered(b: ClientBuilder, srv: &common::FakeServer) -> Option<String> {
    let client = b.build().unwrap();
    client.connect().unwrap();
    client.ready(Some(Duration::from_secs(5))).await.unwrap();
    srv.requests()
        .iter()
        .find_map(|(_, h)| h.get("sec-websocket-protocol"))
        .map(|v| v.to_str().unwrap().to_owned())
}

fn refused(b: ClientBuilder) -> String {
    match b.build() {
        Err(Error::Configuration(m)) => m,
        Err(e) => panic!("expected Error::Configuration, got {e:?}"),
        Ok(_) => panic!("expected build() to fail"),
    }
}

#[tokio::test]
async fn default_offers_only_the_protobuf_subprotocol() {
    let srv = server().await;
    let b = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf);
    assert_eq!(
        offered(b, &srv).await.as_deref(),
        Some("centrifuge-protobuf")
    );
}

#[tokio::test]
async fn extra_subprotocols_follow_the_protobuf_one() {
    let srv = server().await;
    let b = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .websocket_subprotocols(["tenant-a", "v2"]);
    assert_eq!(
        offered(b, &srv).await.as_deref(),
        Some("centrifuge-protobuf, tenant-a, v2")
    );
}

#[tokio::test]
async fn json_offers_only_the_extra_subprotocols() {
    let srv = server().await;
    let b = builder(&srv, TransportKind::WebSocket, Protocol::Json)
        .websocket_subprotocols(["tenant-a"]);
    assert_eq!(offered(b, &srv).await.as_deref(), Some("tenant-a"));
}

#[tokio::test]
async fn json_without_extras_offers_no_subprotocol() {
    let srv = server().await;
    let b = builder(&srv, TransportKind::WebSocket, Protocol::Json);
    assert_eq!(offered(b, &srv).await, None);
}

#[tokio::test]
async fn invalid_subprotocol_values_are_refused() {
    let srv = server().await;
    for bad in ["", "a b", "a,b", "caf\u{e9}"] {
        let b =
            builder(&srv, TransportKind::WebSocket, Protocol::Json).websocket_subprotocols([bad]);
        let m = refused(b);
        assert!(m.contains("subprotocol"), "{bad:?}: {m}");
    }
}
