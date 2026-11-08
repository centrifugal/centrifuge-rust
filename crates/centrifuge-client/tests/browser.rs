//! Browser smoke test (docs/wasm-plan.md, step 6): headless Chrome against
//! the real Centrifugo from `docker compose up`. Run with:
//!
//! ```text
//! CHROMEDRIVER=/path/to/chromedriver \
//! CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//! cargo test -p centrifuge-client --target wasm32-unknown-unknown --test browser
//! ```
//!
//! Set `WASM_BINDGEN_TEST_TIMEOUT=90`: the ping test takes 40 s.
#![cfg(target_arch = "wasm32")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code

use std::time::Duration;

use centrifuge::{Client, EventDelivery, Protocol, TransportEndpoint};
use wasm_bindgen::JsCast as _;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

const WS: &str = "ws://localhost:8010/connection/websocket";

async fn connect_subscribe_publish(protocol: Protocol) {
    let client = Client::builder_with_transports([TransportEndpoint::websocket(WS)])
        .protocol(protocol)
        .event_delivery(EventDelivery::Inline)
        .build()
        .unwrap();
    let channel = format!("recovery:browser_{}", js_sys::Date::now());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sub = client
        .new_subscription(&channel)
        .on_publication(move |_, e| {
            let _ = tx.send(String::from_utf8_lossy(&e.data).into_owned());
        })
        .build()
        .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    client
        .ready(Some(Duration::from_secs(5)))
        .await
        .expect("connected");
    sub.ready(Some(Duration::from_secs(5)))
        .await
        .expect("subscribed");
    sub.publish(r#"{"n":1}"#).await.expect("published");
    // The runner's own timeout bounds this wait.
    assert_eq!(rx.recv().await.as_deref(), Some(r#"{"n":1}"#));
    let h = sub
        .history(centrifuge::HistoryOptions::new().all())
        .await
        .unwrap();
    assert_eq!(h.publications.len(), 1);
    client.close().await;
    assert_eq!(client.state(), centrifuge::State::Closed);
}

#[wasm_bindgen_test]
async fn websocket_protobuf() {
    connect_subscribe_publish(Protocol::Protobuf).await;
}

#[wasm_bindgen_test]
async fn websocket_json() {
    connect_subscribe_publish(Protocol::Json).await;
}

fn client(protocol: Protocol) -> Client {
    Client::builder_with_transports([TransportEndpoint::websocket(WS)])
        .protocol(protocol)
        .event_delivery(EventDelivery::Inline)
        .min_reconnect_delay(Duration::from_millis(50))
        .build()
        .unwrap()
}

/// Waits `d` on the browser's event loop.
async fn sleep(d: Duration) {
    let ms = i32::try_from(d.as_millis()).unwrap();
    let p = js_sys::Promise::new(&mut |resolve, _| {
        let global: web_sys_global::Global = js_sys::global().unchecked_into();
        global.set_timeout(&resolve, ms);
    });
    wasm_bindgen_futures::JsFuture::from(p).await.unwrap();
}

mod web_sys_global {
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    extern "C" {
        pub type Global;
        #[wasm_bindgen(method, js_name = setTimeout)]
        pub fn set_timeout(this: &Global, handler: &js_sys::Function, ms: i32) -> JsValue;
    }
}

#[wasm_bindgen_test]
async fn reconnect_recovers_missed_publications() {
    let c = client(Protocol::Protobuf);
    let channel = format!("recovery:browser_r_{}", js_sys::Date::now());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let tx2 = tx.clone();
    let sub = c
        .new_subscription(&channel)
        .recoverable(true)
        .on_subscribed(move |_, e| {
            let _ = tx2.send(format!("subscribed recovered={}", e.recovered));
        })
        .on_publication(move |_, e| {
            let _ = tx.send(format!("pub {}", String::from_utf8_lossy(&e.data)));
        })
        .build()
        .unwrap();
    sub.subscribe();
    c.connect().unwrap();
    assert_eq!(rx.recv().await.unwrap(), "subscribed recovered=false");
    sub.publish("1").await.unwrap();
    assert_eq!(rx.recv().await.unwrap(), "pub 1");

    c.disconnect();
    // Another client publishes while this one is away.
    let other = client(Protocol::Json);
    other.connect().unwrap();
    for n in ["2", "3"] {
        other.publish(&channel, n).await.unwrap();
    }
    other.close().await;

    c.connect().unwrap();
    assert_eq!(rx.recv().await.unwrap(), "subscribed recovered=true");
    assert_eq!(rx.recv().await.unwrap(), "pub 2");
    assert_eq!(rx.recv().await.unwrap(), "pub 3");
    c.close().await;
}

/// Centrifugo pings every 25 s; without pong handling the client would see
/// no ping within its 35 s window and reconnect.
#[wasm_bindgen_test]
async fn stays_connected_across_pings() {
    let c = client(Protocol::Protobuf);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    c.on_disconnected(move |_, e| {
        let _ = tx.send(format!("disconnected {} {}", e.code, e.reason));
    });
    c.connect().unwrap();
    c.ready(Some(Duration::from_secs(5))).await.unwrap();
    sleep(Duration::from_secs(40)).await;
    assert_eq!(c.state(), centrifuge::State::Connected);
    assert!(rx.try_recv().is_err(), "{:?}", rx.try_recv());
    // The client answered the server's pings in the meantime.
    assert!(c.statistics().messages_out >= 2, "{:?}", c.statistics());
    c.close().await;
}
