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
const HTTP_STREAM: &str = "http://localhost:8010/connection/http_stream";

fn endpoint(http_stream: bool) -> TransportEndpoint {
    if http_stream {
        TransportEndpoint::http_stream(HTTP_STREAM)
    } else {
        TransportEndpoint::websocket(WS)
    }
}

async fn connect_subscribe_publish(protocol: Protocol, http_stream: bool) {
    let client = Client::builder_with_transports([endpoint(http_stream)])
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
    connect_subscribe_publish(Protocol::Protobuf, false).await;
}

#[wasm_bindgen_test]
async fn websocket_json() {
    connect_subscribe_publish(Protocol::Json, false).await;
}

/// Cross-origin `fetch` (the test page isn't on Centrifugo's origin): the
/// stream request and every emulation POST go through a CORS preflight.
#[wasm_bindgen_test]
async fn http_stream_protobuf() {
    connect_subscribe_publish(Protocol::Protobuf, true).await;
}

#[wasm_bindgen_test]
async fn http_stream_json() {
    connect_subscribe_publish(Protocol::Json, true).await;
}

#[wasm_bindgen_test]
async fn http_stream_reconnect_recovers() {
    reconnect_recovers(true).await;
}

fn client(protocol: Protocol, http_stream: bool) -> Client {
    Client::builder_with_transports([endpoint(http_stream)])
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
        #[wasm_bindgen(method, js_name = dispatchEvent)]
        pub fn dispatch_event(this: &Global, event: &Event) -> bool;

        #[wasm_bindgen(extends = js_sys::Object)]
        pub type Event;
        #[wasm_bindgen(constructor)]
        pub fn new(name: &str) -> Event;
    }
}

#[wasm_bindgen_test]
async fn reconnect_recovers_missed_publications() {
    reconnect_recovers(false).await;
}

async fn reconnect_recovers(http_stream: bool) {
    let c = client(Protocol::Protobuf, http_stream);
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
    let other = client(Protocol::Json, http_stream);
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
    let c = client(Protocol::Protobuf, false);
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

/// Disconnecting aborts the stream's `fetch`: the server sees the client go
/// at once, not after its ping timeout (about 25 s). Presence shows it.
#[wasm_bindgen_test]
async fn http_stream_disconnect_aborts_the_request() {
    // Presence is on for channels without a namespace.
    let channel = format!("browser_p_{}", js_sys::Date::now());
    let a = client(Protocol::Protobuf, true);
    let sa = a.new_subscription(&channel).build().unwrap();
    sa.subscribe();
    a.connect().unwrap();
    sa.ready(Some(Duration::from_secs(5))).await.unwrap();
    let b = client(Protocol::Protobuf, false);
    let sb = b.new_subscription(&channel).build().unwrap();
    sb.subscribe();
    b.connect().unwrap();
    sb.ready(Some(Duration::from_secs(5))).await.unwrap();
    assert_eq!(sb.presence().await.unwrap().clients.len(), 2);

    a.disconnect();
    let mut left = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(100)).await;
        if sb.presence().await.unwrap().clients.len() == 1 {
            left = true;
            break;
        }
    }
    assert!(
        left,
        "the server still sees the disconnected client after 2 s"
    );
    a.close().await;
    b.close().await;
}

/// Dispatches `name` on the global scope, with `persisted` for page events.
fn dispatch(name: &str, persisted: Option<bool>) {
    let e = web_sys_global::Event::new(name);
    if let Some(p) = persisted {
        js_sys::Reflect::set(&e, &"persisted".into(), &p.into()).unwrap();
    }
    let global: web_sys_global::Global = js_sys::global().unchecked_into();
    global.dispatch_event(&e);
}

/// `offline` closes the transport (connecting, code 1); `online` reconnects
/// at once, though the backoff is 10 s. Then the same for the back/forward
/// cache: `pagehide` / `pageshow` with `persisted`.
#[wasm_bindgen_test]
async fn lifecycle_events_close_and_reconnect() {
    let c = Client::builder_with_transports([endpoint(false)])
        .event_delivery(EventDelivery::Inline)
        .min_reconnect_delay(Duration::from_secs(10))
        .max_reconnect_delay(Duration::from_secs(10))
        .build()
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let tx2 = tx.clone();
    c.on_connecting(move |_, e| {
        let _ = tx.send(format!("connecting {} {}", e.code, e.reason));
    });
    c.on_connected(move |_, _| {
        let _ = tx2.send("connected".to_owned());
    });
    c.connect().unwrap();
    assert_eq!(rx.recv().await.unwrap(), "connecting 0 connect called");
    assert_eq!(rx.recv().await.unwrap(), "connected");

    dispatch("offline", None);
    assert_eq!(rx.recv().await.unwrap(), "connecting 1 transport closed");
    let t0 = js_sys::Date::now();
    dispatch("online", None);
    assert_eq!(rx.recv().await.unwrap(), "connected");
    assert!(
        js_sys::Date::now() - t0 < 2000.0,
        "online waited for the backoff"
    );

    // A normal unload (`persisted` false) changes nothing.
    dispatch("pagehide", Some(false));
    sleep(Duration::from_millis(200)).await;
    assert_eq!(c.state(), centrifuge::State::Connected);
    dispatch("pagehide", Some(true));
    assert_eq!(
        rx.recv().await.unwrap(),
        "connecting 1 page entered back/forward cache"
    );
    let t0 = js_sys::Date::now();
    dispatch("pageshow", Some(true));
    assert_eq!(rx.recv().await.unwrap(), "connected");
    assert!(
        js_sys::Date::now() - t0 < 2000.0,
        "pageshow waited for the backoff"
    );
    c.close().await;
}
