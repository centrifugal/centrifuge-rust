//! Browser smoke test (docs/wasm-plan.md, step 6): headless Chrome against
//! the real Centrifugo from `docker compose up`. Run with:
//!
//! ```text
//! CHROMEDRIVER=/path/to/chromedriver \
//! CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//! cargo test -p centrifuge-client --target wasm32-unknown-unknown --test browser
//! ```
#![cfg(target_arch = "wasm32")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code

use std::time::Duration;

use centrifuge::{Client, EventDelivery, Protocol, TransportEndpoint};
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
