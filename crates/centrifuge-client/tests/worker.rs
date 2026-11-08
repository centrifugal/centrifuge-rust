//! The client inside a Dedicated Worker (docs/wasm-plan.md, step 9): the
//! module is loaded in a real worker, where the global scope is the worker's
//! (`self`), not a `window`. Run like `tests/browser.rs`.
#![cfg(target_arch = "wasm32")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code

use std::time::Duration;

use centrifuge::{Client, Protocol, TransportEndpoint};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

async fn connect_subscribe_receive(endpoint: TransportEndpoint, protocol: Protocol) {
    // No `window` here: the SDK reaches everything through the global scope.
    assert!(
        js_sys::Reflect::get(&js_sys::global(), &"window".into())
            .unwrap()
            .is_undefined(),
        "not in a worker"
    );
    let client = Client::builder_with_transports([endpoint])
        .protocol(protocol)
        .build()
        .unwrap();
    let channel = format!("recovery:worker_{}", js_sys::Date::now());
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
    sub.ready(Some(Duration::from_secs(5)))
        .await
        .expect("subscribed");
    sub.publish(r#"{"n":1}"#).await.expect("published");
    assert_eq!(rx.recv().await.as_deref(), Some(r#"{"n":1}"#));
    // A clean close from inside the worker.
    client.close().await;
    assert_eq!(client.state(), centrifuge::State::Closed);
}

#[wasm_bindgen_test]
async fn websocket_in_a_worker() {
    connect_subscribe_receive(
        TransportEndpoint::websocket("ws://localhost:8010/connection/websocket"),
        Protocol::Protobuf,
    )
    .await;
}

#[wasm_bindgen_test]
async fn http_stream_in_a_worker() {
    connect_subscribe_receive(
        TransportEndpoint::http_stream("http://localhost:8010/connection/http_stream"),
        Protocol::Json,
    )
    .await;
}
