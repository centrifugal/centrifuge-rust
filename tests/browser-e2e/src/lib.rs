//! Connects a client and subscribes to `channel`; the client lives until
//! the page or worker goes away.
use centrifuge::{Client, TransportEndpoint};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn start(ws: &str, channel: &str) -> Result<(), JsError> {
    let client = Client::builder_with_transports([TransportEndpoint::websocket(ws)])
        .build()
        .map_err(|e| JsError::new(&e.to_string()))?;
    let sub = client
        .new_subscription(channel)
        .build()
        .map_err(|e| JsError::new(&e.to_string()))?;
    sub.subscribe();
    client.connect().map_err(|e| JsError::new(&e.to_string()))?;
    // Kept for the page's or the worker's lifetime.
    std::mem::forget((client, sub));
    Ok(())
}
