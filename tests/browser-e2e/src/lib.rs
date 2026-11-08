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

/// HTTP-streaming whose emulation endpoint never answers: a `send()` hangs
/// in its POST. Disconnecting must fail it at once and abort the request
/// (run.mjs checks the server saw it aborted). Resolves to the outcome and
/// how many milliseconds `send()` took to end after the disconnect.
#[wasm_bindgen]
pub fn hanging_post(stream: String, emulation: String) -> js_sys::Promise {
    use std::cell::RefCell;
    use std::rc::Rc;

    wasm_bindgen_futures::future_to_promise(async move {
        let err = |e: centrifuge::Error| JsValue::from_str(&e.to_string());
        let client = Client::builder_with_transports([TransportEndpoint::http_stream(stream)])
            .emulation_endpoint(emulation)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(err)?;
        client.connect().map_err(err)?;
        client
            .ready(Some(std::time::Duration::from_secs(5)))
            .await
            .map_err(err)?;
        let outcome = Rc::new(RefCell::new(None));
        let (c, o) = (client.clone(), outcome.clone());
        wasm_bindgen_futures::spawn_local(async move {
            let r = c.send("x").await;
            *o.borrow_mut() = Some(r);
        });
        sleep_ms(300).await;
        let t0 = js_sys::Date::now();
        client.disconnect();
        while outcome.borrow().is_none() && js_sys::Date::now() - t0 < 10_000.0 {
            sleep_ms(10).await;
        }
        let ms = js_sys::Date::now() - t0;
        let result = format!("{:?} {ms}", outcome.borrow());
        client.close().await;
        Ok(JsValue::from_str(&result))
    })
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = setTimeout)]
    fn set_timeout(handler: &js_sys::Function, ms: i32) -> JsValue;
}

async fn sleep_ms(ms: i32) {
    let p = js_sys::Promise::new(&mut |resolve, _| {
        set_timeout(&resolve, ms);
    });
    let _ = wasm_bindgen_futures::JsFuture::from(p).await;
}
