//! A chat with `centrifuge-client` in the browser: the page (`index.html`)
//! draws it, Rust does the connection. Run with
//! `docker compose -f examples/browser/compose.yml up`, then open
//! <http://localhost:8080> in two tabs.

use centrifuge::{Client, Subscription};
use wasm_bindgen::prelude::*;

/// One client and its subscription to the chat channel.
#[wasm_bindgen]
#[derive(Debug)]
pub struct Chat {
    // Kept so the connection lives as long as the page holds the chat.
    _client: Client,
    sub: Subscription,
}

#[wasm_bindgen]
impl Chat {
    /// Connects to `url` and subscribes to `channel`. `on_message(text)` gets
    /// each message, `on_status(text)` the connection state.
    ///
    /// # Errors
    ///
    /// An invalid configuration (e.g. a bad URL).
    #[wasm_bindgen(constructor)]
    pub fn new(
        url: &str,
        channel: &str,
        on_message: js_sys::Function,
        on_status: js_sys::Function,
    ) -> Result<Chat, JsError> {
        let status = move |s: &str| {
            let _ = on_status.call1(&JsValue::NULL, &s.into());
        };
        let (s1, s2, s3) = (status.clone(), status.clone(), status);
        let client = Client::builder(url)
            .on_connecting(move |_, _| s1("connecting…"))
            .on_connected(move |_, _| s2("connected"))
            .on_disconnected(move |_, e| s3(&format!("disconnected: {}", e.reason)))
            .build()
            .map_err(|e| JsError::new(&e.to_string()))?;
        let sub = client
            .new_subscription(channel)
            .on_publication(move |_, e| {
                let text = String::from_utf8_lossy(&e.data);
                let _ = on_message.call1(&JsValue::NULL, &text.as_ref().into());
            })
            .build()
            .map_err(|e| JsError::new(&e.to_string()))?;
        sub.subscribe();
        client.connect().map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Chat {
            _client: client,
            sub,
        })
    }

    /// Publishes `text` to the channel; every tab receives it, this one too.
    pub fn send(&self, text: String) -> js_sys::Promise {
        let sub = self.sub.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            sub.publish(text)
                .await
                .map(|()| JsValue::UNDEFINED)
                .map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }
}
