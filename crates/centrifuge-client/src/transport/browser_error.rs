//! JavaScript errors become owned SDK errors at the boundary
//! (docs/wasm-plan.md, step 5). A `JsValue` isn't `Send`/`Sync`, so it never
//! goes into an SDK error: its `name` and `message` are read, the value is
//! dropped, and the transport reports the existing `CloseReason::Failed`.
//!
//! Aborts are never classified from the browser's error: the SDK only aborts
//! what the driver or writer decided to stop, and that code reports its own
//! reason (a disconnect, a timeout) or nothing (a replaced session).
//!
//! Built on every target so host tests cover the mapping; only reading a
//! `JsValue` is wasm-only.

#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

use std::sync::Arc;

use centrifuge_client_core::machine::CloseReason;

/// Longest text kept from a JavaScript value.
const MAX_TEXT: usize = 300;

/// A JavaScript error's `name` and `message`, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrowserError {
    pub(crate) name: Option<String>,
    pub(crate) message: String,
}

impl BrowserError {
    pub(crate) fn new(name: Option<String>, message: impl Into<String>) -> Self {
        Self {
            name: name
                .filter(|n| !n.is_empty())
                .map(|n| truncate(n, MAX_TEXT)),
            message: truncate(message.into(), MAX_TEXT),
        }
    }

    /// An `Error` (a `DOMException` is one) gives its `name` and `message`;
    /// anything else, or an `Error` with both empty, its string form.
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn from_js(value: &wasm_bindgen::JsValue) -> Self {
        use wasm_bindgen::JsCast as _;
        if let Some(e) = value.dyn_ref::<js_sys::Error>() {
            let name = String::from(e.name());
            let message = String::from(e.message());
            if !name.is_empty() || !message.is_empty() {
                return Self::new(Some(name), message);
            }
        }
        // wasm-bindgen's debug form never throws, unlike a value's own
        // `toString()`.
        let text = value.as_string().unwrap_or_else(|| format!("{value:?}"));
        Self::new(None, text)
    }
}

impl std::fmt::Display for BrowserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(n) if self.message.is_empty() => f.write_str(n),
            Some(n) => write!(f, "{n}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for BrowserError {}

/// At most `max` bytes, cut at a character boundary.
fn truncate(mut s: String, max: usize) -> String {
    if s.len() > max {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
        s.push('…');
    }
    s
}

/// What failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    /// `fetch()` rejected: a network failure, or CORS, which a browser
    /// reports as the same generic error.
    Fetch,
    /// The response has no body to stream.
    NoBody,
    /// Reading the response body failed.
    Read,
    /// The `WebSocket` constructor threw (e.g. an invalid URL).
    WsOpen,
    /// The socket's `error` event; browsers give no reason with it.
    WsError,
    /// A browser API the transport needs is missing.
    Unsupported(&'static str),
}

/// The transport failure for `what`, with the browser's error as source.
pub(crate) fn failed(what: Failure, err: Option<BrowserError>) -> CloseReason {
    let detail = |prefix: &str| match &err {
        Some(e) => format!("{prefix}: {e}"),
        None => prefix.to_owned(),
    };
    let (status, message) = match what {
        Failure::Fetch => (
            None,
            detail("HTTP-stream request failed (network error or CORS)"),
        ),
        Failure::NoBody => (None, "HTTP-stream response has no body".to_owned()),
        Failure::Read => (None, detail("reading the HTTP-stream response failed")),
        Failure::WsOpen => (None, detail("can't open the WebSocket")),
        Failure::WsError => (None, detail("WebSocket error")),
        Failure::Unsupported(api) => (
            None,
            format!("unsupported environment: no `{api}` in the global scope"),
        ),
    };
    CloseReason::Failed {
        status,
        message,
        source: err.map(|e| Arc::new(e) as centrifuge_client_core::machine::ArcError),
    }
}

#[cfg(test)]
mod tests {
    use super::{BrowserError, CloseReason, Failure, MAX_TEXT, failed, truncate};

    #[test]
    fn display_and_limits() {
        let e = BrowserError::new(Some("TypeError".into()), "Failed to fetch");
        assert_eq!(e.to_string(), "TypeError: Failed to fetch");
        assert_eq!(BrowserError::new(Some(String::new()), "x").to_string(), "x");
        assert_eq!(
            BrowserError::new(Some("AbortError".into()), "").to_string(),
            "AbortError"
        );
        let long = BrowserError::new(None, "é".repeat(MAX_TEXT));
        assert!(long.message.len() <= MAX_TEXT + '…'.len_utf8());
        assert!(long.message.ends_with('…'));
        assert_eq!(truncate("short".into(), MAX_TEXT), "short");
    }

    #[test]
    fn failures_keep_status_message_and_source() {
        let CloseReason::Failed {
            status,
            message,
            source,
        } = failed(
            Failure::Fetch,
            Some(BrowserError::new(
                Some("TypeError".into()),
                "Failed to fetch",
            )),
        )
        else {
            panic!("not Failed");
        };
        assert_eq!(status, None);
        assert_eq!(
            message,
            "HTTP-stream request failed (network error or CORS): TypeError: Failed to fetch"
        );
        assert_eq!(source.unwrap().to_string(), "TypeError: Failed to fetch");

        let CloseReason::Failed { status, source, .. } = failed(Failure::NoBody, None) else {
            panic!("not Failed");
        };
        assert_eq!(status, None);
        assert!(source.is_none());

        let CloseReason::Failed { message, .. } = failed(Failure::Unsupported("WebSocket"), None)
        else {
            panic!("not Failed");
        };
        assert!(message.contains("no `WebSocket`"), "{message}");
    }
}
