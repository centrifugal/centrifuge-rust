//! A browser `CloseEvent` as the `CloseReason` the native transport gives for
//! the same situation (docs/wasm-plan.md, step 6). Built on every target, so
//! host tests cover every code.

#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

use centrifuge_client_core::machine::CloseReason;

/// Codes an endpoint may send in a close frame (RFC 6455 §7.4).
fn sendable(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
}

/// `had_error`: an `error` event came before the `close` event.
pub(crate) fn close_reason(code: u16, reason: &str, had_error: bool) -> CloseReason {
    let failed = |what: &str| CloseReason::Failed {
        status: None,
        message: format!("WebSocket {what} (close code {code})"),
        source: None,
    };
    match code {
        c if sendable(c) => CloseReason::Frame {
            code: c,
            reason: reason.to_owned(),
        },
        // Closed without a close frame: an error before it means the read or
        // the connect failed, as natively.
        1006 if had_error => failed("connection failed"),
        // A close frame without a code (1005), or closed without one.
        1005 | 1006 => CloseReason::Eof,
        1015 => failed("TLS handshake failed"),
        // Natively, tungstenite replaces a code no endpoint may send with
        // 1002. Browsers fail the connection on one (1006), so this is only
        // reached by a non-conforming browser.
        _ => CloseReason::Frame {
            code: 1002,
            reason: "Protocol violation".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{CloseReason, close_reason};

    #[test]
    fn every_code_has_the_native_answer() {
        for code in 0..=u16::MAX {
            for had_error in [false, true] {
                let r = close_reason(code, "bye", had_error);
                match code {
                    1000..=1003 | 1007..=1014 | 3000..=4999 => assert!(
                        matches!(&r, CloseReason::Frame { code: c, reason } if *c == code && reason == "bye"),
                        "{code}: {r:?}"
                    ),
                    1005 => assert!(matches!(r, CloseReason::Eof), "{code}: {r:?}"),
                    1006 if had_error => {
                        assert!(
                            matches!(&r, CloseReason::Failed { message, .. } if message.contains("1006")),
                            "{r:?}"
                        );
                    }
                    1006 => assert!(matches!(r, CloseReason::Eof), "{r:?}"),
                    1015 => assert!(
                        matches!(&r, CloseReason::Failed { message, .. } if message.contains("1015")),
                        "{r:?}"
                    ),
                    _ => assert!(
                        matches!(&r, CloseReason::Frame { code: 1002, reason } if reason == "Protocol violation"),
                        "{code}: {r:?}"
                    ),
                }
            }
        }
    }
}
