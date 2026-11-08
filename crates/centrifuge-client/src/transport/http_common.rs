//! What both HTTP-streaming transports share (docs/spec.md §5.7): the native
//! one (`http_stream.rs`, hyper) and the browser's (`web_http.rs`, `fetch`).
//! Only sending a request differs ([`Poster`]); the emulation writer, its
//! order and its error rules are this one.

use std::sync::atomic::Ordering;

use centrifuge_client_core::codec::{self, Protocol};
use centrifuge_client_core::machine::CloseReason;
use centrifuge_client_core::proto;
use tokio::sync::mpsc;
use url::Url;

use super::{Ctx, Inbound, WriteReq, drain, encode_frames, fail_calls, mark_sent};
use crate::shared::CALL_ID_BASE;

pub(super) fn content_type(protocol: Protocol) -> &'static str {
    if protocol == Protocol::Protobuf {
        "application/octet-stream"
    } else {
        "application/json"
    }
}

pub(super) fn emulation_url(ctx: &Ctx) -> Url {
    if let Some(u) = &ctx.emulation_endpoint {
        return u.clone();
    }
    let mut u = ctx.url.clone();
    u.set_path("/emulation");
    u.set_query(None);
    u
}

/// The stream response's status and content type: a non-2xx status, or a
/// captive portal's 200 with HTML (a transport failure, §5.3), fails it.
pub(super) fn stream_response(status: u16, content_type: Option<&str>) -> Result<(), CloseReason> {
    if !(200..300).contains(&status) {
        return Err(CloseReason::Failed {
            status: Some(status),
            message: format!("HTTP-stream request answered with status {status}"),
            source: None,
        });
    }
    if content_type.is_some_and(|v| v.starts_with("text/html")) {
        return Err(CloseReason::Undecodable(codec::DecodeError::Json(
            "HTML response to an HTTP-stream request".into(),
        )));
    }
    Ok(())
}

/// A failed emulation request.
pub(super) enum PostError {
    /// The outcome is unknown, or the session is unusable: close and
    /// reconnect.
    Close(CloseReason),
    /// Only this request's commands fail (e.g. 413).
    Rejected(u16),
}

/// What an emulation request's HTTP status means.
pub(super) fn post_status(status: u16) -> Result<(), PostError> {
    if (200..300).contains(&status) {
        Ok(())
    } else if status == 404
        || status == 401
        || status == 403
        || (300..400).contains(&status)
        || status >= 500
    {
        // 404: the session's node is gone; 5xx; a redirect or auth failure.
        Err(PostError::Close(CloseReason::Failed {
            status: Some(status),
            message: format!("emulation request answered with status {status}"),
            source: None,
        }))
    } else {
        Err(PostError::Rejected(status))
    }
}

/// Sends one emulation request and returns once its response status is in.
pub(super) trait Poster {
    async fn post(&mut self, ctx: &Ctx, url: &Url, body: Vec<u8>) -> Result<(), PostError>;
}

/// Posts commands one request at a time, in order, each with a deadline
/// (pattern 83).
pub(super) async fn emulation_writer(
    ctx: Ctx,
    mut rx: mpsc::UnboundedReceiver<WriteReq>,
    mut poster: impl Poster,
) {
    let url = emulation_url(&ctx);
    let mut closing = ctx.closing.clone();
    loop {
        let first = tokio::select! {
            biased;
            r = rx.recv() => match r {
                Some(r) => r,
                None => return,
            },
            _ = closing.wait_for(|c| *c) => return,
        };
        let mut reqs = drain(first, &mut rx);
        let emu = reqs.iter().find_map(|r| r.emu.clone());
        let cmds: Vec<_> = reqs
            .iter_mut()
            .flat_map(|r| std::mem::take(&mut r.cmds))
            .collect();
        let core_ids: Vec<u32> = cmds
            .iter()
            .map(|c| c.id)
            .filter(|id| *id > 0 && *id < CALL_ID_BASE)
            .collect();
        let had_pong = cmds.iter().any(|c| *c == proto::Command::default());
        let Some((session, node)) = emu else {
            fail_calls(&ctx.shared, &mut reqs, &crate::Error::ClientDisconnected);
            continue;
        };
        let frames = match encode_frames(ctx.protocol, &cmds) {
            Ok(f) => f,
            Err(e) => {
                fail_calls(
                    &ctx.shared,
                    &mut reqs,
                    &crate::Error::InvalidData(e.to_string()),
                );
                continue;
            }
        };
        for frame in frames {
            let body = match codec::encode_emulation_request(ctx.protocol, &node, &session, &frame)
            {
                Ok(b) => b,
                Err(e) => {
                    fail_calls(
                        &ctx.shared,
                        &mut reqs,
                        &crate::Error::InvalidData(e.to_string()),
                    );
                    continue;
                }
            };
            ctx.shared
                .bytes_out
                .fetch_add(body.len() as u64, Ordering::Relaxed);
            // A closed session stops its request in flight, not after it ends or
            // times out: the outcome is unknown, as for any write a disconnect
            // interrupts, and nothing it reports could reach a newer session.
            let outcome = tokio::select! {
                biased;
                _ = closing.wait_for(|c| *c) => {
                    fail_calls(&ctx.shared, &mut reqs, &crate::Error::ClientDisconnected);
                    return;
                }
                // A late timer gets the same grace as call timers (§9.7).
                r = crate::rt::timeout(ctx.timeout, poster.post(&ctx, &url, body)) => {
                    r.unwrap_or_else(|_| Err(PostError::Close(CloseReason::Failed {
                        status: None,
                        message: "emulation request timed out".into(),
                        source: None,
                    })))
                }
            };
            match outcome {
                Ok(()) => {}
                Err(PostError::Close(reason)) => {
                    // 404 (the session's node is gone), 5xx, a network error, a
                    // hang: the outcome is unknown, so close and reconnect (§5.7).
                    fail_calls(&ctx.shared, &mut reqs, &crate::Error::ClientDisconnected);
                    ctx.closed(reason).await;
                    return;
                }
                Err(PostError::Rejected(status)) => {
                    // E.g. 413: only this request's commands fail.
                    fail_calls(
                        &ctx.shared,
                        &mut reqs,
                        &crate::Error::Transport {
                            status: Some(status),
                            message: format!("emulation request rejected with status {status}"),
                        },
                    );
                    ctx.report(Inbound::WriteFailed {
                        transport: ctx.id,
                        command_ids: core_ids.clone(),
                        had_pong,
                    })
                    .await;
                }
            }
        }
        mark_sent(&mut reqs);
    }
}

#[cfg(test)]
mod tests {
    use super::{PostError, post_status};

    #[test]
    fn post_status_classes() {
        for s in [200, 204, 299] {
            assert!(post_status(s).is_ok(), "{s}");
        }
        for s in [301, 302, 401, 403, 404, 500, 502, 503] {
            assert!(matches!(post_status(s), Err(PostError::Close(_))), "{s}");
        }
        for s in [400, 409, 413, 429] {
            assert!(
                matches!(post_status(s), Err(PostError::Rejected(x)) if x == s),
                "{s}"
            );
        }
    }
}
