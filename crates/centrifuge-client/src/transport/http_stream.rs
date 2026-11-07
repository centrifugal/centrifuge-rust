//! HTTP-streaming with bidirectional emulation (docs/spec.md §5.7), wire-compatible
//! with centrifuge-js and centrifuge-csharp.

use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use centrifuge_client_core::codec::{self, Protocol, StreamDecoder};
use centrifuge_client_core::machine::CloseReason;
use centrifuge_client_core::proto;
use http_body_util::{BodyExt as _, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;
use url::Url;

use super::{Ctx, Inbound, WriteReq, drain, encode_frames, fail_calls, mark_sent};
use crate::shared::CALL_ID_BASE;

fn content_type(protocol: Protocol) -> &'static str {
    if protocol == Protocol::Protobuf {
        "application/octet-stream"
    } else {
        "application/json"
    }
}

fn failed(status: Option<u16>, message: impl Into<String>) -> CloseReason {
    CloseReason::Failed {
        status,
        message: message.into(),
        source: None,
    }
}

async fn http1_sender(
    ctx: &Ctx,
    url: &Url,
) -> Result<http1::SendRequest<Full<Bytes>>, CloseReason> {
    let io = ctx
        .net
        .connect(url, &[b"http/1.1"])
        .await
        .map_err(|e| failed(e.status, e.message))?;
    let (sender, conn) = http1::handshake(TokioIo::new(io))
        .await
        .map_err(|e| failed(None, e.to_string()))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(sender)
}

fn authority(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_owned(),
    }
}

fn request(
    ctx: &Ctx,
    url: &Url,
    body: Vec<u8>,
    accept: bool,
) -> Result<http::Request<Full<Bytes>>, CloseReason> {
    let path = match url.query() {
        Some(q) => format!("{}?{q}", url.path()),
        None => url.path().to_owned(),
    };
    let mut b = http::Request::post(path)
        .header(http::header::HOST, authority(url))
        .header(http::header::CONTENT_TYPE, content_type(ctx.protocol));
    if accept {
        b = b.header(http::header::ACCEPT, content_type(ctx.protocol));
    }
    let mut req = b
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| failed(None, e.to_string()))?;
    for (k, v) in &ctx.http_headers {
        // The protocol's own content type and accept win (sent once).
        if k == http::header::CONTENT_TYPE || k == http::header::ACCEPT {
            continue;
        }
        req.headers_mut().append(k, v.clone());
    }
    Ok(req)
}

pub(super) async fn run(ctx: Ctx, initial: Option<proto::Command>) {
    let Some(connect) = initial else {
        ctx.closed(failed(None, "HTTP-streaming needs the connect command"))
            .await;
        return;
    };
    let mut body = Vec::new();
    if let Err(e) = codec::encode_commands(ctx.protocol, std::slice::from_ref(&connect), &mut body)
    {
        ctx.closed(failed(None, e.to_string())).await;
        return;
    }
    let url = ctx.url.clone();
    let mut sender = match http1_sender(&ctx, &url).await {
        Ok(s) => s,
        Err(r) => {
            ctx.closed(r).await;
            return;
        }
    };
    let req = match request(&ctx, &url, body, true) {
        Ok(r) => r,
        Err(r) => {
            ctx.closed(r).await;
            return;
        }
    };
    let resp = match sender.send_request(req).await {
        Ok(r) => r,
        Err(e) => {
            ctx.closed(failed(None, e.to_string())).await;
            return;
        }
    };
    let status = resp.status().as_u16();
    if !resp.status().is_success() {
        ctx.closed(failed(
            Some(status),
            format!("HTTP-stream request answered with status {status}"),
        ))
        .await;
        return;
    }
    // A captive portal answers 200 with HTML: a transport failure (§5.3).
    let html = resp
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/html"));
    if html {
        ctx.closed(CloseReason::Undecodable(codec::DecodeError::Json(
            "HTML response to an HTTP-stream request".into(),
        )))
        .await;
        return;
    }
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(emulation_writer(ctx.clone(), rx));
    ctx.report(Inbound::Opened {
        transport: ctx.id,
        writer: tx,
    })
    .await;

    let mut body = resp.into_body();
    let mut decoder = StreamDecoder::new(ctx.protocol, ctx.max_message_size);
    let mut replies = Vec::new();
    loop {
        let frame = match body.frame().await {
            Some(Ok(f)) => f,
            Some(Err(e)) => {
                ctx.closed(failed(None, e.to_string())).await;
                return;
            }
            None => {
                let reason = match decoder.finish() {
                    Ok(()) => CloseReason::Eof,
                    Err(e) => failed(None, e.to_string()),
                };
                ctx.closed(reason).await;
                return;
            }
        };
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        ctx.shared
            .bytes_in
            .fetch_add(chunk.len() as u64, Ordering::Relaxed);
        let res = decoder.push(&chunk, &mut replies);
        // Replies decoded before an error were received before it: dispatched
        // first (§5.7).
        let batch = std::mem::take(&mut replies);
        if !ctx.route(batch, chunk.len()).await {
            return;
        }
        if let Err(e) = res {
            ctx.closed(super::decode_close(&e)).await;
            return;
        }
    }
}

fn emulation_url(ctx: &Ctx) -> Url {
    if let Some(u) = &ctx.emulation_endpoint {
        return u.clone();
    }
    let mut u = ctx.url.clone();
    u.set_path("/emulation");
    u.set_query(None);
    u
}

/// Posts commands one request at a time, in order, each with a deadline
/// (pattern 83).
async fn emulation_writer(ctx: Ctx, mut rx: mpsc::UnboundedReceiver<WriteReq>) {
    let url = emulation_url(&ctx);
    let mut sender: Option<http1::SendRequest<Full<Bytes>>> = None;
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
                r = post(&ctx, &url, &mut sender, body) => r,
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

enum PostError {
    Close(CloseReason),
    Rejected(u16),
}

async fn post(
    ctx: &Ctx,
    url: &Url,
    sender: &mut Option<http1::SendRequest<Full<Bytes>>>,
    body: Vec<u8>,
) -> Result<(), PostError> {
    let attempt = async {
        if sender.as_ref().is_none_or(http1::SendRequest::is_closed) {
            *sender = Some(http1_sender(ctx, url).await.map_err(PostError::Close)?);
        }
        let Some(s) = sender.as_mut() else {
            return Err(PostError::Close(failed(None, "no connection")));
        };
        s.ready()
            .await
            .map_err(|e| PostError::Close(failed(None, e.to_string())))?;
        let req = request(ctx, url, body, false).map_err(PostError::Close)?;
        let resp = s
            .send_request(req)
            .await
            .map_err(|e| PostError::Close(failed(None, e.to_string())))?;
        let status = resp.status().as_u16();
        // An emulation reply has no body worth reading; bound what's read.
        let _ = http_body_util::Limited::new(resp.into_body(), 64 * 1024)
            .collect()
            .await;
        if resp_ok(status) {
            Ok(())
        } else if status == 404
            || status == 401
            || status == 403
            || (300..400).contains(&status)
            || status >= 500
        {
            Err(PostError::Close(failed(
                Some(status),
                format!("emulation request answered with status {status}"),
            )))
        } else {
            Err(PostError::Rejected(status))
        }
    };
    // A late timer gets the same grace as call timers (§9.7).
    match tokio::time::timeout(ctx.timeout, attempt).await {
        Ok(r) => r,
        Err(_) => Err(PostError::Close(failed(
            None,
            "emulation request timed out",
        ))),
    }
}

fn resp_ok(status: u16) -> bool {
    (200..300).contains(&status)
}

#[allow(dead_code)]
const _: Duration = Duration::ZERO;
