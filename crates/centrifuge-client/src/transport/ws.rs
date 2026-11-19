//! WebSocket transport (docs/spec.md §5.1).

use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use centrifuge_client_core::codec::{self, Protocol};
use centrifuge_client_core::machine::CloseReason;
use futures_util::{SinkExt as _, StreamExt as _};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{self, Message};

use super::{Ctx, Inbound, WriteReq, drain, encode_frames, fail_calls, mark_sent};

/// How long a graceful close may take before the socket is dropped (§5.1).
const CLOSE_GRACE: Duration = Duration::from_secs(1);

pub(super) async fn run(ctx: Ctx) {
    let ws = match open(&ctx).await {
        Ok(ws) => ws,
        Err(reason) => {
            ctx.closed(reason).await;
            return;
        }
    };
    let (sink, mut stream) = ws.split();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(writer(ctx.clone(), sink, rx));
    ctx.report(Inbound::Opened {
        transport: ctx.id,
        writer: tx,
    })
    .await;
    loop {
        let msg = match stream.next().await {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                let reason = match e {
                    tungstenite::Error::Capacity(_) => CloseReason::TooLarge,
                    e => CloseReason::Failed {
                        status: None,
                        message: e.to_string(),
                        source: None,
                    },
                };
                ctx.closed(reason).await;
                return;
            }
            None => {
                ctx.closed(CloseReason::Eof).await;
                return;
            }
        };
        let data: Bytes = match msg {
            Message::Binary(b) => b,
            Message::Text(t) => Bytes::from(t),
            Message::Close(frame) => {
                let reason = match frame {
                    Some(f) => CloseReason::Frame {
                        code: f.code.into(),
                        reason: f.reason.to_string(),
                    },
                    None => CloseReason::Eof,
                };
                ctx.closed(reason).await;
                return;
            }
            _ => continue,
        };
        ctx.shared
            .bytes_in
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        let len = data.len();
        match codec::decode_frame(ctx.protocol, data, ctx.max_message_size) {
            Ok(replies) => {
                if !ctx.route(replies, len).await {
                    return;
                }
            }
            Err(e) => {
                // Nothing after it in the frame is dispatched (patterns 73, 81).
                ctx.closed(super::decode_close(&e)).await;
                return;
            }
        }
    }
}

type Ws = tokio_tungstenite::WebSocketStream<super::net::BoxIo>;

async fn open(ctx: &Ctx) -> Result<Ws, CloseReason> {
    let io = ctx
        .net
        .connect(&ctx.url, &[b"http/1.1"])
        .await
        .map_err(|e| CloseReason::Failed {
            status: e.status,
            message: e.message,
            source: None,
        })?;
    let mut req = ctx
        .url
        .as_str()
        .into_client_request()
        .map_err(|e| CloseReason::Failed {
            status: None,
            message: e.to_string(),
            source: None,
        })?;
    for (k, v) in &ctx.http_headers {
        req.headers_mut().append(k, v.clone());
    }
    let offered = ctx.offered_subprotocols();
    if !offered.is_empty() {
        let value =
            http::HeaderValue::from_str(&offered.join(", ")).map_err(|e| CloseReason::Failed {
                status: None,
                message: e.to_string(),
                source: None,
            })?;
        req.headers_mut().insert("Sec-WebSocket-Protocol", value);
    }
    let config = WebSocketConfig::default()
        .max_message_size(Some(ctx.max_message_size))
        .max_frame_size(Some(ctx.max_message_size));
    let (ws, _resp) = tokio_tungstenite::client_async_with_config(req, io, Some(config))
        .await
        .map_err(|e| {
            let status = match &e {
                tungstenite::Error::Http(r) => Some(r.status().as_u16()),
                _ => None,
            };
            CloseReason::Failed {
                status,
                message: format!("WebSocket handshake failed: {e}"),
                source: None,
            }
        })?;
    Ok(ws)
}

async fn writer(
    ctx: Ctx,
    mut sink: futures_util::stream::SplitSink<Ws, Message>,
    mut rx: mpsc::UnboundedReceiver<WriteReq>,
) {
    let mut closing = ctx.closing.clone();
    loop {
        let first = tokio::select! {
            biased;
            r = rx.recv() => match r {
                Some(r) => r,
                None => break,
            },
            _ = closing.wait_for(|c| *c) => break,
        };
        let mut reqs = drain(first, &mut rx);
        let cmds: Vec<_> = reqs
            .iter_mut()
            .flat_map(|r| std::mem::take(&mut r.cmds))
            .collect();
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
        let mut ok = true;
        for frame in frames {
            ctx.shared
                .bytes_out
                .fetch_add(frame.len() as u64, Ordering::Relaxed);
            let msg = if ctx.protocol == Protocol::Protobuf {
                Message::Binary(Bytes::from(frame))
            } else {
                match String::from_utf8(frame) {
                    Ok(s) => Message::Text(s.into()),
                    Err(e) => Message::Binary(Bytes::from(e.into_bytes())),
                }
            };
            // A stalled socket must not keep the old transport alive after a
            // reconnect: closing stops the write in progress.
            let sent = tokio::select! {
                r = sink.send(msg) => r,
                _ = closing.wait_for(|c| *c) => {
                    fail_calls(&ctx.shared, &mut reqs, &crate::Error::ClientDisconnected);
                    return;
                }
            };
            if let Err(e) = sent {
                ok = false;
                fail_calls(&ctx.shared, &mut reqs, &crate::Error::ClientDisconnected);
                // Let the reader finish what's buffered first, so a server close
                // code that explains the failure wins (§5.1).
                tokio::time::sleep(Duration::from_millis(200)).await;
                ctx.closed(CloseReason::Failed {
                    status: None,
                    message: format!("write failed: {e}"),
                    source: None,
                })
                .await;
                break;
            }
        }
        if !ok {
            return;
        }
        mark_sent(&mut reqs);
    }
    // Graceful close in the background; never waits for the peer for long.
    let close = Message::Close(Some(tungstenite::protocol::CloseFrame {
        code: CloseCode::Normal,
        reason: "".into(),
    }));
    let _ = tokio::time::timeout(CLOSE_GRACE, async {
        let _ = sink.send(close).await;
        let _ = sink.close().await;
    })
    .await;
}
