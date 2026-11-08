//! HTTP-streaming with bidirectional emulation (docs/spec.md §5.7), wire-compatible
//! with centrifuge-js and centrifuge-csharp.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use centrifuge_client_core::codec::{self, StreamDecoder};
use centrifuge_client_core::machine::CloseReason;
use centrifuge_client_core::proto;
use http_body_util::{BodyExt as _, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;
use url::Url;

use super::http_common::{
    PostError, Poster, content_type, emulation_writer, post_status, stream_response,
};
use super::{Ctx, Inbound};

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
    let content_type = resp
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    if let Err(reason) = stream_response(resp.status().as_u16(), content_type) {
        ctx.closed(reason).await;
        return;
    }
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(emulation_writer(
        ctx.clone(),
        rx,
        HyperPoster { sender: None },
    ));
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

/// Emulation requests over one kept-alive HTTP/1.1 connection, reopened when
/// the server closes it.
struct HyperPoster {
    sender: Option<http1::SendRequest<Full<Bytes>>>,
}

impl Poster for HyperPoster {
    async fn post(&mut self, ctx: &Ctx, url: &Url, body: Vec<u8>) -> Result<(), PostError> {
        let sender = &mut self.sender;
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
        post_status(status)
    }
}
